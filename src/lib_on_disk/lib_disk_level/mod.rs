use log::{debug};
use std::collections::HashSet;

use crate::configuration::CONFIGURATION;

use crate::lib_in_memory::{MemoryBuffer};
use crate::lib_helper::{records_to_bytes};
use crate::lib_template::{Record};
use crate::lib_merge::{merge_from_files};
use super::lib_disk_run::{Run};
use super::lib_disk_file::{DiskFile};
use std::time::{SystemTime};
use std::sync::{Arc};
use parking_lot::{RwLock};
use atomic_counter::{RelaxedCounter, AtomicCounter};
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::metrics::{PUT_IO_COUNTER};

pub struct DiskLevel {
    //pub lock: RwLock<i32>,
    pub level: usize, // immutable
    pub size: AtomicUsize, // bytes of data in level
    pub capacity: AtomicUsize,
    pub runs: RwLock<Vec<Run>>,
    pub run_counter: RelaxedCounter,
}

impl DiskLevel {
    pub fn empty_level(capacity_of_run: usize, level: usize) -> DiskLevel {
        DiskLevel {
            level: level,
            size: AtomicUsize::new(0),
            capacity: AtomicUsize::new(capacity_of_run * CONFIGURATION.RUNS_PER_LEVEL),
            runs: RwLock::new(Vec::new()),
            run_counter: RelaxedCounter::new(0),
        }
    }

    pub fn create_level(files: Vec<Vec<Arc<DiskFile>>>, size_of_run: usize, capacity_of_run: usize, level: usize) -> DiskLevel {
        let mut new_level = DiskLevel::empty_level(capacity_of_run, level);
        new_level.flush(files, size_of_run, size_of_run, capacity_of_run);
        new_level
    }

    pub fn create_level_from_buffer(data: Vec<Record>, size: usize, capacity_of_run: usize, level: usize) -> DiskLevel {
        let mut new_level = DiskLevel::empty_level(capacity_of_run, level);
        new_level.flush_from_buffer(data, size, capacity_of_run);
        new_level
    }

    pub fn is_full(&self) -> bool {
        let runs = self.runs.read();
        let num_runs = runs.len();
        let last_run_full = num_runs == CONFIGURATION.RUNS_PER_LEVEL && runs[num_runs - 1].is_full();
        last_run_full || num_runs > CONFIGURATION.RUNS_PER_LEVEL
    }

    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }
    
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn add_size(&self, sz: usize) {
        self.size.fetch_add(sz, Ordering::Relaxed);
    }

    pub fn dec_size(&self, sz: usize) {
        self.size.fetch_sub(sz, Ordering::Relaxed);
    }

    pub fn add_capacity(&self, sz: usize) {
        self.capacity.fetch_add(sz, Ordering::Relaxed);
    }

    pub fn dec_capacity(&self, sz: usize) {
        self.capacity.fetch_sub(sz, Ordering::Relaxed);
    }

	pub fn level(&self) -> usize {
        self.level
    }

    pub fn clear(&self) {
        let mut runs = self.runs.write();
        for run in runs.iter() {
            run.delete_files();
        }
        runs.clear();
        self.size.store(0, Ordering::Relaxed);
    }

    pub fn get_all_files(&self) -> Vec<Vec<Arc<DiskFile>>> {
        let mut level_files = Vec::new();
        let runs = self.runs.read();
        for run in runs.iter() {
            assert!(run.files.len() > 0);
            level_files.push(run.files.clone());
        }
        level_files
    }

    pub fn flush_from_buffer(&self, data_records: Vec<Record>, size: usize, capacity_of_run: usize) {
        let data = records_to_bytes(&data_records);
        let mut num_flushed = 0;
        let mut runs = Vec::new();
        
        while num_flushed < size {
            let to_flush = std::cmp::min(std::cmp::max(capacity_of_run, CONFIGURATION.FILE_SIZE), size - num_flushed);
            let new_run = Run::create_run(to_flush, capacity_of_run, &data[num_flushed..num_flushed + to_flush], self.level, self.run_counter.get());
            PUT_IO_COUNTER.inc_by((to_flush as f64 / CONFIGURATION.BLOCK_SIZE as f64).ceil() as i64);
            num_flushed += to_flush;
            runs.push(new_run);
            self.run_counter.inc();
        }
        let mut original_runs = self.runs.write();
        self.add_size(size);
        original_runs.extend(runs);
    }

    pub fn flush(&self, files: Vec<Vec<Arc<DiskFile>>>, data_size: usize, size_per_run: usize, capacity_of_run: usize) {
        let runs = self.runs.read();
        let num_runs = runs.len();
        if num_runs > 0 {
            let last_run = &runs[num_runs - 1];
            let last_run_size = last_run.size;
            assert!(last_run.capacity == capacity_of_run);

            // Merge data in files with last run 
            if size_per_run > last_run_size && !last_run.is_full() {
                let mut all_files_merge = vec![last_run.files.clone()];
                all_files_merge.extend(files);
                let merged_files = merge_from_files(all_files_merge, last_run);

                let mut files_size = 0;
                let mut file_idx = merged_files.len() - 1;
                let mut new_last_run = Run::create_empty_run(capacity_of_run, self.level, self.run_counter.get());
                self.run_counter.inc();
                for i in 0..merged_files.len() {
                    files_size += merged_files[i].size;
                    // file_idx is the index of merged_files in which all files before this index could fit into this run
                    if file_idx == merged_files.len() - 1 && files_size >= size_per_run {
                        file_idx = i;
                    }
                }
                new_last_run.insert_files(merged_files[..file_idx + 1].to_vec());
                drop(runs);

                // replace last run
                let mut runs = self.runs.write();
                self.add_size(files_size - last_run_size);
                runs[num_runs - 1].delete_files();
                runs[num_runs - 1] = new_last_run;

                // if still have data left over, add left over data as new run to level
                if merged_files.len() > file_idx + 1 {
                    let mut new_run = Run::create_empty_run(capacity_of_run, self.level, self.run_counter.get());
                    self.run_counter.inc();
                    new_run.insert_files(merged_files[file_idx + 1..].to_vec());
                    runs.push(new_run);
                }
                return;
            }
        }
        drop(runs);

        // do not merge with last run, just append to end of level
        let mut counter = 1;
        let mut last_counter = 0;
        let mut files_size = 0;
        let mut runs_to_add = Vec::new();
        let mut size_to_add = 0;

        let mut empty_run = Run::create_empty_run(capacity_of_run, self.level, self.run_counter.get());
        self.run_counter.inc();

        let merged_files = merge_from_files(files, &mut empty_run);
        while counter < merged_files.len() + 1 {
            files_size += merged_files[counter - 1].size;
            if files_size >= size_per_run || counter == merged_files.len() {
                let mut new_run = Run::create_empty_run(capacity_of_run, self.level, self.run_counter.get());
                new_run.insert_files(merged_files[last_counter..counter].to_vec());
                last_counter = counter;
                self.run_counter.inc();
                runs_to_add.push(new_run);
                size_to_add += files_size;
                files_size = 0;
            }
            counter += 1;
        }
        let mut runs = self.runs.write();
        runs.extend(runs_to_add);
        self.add_size(size_to_add);
    }

    pub fn get(&self, key: &i32, record: &mut Record) -> bool {
        //debug!("get: I am level {}, I have {} runs", self.level, self.runs.len());
        let runs = self.runs.read();
        for run in runs.iter().rev() {
            if run.get(key, record) {
                return true;
            }
        }
        return false;
    }

    pub fn print_stats(&self, distinct_keys: &mut HashSet<i32>) {
        let runs = self.runs.read();
        for run in runs.iter() {
            run.print_stats(distinct_keys);
        }
        debug!("");
    }
}