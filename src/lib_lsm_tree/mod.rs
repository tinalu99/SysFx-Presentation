use std::collections::{HashSet};
use log::{debug};

use parking_lot::{RwLock, RwLockWriteGuard};
use std::i32;

use crate::configuration::CONFIGURATION;

use crate::lib_template::{Record};
use crate::lib_on_disk::lib_disk_level::{DiskLevel};
use crate::lib_in_memory::{MemoryBuffer};

use std::sync::atomic::{AtomicBool, Ordering};

pub struct LSMTree {
    pub buffer: RwLock<MemoryBuffer>,
    pub levels: RwLock<Vec<DiskLevel>>,
    pub compacting: AtomicBool, // true if thread is currently compacting, false otherwise
}

impl LSMTree {
    pub fn create_lsmtree() -> LSMTree {
        let buffer = RwLock::new(MemoryBuffer::create_buffer());
        LSMTree {
            buffer: buffer,
            levels: RwLock::new(Vec::new()),
            compacting: AtomicBool::new(false),
        }
    }

    pub fn put(&self, key: &i32, value: &i32) {
        let mut buffer = self.buffer.write();
        buffer.put(key, value);
        if buffer.is_full() {
            self.flush_buffer_with_guard(buffer);
            self.compaction();
        }
    }

    pub fn flush_buffer_with_guard(&self, mut buffer: RwLockWriteGuard<MemoryBuffer>) {
        let data = buffer.merge();
        let mut new_capacity = buffer.capacity() as f64 * CONFIGURATION.T_OVER_K;
        new_capacity -= new_capacity % CONFIGURATION.RECORD_SIZE as f64;
        let buffer_size = buffer.size();

        let levels = self.levels.read();
        if levels.len() > 0 {
            levels[0].flush_from_buffer(data, buffer_size, new_capacity as usize);
        } else {
            drop(levels);
            let new_level = DiskLevel::create_level_from_buffer(data, buffer_size, new_capacity as usize, 1);
            let mut levels = self.levels.write();
            levels.push(new_level);
            drop(levels);
        }
        buffer.clear();
    }

    pub fn compaction(&self) {
        // If compacting was true before, don't compact
        if self.compacting.compare_and_swap(false, true, Ordering::Relaxed) {
            return;
        }
        self.merge_and_flush();
        self.compacting.store(false, Ordering::Relaxed);
    }

    pub fn merge_and_flush(&self) {
        let mut levels = self.levels.read();
        let mut level_files = Vec::new();
        for i in 0..levels.len() {
            if level_files.len() > 0 {
                let (prev_size, new_capacity) = self.prev_size_new_capacity(i);
                levels[i].flush(level_files, prev_size, std::cmp::max(CONFIGURATION.FILE_SIZE, new_capacity as usize), new_capacity as usize);
                drop(levels);

                self.clear_prev_level(i);
                levels = self.levels.read();
            }
            if !levels[i].is_full() {
                return;
            }
            level_files = levels[i].get_all_files();
        }

        // need to create new level
        if level_files.len() > 0 {
            let level_len = levels.len();
            // drop read-only reference, we need write reference later
            drop(levels);

            let (_prev_size, new_capacity) = self.prev_size_new_capacity(level_len);

            let mut levels = self.levels.write();
            levels.push(DiskLevel::create_level(level_files, std::cmp::max(CONFIGURATION.FILE_SIZE, new_capacity as usize), new_capacity as usize, level_len + 1));
            drop(levels);

            self.clear_prev_level(level_len);
        }
    }

    fn prev_size_new_capacity(&self, level_idx: usize) -> (usize, f64) {
        let (prev_size, prev_capacity) = if level_idx == 0 {
            let buffer = self.buffer.read();
            (buffer.size(), buffer.capacity())
        } else {
            let levels = self.levels.read();
            (levels[level_idx - 1].size(), levels[level_idx - 1].capacity())
        }; 
        let mut new_capacity = prev_capacity as f64 * CONFIGURATION.T_OVER_K;
        new_capacity -= new_capacity % CONFIGURATION.RECORD_SIZE as f64;
        (prev_size, new_capacity)
    }

    fn clear_prev_level(&self, level_idx: usize) {
        if level_idx == 0 {
            let mut buffer = self.buffer.write();
            buffer.clear();
        } else {
            let levels = self.levels.read();
            levels[level_idx - 1].clear();
        }
    }

    pub fn get(&self, key: &i32, value: &mut i32) -> bool {
        let mut record = Record::create_record(0, 0);
        
        let buffer = self.buffer.read();
        if buffer.get(key, &mut record) {
            *value = record.value;
            return true;
        }
        drop(buffer);

        let levels = self.levels.read();
        for level in levels.iter() {
            if level.get(key, &mut record) {
                *value = record.value;
                return true;
            }
        }
        false
    }

    pub fn print_stats(&self) {    
        let levels = self.levels.read();
        debug!("LOGICAL PAIRS: will be printed at end");
        let mut distinct_keys = HashSet::new();
        for level in levels.iter() {
            print!("LVL{}: {}", level.level(), level.size() / CONFIGURATION.RECORD_SIZE);
        }
        for level in levels.iter() {
            level.print_stats(&mut distinct_keys);
        }
        debug!("LOGICAL PAIRS: {}", distinct_keys.len());
    }

    pub fn delete_files(&self) {
        let levels = self.levels.read();
        for level in levels.iter() {
            level.clear();
        }
    }
}
