use std::cmp;
use std::collections::{HashSet};
use std::sync::{Arc};
use std::convert::TryInto;
use std::fs;
use std::fs::{File};
use std::io::prelude::*;
use std::time::{SystemTime};
use atomic_counter::{RelaxedCounter, AtomicCounter};

use log::{debug};

use crate::configuration::CONFIGURATION;
use crate::lib_helper::{generate_filename, bytes_to_records, binary_search_fp};
use crate::lib_template::{Record};
use super::lib_disk_file::{DiskFile};

pub struct Run {
    pub level: usize, // level run is on
    pub run: usize, // index of run in level (0, 1, 2, 3)
    pub size: usize, // bytes of data in run
    pub capacity: usize, // bytes of data run can hold
    pub file_counter: RelaxedCounter, 
    pub files: Vec<Arc<DiskFile>>,
    pub fence_pointers: Vec<i32>,
}

impl Run {
	pub fn create_run(size: usize, capacity: usize, data: &[u8], level: usize, run: usize) -> Run {
        assert!(size > 0);
        let number_files = (size - 1) / CONFIGURATION.FILE_SIZE + 1;
        let mut fence_pointers: Vec<i32> = Vec::new();
        let mut files = Vec::new();
        let mut offset: usize = 0;

        for i in 0..number_files {
            assert!(offset < size);
            let bytes_to_write = cmp::min(CONFIGURATION.FILE_SIZE, size - offset);
            let filename = generate_filename(level, run, i);
            files.push(Arc::new(DiskFile::create_disk_file(filename, &data[offset..offset + bytes_to_write], bytes_to_write)));
            
            let key = i32::from_be_bytes(data[offset..offset + CONFIGURATION.KEY_SIZE].try_into().unwrap());
            fence_pointers.push(key);

            offset += bytes_to_write;
        }
        assert!(offset == size);

        Run {
            level: level,
            run: run,
            size: size,
            capacity: capacity,
            file_counter: RelaxedCounter::new(number_files),
            fence_pointers: fence_pointers,
            files: files,
		}
    }

    pub fn create_empty_run(capacity: usize, level: usize, run: usize) -> Run {
        Run {
            level: level,
            run: run,
            size: 0,
            capacity: capacity,
            file_counter: RelaxedCounter::new(0), 
            fence_pointers: Vec::new(),
            files: Vec::new(),
        }
    }

    pub fn is_full(&self) -> bool {
        return self.size as f64 >= self.capacity as f64 * CONFIGURATION.FULL_THRESHOLD;
    }

    pub fn get_fullness(&self) -> f64 {
        return self.size as f64 / self.capacity as f64;
    }

    pub fn delete_files(&self) {
        for file in self.files.iter() {
            fs::remove_file(&file.filename).expect(&format!("Failed to remove file {} in disk run, level is {}, run is {}, file counter is {}, size is {}", &file.filename, self.level, self.run, self.file_counter.get(), self.size));
        }
    }

    pub fn insert_files(&mut self, files_to_merge: Vec<Arc<DiskFile>>) {
        let mut fp_idx = self.fence_pointers.binary_search(&files_to_merge[0].fence_pointers[0]).unwrap_or_else(|x| x);
        for i in 0..files_to_merge.len() {
            self.fence_pointers.insert(fp_idx, files_to_merge[i].fence_pointers[0]);
            self.size += files_to_merge[i].size;
            self.files.insert(fp_idx, files_to_merge[i].clone());
            fp_idx += 1;
        }
    }

    pub fn get(&self, key: &i32, record: &mut Record) -> bool {
        let file_idx = match binary_search_fp(&self.fence_pointers, &key) {
            Some(idx) => idx,
            None => {
                return false;
            },
        };
        return self.files[file_idx].get(key, record);
    }

    pub fn get_all_bytes(&self) -> Vec<u8> {
        let mut offset = 0;
        let mut buffer = vec![0; self.size];
        for file in self.files.iter() {
            offset += file.read_file_to_buffer(0, &mut buffer[offset..offset + file.size]);
        }
        assert!(offset == self.size);
        buffer
    }

    pub fn get_all_records(&self) -> Vec<Record> {
        bytes_to_records(&self.get_all_bytes())
    }

    pub fn print_stats(&self, distinct_keys: &mut HashSet<i32>) {    
        for file in self.files.iter() {
            let mut buffer = vec![0; file.size];

            let mut f = File::open(&file.filename).expect("Failed to open file in disk run get");
            f.read(&mut buffer[..]).expect("Failed to read file in print stats");
            let records = bytes_to_records(&buffer);
            for j in 0..records.len() {
                print!("{}:{}:L{} ", records[j].key, records[j].value, self.level);
                distinct_keys.insert(records[j].key);
            }
        }
    }
}


