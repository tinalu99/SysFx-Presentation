use std::convert::TryInto;
use std::fs::{File};
use std::io::{SeekFrom};
use std::io::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::configuration::CONFIGURATION;
use crate::lib_helper::{bytes_to_records, binary_search_fp};
use crate::lib_template::{Record};
use crate::metrics::{GET_IO_COUNTER};

use bloom::{BloomFilter};

pub struct DiskFile {
    pub filename: String,
    pub size: usize,
    pub fence_pointers: Vec<i32>,
    bloom_filter: BloomFilter,
}

impl DiskFile {

    pub fn create_disk_file(filename: String, data: &[u8], size: usize) -> DiskFile {
        let mut fence_pointers: Vec<i32> = Vec::new();
        let mut bloom_filter = DiskFile::init_bloom_filter(size);

        // initialize bloom filters and fence pointers
        for i in (0..size).step_by(CONFIGURATION.RECORD_SIZE) {
            let key = i32::from_be_bytes(data[i..i + CONFIGURATION.KEY_SIZE].try_into().unwrap());
            bloom_filter.insert(&key);
            if (i % CONFIGURATION.BLOCK_SIZE) == 0 || i == size - CONFIGURATION.RECORD_SIZE {
                fence_pointers.push(key);
            }
        }

        let mut file = File::create(&filename).expect(&format!("Failed to create file {}", &filename));
        let bytes_written = file.write(&data[..]).expect(&format!("Failed to write to file {}", &filename));

        assert!(size == bytes_written);

        DiskFile {
            filename: filename,
            size: size,
            fence_pointers: fence_pointers,
            bloom_filter: bloom_filter,
        }
    }

    pub fn get(&self, key: &i32, record: &mut Record) -> bool {
        // if bloom filter does not contain key or key bigger than max fence pointer
        if !self.bloom_filter.contains(key) || key > self.fence_pointers.last().unwrap() {
            return false;
        }
        let file_offset = match binary_search_fp(&self.fence_pointers, &key) {
            Some(idx) => {
                if idx == self.fence_pointers.len() - 1 {
                    idx - 1
                } else {
                    idx
                }
            },
            None => {
                return false;
            },
        };
        
        GET_IO_COUNTER.inc();
        let records = self.read_file(file_offset * CONFIGURATION.BLOCK_SIZE, CONFIGURATION.BLOCK_SIZE);
        let idx = records.binary_search_by_key(key, |&record| record.key);
        let idx = match idx {
            Err(_) => {
                return false
            },
            Ok(v) => v,
        };
        *record = records[idx];
        return true;
    }

    pub fn read_all_file_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![0; self.size];
        let bytes_read = self.read_file_to_buffer(0, &mut buffer);
        assert!(bytes_read == self.size);
        buffer
    }

    pub fn read_all_file_records(&self) -> Vec<Record> {
        let records = bytes_to_records(&self.read_all_file_bytes());
        assert!(records[0].key == self.fence_pointers[0]);
        assert!(records.last().unwrap().key == *self.fence_pointers.last().unwrap());
        assert!(records.len() * CONFIGURATION.RECORD_SIZE == self.size);
        records
    }

    pub fn read_file(&self, start_offset: usize, num_bytes: usize) -> Vec<Record> {
        let mut buffer = vec![0; num_bytes];
        let bytes_read = self.read_file_to_buffer(start_offset, &mut buffer);
        let records = bytes_to_records(&buffer[..bytes_read]);
        let num_records = bytes_read / CONFIGURATION.RECORD_SIZE;
        assert!(num_records == records.len());
        records
    }

    pub fn read_file_to_buffer(&self, start_offset: usize, buffer: &mut[u8]) -> usize {
        let mut f = File::open(&self.filename).expect(&format!("Failed to open file {} for disk read", &self.filename));
        f.seek(SeekFrom::Start(start_offset as u64)).expect("Failed to seek file");
        f.read(&mut buffer[..]).expect("Failed to read file")
    }

    // private helper function
    fn init_bloom_filter(size: usize) -> BloomFilter {
        let bf_records = size / CONFIGURATION.RECORD_SIZE;
        let bf_bits = CONFIGURATION.BF_BITS_PER_ENTRY * bf_records;
        let bf_hashes = bloom::optimal_num_hashes(bf_bits, bf_records as u32);
        BloomFilter::with_size(bf_bits, bf_hashes)
    }
}