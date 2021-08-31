//use std::mem;
use log::{debug};

use std::collections::{HashMap, HashSet};
use std::i32;
use crate::configuration::CONFIGURATION;
use super::lib_template::{Record};

use super::lib_helper::{bytes_to_records};

#[allow(non_upper_case_globals)]

pub struct MemoryBuffer {
	pub buffer_size: usize,
	pub buffer: HashMap<i32, i32>,
	level: usize,
}

impl MemoryBuffer {
	pub fn create_buffer() -> MemoryBuffer {
		MemoryBuffer {
			buffer_size: CONFIGURATION.BUFFER_CAPACITY/CONFIGURATION.RECORD_SIZE, // #elements in the in-memory buffer
			buffer: HashMap::new(),
			level: 0,
		}
	}

	pub fn put(&mut self, key: &i32, value: &i32) {
		self.buffer.insert(*key, *value);
	}

	pub fn get(&self, key: &i32, record: &mut Record) -> bool {
		match self.buffer.get(key) {
			Some(&value) => {
				*record = Record::create_record(*key, value);
				return true;
			}
			_ => return false,
		}
	}

	pub fn merge(&self) -> Vec<Record> {
		let mut records = Vec::new();
		let mut buffer_data: Vec<_> = self.buffer.clone().into_iter().collect();
		buffer_data.sort();
		for (key, value) in buffer_data.iter() {
			records.push(Record::create_record(*key, *value));
		}
		records
	}

	pub fn flush(&mut self, data: Vec<u8>, _capacity: usize) {
		let records = bytes_to_records(&data);
		for record in records.iter() {
			self.buffer.insert(record.key, record.value);
		}
	}

	pub fn size(&self) -> usize {
		self.buffer.len() * CONFIGURATION.RECORD_SIZE
	}

	pub fn capacity(&self) -> usize {
		CONFIGURATION.BUFFER_CAPACITY
	}

	pub fn level(&self) -> usize { 
		self.level 
	}

    	pub fn is_full(&self) -> bool {
		assert!(self.buffer.len() <= self.buffer_size);
		self.buffer.len() >= self.buffer_size
	}

	pub fn clear(&mut self) { 
		self.buffer.clear();
	}

	pub fn print_stats(&self, distinct_keys: &mut HashSet<i32>) {
		for (key, &value) in self.buffer.iter() {
			debug!("{}:{}:L{}", key, value, self.level);
			distinct_keys.insert(*key);
		}
		debug!("\n");
	}
}
