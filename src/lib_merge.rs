use std::cmp::Ordering;
use std::collections::BinaryHeap;
use log::{debug};
use std::sync::{Arc};
use std::time::{SystemTime};

use super::configuration::CONFIGURATION;
use super::lib_template::{Record};
use super::lib_helper::{generate_filename};
use super::lib_on_disk::lib_disk_file::{DiskFile};
use super::lib_on_disk::lib_disk_run::{Run};
use crate::metrics::{PUT_IO_COUNTER};

use atomic_counter::AtomicCounter;

#[derive(Eq, Hash, PartialEq)]
pub struct HeapNode {
    element: Record, // the record stored in the heap node
    run_idx: usize, // index of run of element
    next_ele_idx: usize, // index of next elementin run
}

impl HeapNode {
    pub fn create_heap_node(element: Record, run_idx: usize, next_ele_idx: usize) -> HeapNode {
        HeapNode {
            element: element,
            run_idx: run_idx,
            next_ele_idx: next_ele_idx,
        }
    }
}

impl PartialOrd for HeapNode {
    fn partial_cmp(&self, other: &HeapNode) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapNode {
    // flip orderings so priority queue becomes a min queue rather than a max queue
    fn cmp(&self, other: &HeapNode) -> Ordering {
        other.element.cmp(&self.element)
            // we want greater run indexes to have greater priority
            .then_with(|| self.run_idx.cmp(&other.run_idx))
    }
}

pub fn merge_from_files(mut files_to_merge: Vec<Vec<Arc<DiskFile>>>, run_merge_into: &Run) -> Vec<Arc<DiskFile>> {
    let mut merged_runs: Vec<u8> = Vec::new();
    let mut merged_files: Vec<Arc<DiskFile>> = Vec::new();
    let mut heap = BinaryHeap::new();
    let mut runs_to_merge = Vec::new();
    for i in 0..files_to_merge.len() {
        let file_records = files_to_merge[i][0].read_all_file_records();
        PUT_IO_COUNTER.inc_by((files_to_merge[i][0].size as f64 / CONFIGURATION.BLOCK_SIZE as f64).ceil() as i64);
        let first = file_records[0];
        files_to_merge[i].remove(0);
        runs_to_merge.push(file_records);
        heap.push(HeapNode::create_heap_node(first, i, 1));
    }
    let mut last_key = 0;
    while let Some(HeapNode {element, run_idx, mut next_ele_idx}) = heap.pop() {
        if merged_runs.len() == 0 || element.key != last_key {
            merged_runs.extend(&element.key.to_be_bytes());
            merged_runs.extend(&element.value.to_be_bytes());
            last_key = element.key;
        }
        // re-fill runs_to_merge[run_idx]
        if next_ele_idx == runs_to_merge[run_idx].len() && files_to_merge[run_idx].len() > 0 {
            runs_to_merge[run_idx] = files_to_merge[run_idx][0].read_all_file_records();
            PUT_IO_COUNTER.inc_by((files_to_merge[run_idx][0].size as f64 / CONFIGURATION.BLOCK_SIZE as f64).ceil() as i64);
            files_to_merge[run_idx].remove(0);
            next_ele_idx = 0;
        }
        // add the next element in runs_to_merge[run_idx] to heap
        if next_ele_idx < runs_to_merge[run_idx].len() {
            let next_element = runs_to_merge[run_idx][next_ele_idx];
            assert!(next_element.key >= last_key);
            let new_node = HeapNode::create_heap_node(next_element, run_idx, next_ele_idx + 1);
            heap.push(new_node);
        }
        // write file out
        if merged_runs.len() == CONFIGURATION.FILE_SIZE {
            let merged_file = DiskFile::create_disk_file(generate_filename(run_merge_into.level, run_merge_into.run, run_merge_into.file_counter.get()), &merged_runs, merged_runs.len());
            PUT_IO_COUNTER.inc_by((merged_file.size as f64 / CONFIGURATION.BLOCK_SIZE as f64).ceil() as i64);
            merged_files.push(Arc::new(merged_file));
            run_merge_into.file_counter.inc();
            merged_runs.clear();
        } 
    }
    // write rest of data out into file
    if merged_runs.len() > 0 {
        let merged_file = DiskFile::create_disk_file(generate_filename(run_merge_into.level, run_merge_into.run, run_merge_into.file_counter.get()), &merged_runs, merged_runs.len());
        PUT_IO_COUNTER.inc_by((merged_file.size as f64 / CONFIGURATION.BLOCK_SIZE as f64).ceil() as i64);
        merged_files.push(Arc::new(merged_file));
        run_merge_into.file_counter.inc();
        merged_runs.clear();
    }
    merged_files
}

// pub fn merge_k_sorted(vectors_to_merge: Vec<Vec<Record>>) -> Vec<Record> {
//     let mut merged_vectors: Vec<Record> = Vec::new();
//     let mut heap = BinaryHeap::new();
//     for i in 0..vectors_to_merge.len() {
//         heap.push(HeapNode::create_heap_node(vectors_to_merge[i][0], i, 1)); // Store the first element of each vector in heap
//     }
//     let mut last_key = 0;
//     while let Some(HeapNode {element, run_idx, next_ele_idx}) = heap.pop() {
//         if merged_vectors.len() == 0 || element.key != last_key {
//             merged_vectors.push(element);
//             last_key = element.key;
//         }
//         if next_ele_idx < vectors_to_merge[run_idx].len() {
//             let next_element = vectors_to_merge[run_idx][next_ele_idx];
//             let new_node = HeapNode::create_heap_node(next_element, run_idx, next_ele_idx + 1);
//             heap.push(new_node);
//         }
//     }
//     merged_vectors
// }
