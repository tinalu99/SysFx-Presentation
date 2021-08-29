use lazy_static::lazy_static;
use std::env::args;
use log::{info};

lazy_static! {
    // Global configuration variable.
    // Lazy-static works by creating one-off types that wrap your value and provide
    // thread-safe single initialization guarantees:
    //     For a given static ref NAME: TYPE = EXPR;,
    // the macro generates a unique type that implements Deref<TYPE> and stores it
    // in a static with name NAME. This wrapper type does not implement your trait,
    // (e.g. Debug) only the wrapped type does. Hence to print using the Debug
    // format, use `*CONFIGURATION`.
    pub static ref CONFIGURATION: Configuration = Configuration::new();
}

/// Represents the final, global configuration of the program.
#[derive(Debug)]
#[allow(non_snake_case)]
pub struct Configuration {
    pub BLOCK_SIZE: usize,
	pub FILE_SIZE: usize,
	pub KEY_SIZE: usize,
	pub RECORD_SIZE: usize,
	pub BUFFER_CAPACITY: usize,
	pub SIZE_RATIO: usize, // T
    pub RUNS_PER_LEVEL: usize, // K
    pub RUNS_LAST_LEVEL: usize, // Z

    pub CPUS: usize,
    
    pub COMPACTION_STRATEGY: &'static str, // options: partial, full
    pub RUN_STRATEGY: &'static str, // options: first, last_full, fullest
    pub FILE_STRATEGY: &'static str, // options: oldest_merged, oldest_flushed, dense_fp, sparse_fp, choose_first

    pub T_OVER_K: f64, // T/K, storing this in configuration to avoid calculating it each time
    pub FULL_THRESHOLD: f64, // if the size/capacity of a run is below the full_threshold, we will merge into the run
    pub PC_FULL_THRESHOLD: f64,
    
	// we eventually want to change this(?) with Monkey
	pub BF_BITS_PER_ENTRY: usize,
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            BLOCK_SIZE: 4096, // 4 KB 
            FILE_SIZE: 24576,//24576, // 24 KB 
            KEY_SIZE: 4,
            RECORD_SIZE: 8, // 4+4 byte
            BUFFER_CAPACITY: 24576, // 24 KBs
            SIZE_RATIO: 4, 
            RUNS_PER_LEVEL: 1,
            RUNS_LAST_LEVEL: 1,
            CPUS: 2,
            COMPACTION_STRATEGY: "full", 
            FILE_STRATEGY: "oldest_merged", 
            RUN_STRATEGY: "first", 
            T_OVER_K: 1.0, 
            FULL_THRESHOLD: 0.75,
            PC_FULL_THRESHOLD: 0.75,

            BF_BITS_PER_ENTRY: 10,
        }
    }
}

impl Configuration {
    fn new() -> Self {
        let mut default = Configuration::default();
        let args: Vec<String> = args().collect();
        if args.len() == 3 {
            default.SIZE_RATIO = args[1].parse::<usize>().unwrap();
            default.RUNS_PER_LEVEL = args[2].parse::<usize>().unwrap();
        }
        default.T_OVER_K = default.SIZE_RATIO as f64 / default.RUNS_PER_LEVEL as f64;
        if default.COMPACTION_STRATEGY == "full" {
            default.PC_FULL_THRESHOLD = 1.0;
        } else {
            default.FULL_THRESHOLD = 1.0;
        }
        info!("size ratio is {}, runs per level is {}, compaction strategy is {}", default.SIZE_RATIO, default.RUNS_PER_LEVEL, default.COMPACTION_STRATEGY);
        default
    }
}