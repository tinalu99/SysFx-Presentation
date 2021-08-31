extern crate prometheus;
extern crate log;
extern crate env_logger;

//use hybrid_de::lib_helper;
use hybrid_de::lib_test;
use hybrid_de::lib_workload;
use prometheus::{TextEncoder, Encoder};
use log::{info, debug};
use threadpool::ThreadPool;
use hybrid_de::configuration::CONFIGURATION;

fn main() 
{
    let pool = ThreadPool::new(CONFIGURATION.CPUS);
    env_logger::init();

    debug!("\t\tPlease wait ... Generating workload!");
    let mut workload_file: std::string::String = "workload.txt".to_string();
    let mut bulkwrite_file: std::string::String = "bulkwrite.txt".to_string();
    lib_workload::set_workload_specifications(true, true, true, 1_000_000, 10_000, 10_000, i32::min_value(), i32::max_value(), &mut workload_file, &mut bulkwrite_file);
    //lib_helper::run_with_time(&bulkwrite_file, &workload_file, pool);
    lib_test::test_regular_workload(pool);
    
    // output metrics
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();

    // Gather the metrics.
    let metric_families = prometheus::gather();
    // Encode them to send.
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let output = String::from_utf8(buffer.clone()).unwrap();
    info!("{}", output);
}
