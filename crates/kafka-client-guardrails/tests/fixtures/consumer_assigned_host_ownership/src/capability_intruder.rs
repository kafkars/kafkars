//! Deliberately reaches raw transport, clocks, threads, and async runtimes.

use kafka_driver;
use kafka_wire;
use std::thread;
use std::time::{Instant, SystemTime};

async fn raw_runtime() {
    let _driver = kafka_driver;
    let _wire = kafka_wire;
    let _tokio = tokio::spawn;
    let _async_std = async_std::task::spawn;
    let _smol = smol::spawn;
    let _thread = thread::spawn;
    let _instant = Instant::now();
    let _system = SystemTime::now();
    let _callback = Callback;
    let _metadata = Metadata;
    let _retry = Retry;
}
