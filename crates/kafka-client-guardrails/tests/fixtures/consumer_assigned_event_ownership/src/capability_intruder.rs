//! Deliberately reaches foreign domains, transport, and async runtimes.

use crate::admin;
use crate::clock;
use crate::completion;
use crate::driver;
use crate::producer;
use crate::protocol;
use crate::transaction;
use kafka_driver;
use kafka_wire;
use kafka_wire_core;
use kafka_wire_records;
use std::future::Future;
use std::net::TcpStream;
use std::thread;
use std::time::{Instant, SystemTime};

async fn violate() {
    let _admin = admin;
    let _clock = clock;
    let _completion = completion;
    let _driver = driver;
    let _producer = producer;
    let _protocol = protocol;
    let _transaction = transaction;
    let _driver_crate = kafka_driver;
    let _wire = kafka_wire;
    let _wire_core = kafka_wire_core;
    let _wire_records = kafka_wire_records;
    let _tokio = tokio::spawn;
    let _async_std = async_std::task::spawn;
    let _smol = smol::spawn;
    let _future = Future;
    let _socket = TcpStream;
    let _thread = thread::spawn;
    let _instant = Instant::now();
    let _system = SystemTime::now();
    let _callback = Callback;
    let _metadata = Metadata;
    let _retry = Retry;
    let _transport = Transport;
}
