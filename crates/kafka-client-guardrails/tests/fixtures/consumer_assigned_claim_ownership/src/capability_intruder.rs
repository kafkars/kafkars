//! Deliberately imports runtimes, transport, clocks, and sibling policy.

use kafka_client_core;
use kafka_driver;
use kafka_wire;
use kafka_wire_core;
use kafka_wire_records;
use std::{future::Future, net, thread, time};

async fn violate() {
    let _admin = crate::admin;
    let _driver = crate::driver;
    let _producer = crate::producer;
    let _protocol = crate::protocol;
    let _transaction = crate::transaction;
    let _core = kafka_client_core;
    let _raw_driver = kafka_driver;
    let _wire = kafka_wire;
    let _wire_core = kafka_wire_core;
    let _wire_records = kafka_wire_records;
    let _tokio = tokio::spawn;
    let _async_std = async_std::task::spawn;
    let _smol = smol::spawn;
    let _future = Future;
    let _net = net;
    let _thread = thread;
    let _time = time;
    let _callback = Callback;
    let _metadata = Metadata;
    let _retry = Retry;
    let _stream = Stream;
    let _transport = Transport;
}
