//! Foreign capabilities forbidden in assignment topic ownership.

use kafka_driver as raw_driver;
use std::{
    future::Future,
    sync::{Condvar, Mutex, RwLock},
};

fn intrude(
    _driver: raw_driver::Driver,
    _wire: kafka_wire::Metadata,
    _wire_core: kafka_wire_core::DecodeError,
    _records: kafka_wire_records::RecordBatch,
    _future: &dyn Future<Output = ()>,
    _clock: crate::clock::Owner,
    _network: std::net::TcpStream,
    _thread: std::thread::Thread,
    _condvar: Condvar,
    _mutex: Mutex<()>,
    _rwlock: RwLock<()>,
    _transport: Transport,
    _retry: Retry,
    _metadata: Metadata,
    _admin: crate::admin::Owner,
    _driver_owner: crate::driver::Owner,
    _producer: crate::producer::Owner,
    _protocol: crate::protocol::Owner,
    _transaction: crate::transaction::Owner,
) {
    let _now = std::time::Instant::now();
}

async fn hidden_executor() {}
