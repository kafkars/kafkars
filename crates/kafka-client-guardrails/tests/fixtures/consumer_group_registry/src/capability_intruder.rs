//! Runtime, transport, policy, and per-entry host theft forbidden by this fixture.

use kafka_driver as raw_driver;
use std::{
    future::Future,
    sync::{Condvar, Mutex, RwLock},
};

struct GroupOffsetCommitHost;

fn intrude(
    _driver_owner: crate::driver::Owner,
    _host_owner: crate::host::Owner,
    _protocol_owner: crate::protocol::Owner,
    _driver: raw_driver::Driver,
    _wire: kafka_wire::Metadata,
    _wire_core: kafka_wire_core::DecodeError,
    _records: kafka_wire_records::RecordBatch,
    _tokio: tokio::runtime::Runtime,
    _async_std: async_std::task::JoinHandle<()>,
    _smol: smol::Task<()>,
    _future: &dyn Future<Output = ()>,
    _network: std::net::TcpStream,
    _thread: std::thread::Thread,
    _condvar: Condvar,
    _mutex: Mutex<()>,
    _rwlock: RwLock<()>,
    _callback: Callback,
    _retry: Retry,
    _host: GroupOffsetCommitHost,
) {
    let _now = std::time::Instant::now();
    invalidate();
}

async fn hidden_executor() {}

fn remove_retained_entries(entries: &mut Vec<u64>) {
    entries.remove(0);
    entries.swap_remove(0);
    entries.retain(|_entry| true);
    entries.pop();
    entries.clear();
    entries.drain(..);
}
