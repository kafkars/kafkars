//! Deliberately reaches runtime, raw Kafka, and sibling-domain capabilities.

use kafka_driver as raw_driver;
use std::{future::Future, sync::Mutex, thread, time::Instant};

fn violate(
    _driver: raw_driver::Driver,
    _wire: kafka_wire::FetchRequest,
    _wire_core: kafka_wire_core::DecodeError,
    _records: kafka_wire_records::RecordBatch,
    _future: &dyn Future<Output = ()>,
    _clock: crate::clock::Clock,
    _port: crate::driver::Port,
    _producer: crate::producer::Policy,
    _lock: Mutex<()>,
) {
    let _now = Instant::now();
    let _thread = thread::current();
    let _runtime = tokio::runtime::Handle::current();
}

async fn hidden_timer_executor() {}
