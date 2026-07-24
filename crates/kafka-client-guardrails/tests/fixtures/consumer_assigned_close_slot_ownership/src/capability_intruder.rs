//! Deliberately reaches allocation, runtime, transport, and sibling policy.

use kafka_driver as raw_driver;
use std::future::Future;
use std::sync::Arc;

fn reaches_forbidden_capabilities(
    _driver: raw_driver::Driver,
    _wire: kafka_wire::ProduceRequest,
    _wire_core: kafka_wire_core::DecodeError,
    _records: kafka_wire_records::RecordBatch,
    _future: &dyn Future<Output = ()>,
    _map: std::collections::BTreeMap<u8, u8>,
    _socket: std::net::TcpStream,
    _join: std::thread::JoinHandle<()>,
    _instant: std::time::Instant,
    _arc: Arc<u8>,
    _boxed: Box<u8>,
    _string: String,
    _bytes: Vec<u8>,
    _engine_driver: crate::driver::Port,
    _producer: crate::producer::Policy,
) {
}

async fn reaches_a_hidden_executor() {}
