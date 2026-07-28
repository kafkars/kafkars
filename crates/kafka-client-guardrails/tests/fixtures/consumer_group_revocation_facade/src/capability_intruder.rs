//! Capabilities forbidden from classic-group event facades.

use kafka_client_core::Machine;
use kafka_client_engine::Engine;
use kafka_driver::Driver;
use std::{
    net::TcpStream,
    thread::JoinHandle,
    time::{Instant, SystemTime},
};

fn intrude(
    _callback: Callback,
    _core: Machine,
    _driver: Driver,
    _engine: Engine,
    _instant: Instant,
    _join: JoinHandle<()>,
    _retry: Retry,
    _socket: TcpStream,
    _system: SystemTime,
    _wire: kafka_wire::JoinGroupRequest,
    _wire_core: kafka_wire_core::JoinGroupRequest,
    _wire_records: kafka_wire_records::RecordBatch,
    _async_std: async_std::Task,
    _smol: smol::Task<()>,
    _tokio: tokio::task::JoinHandle<()>,
) {
}

async fn hidden_executor() {}
