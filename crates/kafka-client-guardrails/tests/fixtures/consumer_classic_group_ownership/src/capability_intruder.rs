//! Capabilities forbidden from deterministic classic-group policy.

use bytes::Bytes;
use kafka_client::Client;
use kafka_client_engine::Engine as ForeignEngine;
use kafka_driver::Driver;
use kafka_wire::JoinGroupRequest;
use kafka_wire_core::DecodeError;
use kafka_wire_records::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::future::Future;
use std::io::Error as IoError;
use std::net::TcpStream;
use std::os::unix::io::RawFd;
use std::process::Child;
use std::sync::Mutex;
use std::thread;
use std::time::{Instant, SystemTime};

use crate::admin::AdminPolicy;
use crate::exports::PublicExport;
use crate::producer::ProducerPolicy;
use crate::public_api::PublicApi;
use crate::transaction::TransactionPolicy;

fn intrude(
    _admin: AdminPolicy,
    _assigned: AssignedConsumerMachine,
    _bytes: Bytes,
    _callback: Callback,
    _child: Child,
    _client: Client,
    _clock: Clock,
    _decode: DecodeError,
    _deadline: OperationDeadline,
    _driver: Driver,
    _engine: Engine,
    _file: File,
    _foreign_engine: ForeignEngine,
    _future: &dyn Future<Output = ()>,
    _generated: Generated,
    _hash_map: HashMap<(), ()>,
    _hash_set: HashSet<()>,
    _io: IoError,
    _join: JoinGroupRequest,
    _metadata: Metadata,
    _mutex: Mutex<()>,
    _producer: ProducerPolicy,
    _public_api: PublicApi,
    _public_export: PublicExport,
    _raw_byte: u8,
    _record_batch: RecordBatch,
    _retry: Retry,
    _raw_fd: RawFd,
    _runtime: Runtime,
    _stream: TcpStream,
    _text: String,
    _text_view: &str,
    _transaction: TransactionPolicy,
    _transport: Transport,
    _wire: Wire,
    _async_std: async_std::Task,
    _smol: smol::Task<()>,
    _tokio: tokio::runtime::Runtime,
) {
    let _environment = std::env::var_os("CLASSIC_GROUP_FIXTURE");
    let _instant = Instant::now();
    let _system = SystemTime::now();
    let _thread = thread::current();
}

async fn hidden_executor() {}
