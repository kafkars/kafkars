//! Deliberately reaches raw transport, clock, async, and sibling policy.

use kafka_driver as raw_driver;
use std::future::Future;

fn reaches_forbidden_capabilities(
    _driver: raw_driver::Driver,
    _wire: kafka_wire::FetchRequest,
    _wire_core: kafka_wire_core::DecodeError,
    _records: kafka_wire_records::RecordBatch,
    _future: &dyn Future<Output = ()>,
    _transport: Transport,
    _retry: Retry,
    _metadata: Metadata,
    _admin: crate::admin::Policy,
) {
    let _now = std::time::Instant::now();
}

async fn reaches_a_hidden_executor() {}
