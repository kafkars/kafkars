//! The bridge may translate engine values but cannot bypass lower layers.

use kafka_client_core::Producer;
use kafka_driver::Reactor;
use kafka_wire::ProduceRequest;
use kafka_wire_records::RecordBatch;

fn retain_forbidden_types(_: Producer, _: Reactor, _: ProduceRequest, _: RecordBatch) {}
