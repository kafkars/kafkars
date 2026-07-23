//! Wire types are permitted inside the protocol adapter.

use kafka_wire::ProduceRequest;
use kafka_wire_records::RecordBatch;

fn retain(_: ProduceRequest, _: RecordBatch) {}
