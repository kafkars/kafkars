//! Renamed record-batch imports remain confined to the protocol adapter.

use kafka_wire_records::RecordBatch as Batch;

fn retain(_: Batch) {}
