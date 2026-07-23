//! Wire access remains forbidden elsewhere in the shared driver adapter.

use kafka_wire::ProduceRequest;

fn retain(_: ProduceRequest) {}
