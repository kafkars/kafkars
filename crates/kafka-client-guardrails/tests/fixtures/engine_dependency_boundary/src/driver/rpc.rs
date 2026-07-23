//! The exact driver RPC join point may combine driver and wire owners.

use kafka_driver::Driver;
use kafka_wire::ProduceRequest;

fn retain(_: Driver, _: ProduceRequest) {}
