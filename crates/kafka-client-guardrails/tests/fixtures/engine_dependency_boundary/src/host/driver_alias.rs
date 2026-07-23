//! Aliasing does not permit driver access from the engine host.

use kafka_driver as transport;

fn retain(_: transport::Reactor) {}
