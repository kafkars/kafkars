//! Forbidden public transaction dependency on the engine.

use kafka_client_engine::Engine;

fn steal(_engine: Engine) {}
