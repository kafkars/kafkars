//! Public facade modules cannot import the engine directly.

use kafka_client_engine as engine;

fn retain_engine_type(_: engine::Engine) {}
