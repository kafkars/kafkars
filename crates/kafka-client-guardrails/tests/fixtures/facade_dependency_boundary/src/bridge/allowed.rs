//! Engine imports are allowed only inside the facade's private bridge.

use kafka_client_engine::Engine;

fn retain_engine_type(_: Engine) {}
