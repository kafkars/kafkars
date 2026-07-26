//! Scenarios for facade-owned engine startup and child-handle retention.

use kafka_client_engine::ConsumerReadIsolation as EngineReadIsolation;

use super::client::{ClientEngine, engine_read_isolation};
use crate::{ReadIsolation, producer::Compression};

#[test]
fn client_bridge_retains_validated_endpoints_and_builds_a_producer() {
    let result = ClientEngine::start(vec!["127.0.0.1:1".to_owned()], Compression::None, None);
    let Ok(client) = result else {
        panic!("valid local engine configuration should start")
    };

    assert_eq!(client.bootstrap_servers(), &["127.0.0.1:1".to_owned()]);
    let _producer = client.producer();
}

#[test]
fn facade_read_isolation_maps_exhaustively_to_engine_configuration() {
    for (public, engine) in [
        (
            ReadIsolation::ReadUncommitted,
            EngineReadIsolation::ReadUncommitted,
        ),
        (
            ReadIsolation::ReadCommitted,
            EngineReadIsolation::ReadCommitted,
        ),
    ] {
        assert_eq!(engine_read_isolation(public), engine);
    }
}
