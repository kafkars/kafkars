//! Scenarios for facade-owned engine startup and child-handle retention.

use super::client::ClientEngine;

#[test]
fn client_bridge_retains_validated_endpoints_and_builds_a_producer() {
    let result = ClientEngine::start(vec!["127.0.0.1:1".to_owned()]);
    let Ok(client) = result else {
        panic!("valid local engine configuration should start")
    };

    assert_eq!(client.bootstrap_servers(), &["127.0.0.1:1".to_owned()]);
    let _producer = client.producer();
}
