//! Accepted identity calls keep producer shutdown live after public records settle.

use std::{sync::Arc, time::Duration};

use kafka_client_core::ProducerIdentityGeneration;

use crate::EngineConfig;

use super::{EngineLifecycle, finalize, produce_turn, start};

#[test]
fn retained_identity_call_prevents_producer_quiescence_without_public_completions() {
    let config = EngineConfig::new(vec!["127.0.0.1:1".to_owned()]);
    let validated = config
        .validate()
        .unwrap_or_else(|error| panic!("validate shutdown config: {error:?}"));
    let lifecycle = Arc::new(EngineLifecycle::new());
    let (mut resources, started) = start::prepare(&config, validated, &lifecycle)
        .unwrap_or_else(|error| panic!("prepare shutdown host: {error}"));
    let capture = resources
        .clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture identity deadline: {error}"));
    let driver = resources
        .driver
        .as_ref()
        .unwrap_or_else(|| panic!("driver"));
    resources
        .producer_identity_calls
        .try_reserve()
        .unwrap_or_else(|| panic!("identity capacity"))
        .submit(
            driver,
            ProducerIdentityGeneration::initial(),
            capture.operation_deadline(),
        )
        .unwrap_or_else(|error| panic!("identity admission: {error}"));
    started.control.request_shutdown();
    let progress = produce_turn::drive(&mut resources, capture.now())
        .unwrap_or_else(|error| panic!("producer shutdown turn: {error}"));
    let public_completions = resources
        .producer
        .try_data()
        .unwrap_or_else(|error| panic!("producer shard: {error:?}"))
        .unsettled_completions();
    started.control.request_failure();
    finalize::finish_host(resources, &lifecycle);

    assert_eq!(public_completions, 0);
    assert!(
        progress.unsettled > 0,
        "driver ownership must keep the host running"
    );
}
