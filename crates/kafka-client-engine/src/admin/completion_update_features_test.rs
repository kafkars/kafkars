//! API-key 57 typed publication evidence for the shared admin notifier.

use kafka_client_core::{UpdateFeaturesBatch, UpdateFeaturesTerminal};

use super::{completion_test::exercise_terminal, test_support::completion_owner};

#[test]
fn shared_worker_publishes_update_features_off_reactor() {
    let reactor = std::thread::current().id();
    let (mut notifier, ports) = completion_owner();
    let worker = notifier
        .thread_id()
        .unwrap_or_else(|| panic!("shared admin notifier must own one worker"));
    assert_ne!(worker, reactor);

    exercise_terminal(
        ports.update_features,
        UpdateFeaturesTerminal::Updated(UpdateFeaturesBatch::new(0, Vec::new())),
        worker,
    );
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop shared notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
