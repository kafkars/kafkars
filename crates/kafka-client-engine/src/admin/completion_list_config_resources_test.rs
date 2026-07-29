//! API-key 74 v1 typed publication evidence for the shared admin notifier.

use std::num::NonZeroI16;

use kafka_client_core::{ListConfigResourcesBrokerError, ListConfigResourcesTerminal};

use super::{completion_test::exercise_terminal, test_support::completion_owner};

#[test]
fn shared_worker_publishes_list_config_resources_off_reactor() {
    let reactor = std::thread::current().id();
    let (mut notifier, ports) = completion_owner();
    let worker = notifier
        .thread_id()
        .unwrap_or_else(|| panic!("shared admin notifier must own one worker"));
    assert_ne!(worker, reactor);

    exercise_terminal(
        ports.list_config_resources,
        ListConfigResourcesTerminal::BrokerRejected(ListConfigResourcesBrokerError::new(
            0,
            NonZeroI16::MIN,
        )),
        worker,
    );
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop shared notifier: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
