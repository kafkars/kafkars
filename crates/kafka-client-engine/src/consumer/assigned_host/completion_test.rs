//! Assigned-consumer notifier ownership and shutdown scenarios.

use super::completion::AssignedConsumerCompletionNotifier;

#[test]
fn one_consumer_notifier_issues_a_close_port_and_stops_linearly() {
    let (mut notifier, _close) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start notifier: {error}"));

    assert!(notifier.thread_id().is_some());
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    assert!(notifier.thread_id().is_none());
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}
