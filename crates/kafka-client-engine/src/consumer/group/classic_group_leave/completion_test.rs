//! Pre-reserved close terminal publication scenarios.

use super::completion::{
    GroupConsumerCloseCompletion, GroupConsumerCloseCompletionObservation,
    GroupConsumerCloseTerminal, GroupConsumerCloseTerminalFailure,
    GroupConsumerCloseTerminalFailureKind,
};

#[test]
fn one_exact_signed_broker_terminal_is_published_once() {
    let completion = GroupConsumerCloseCompletion::pending();
    let terminal = GroupConsumerCloseTerminal::Failed(GroupConsumerCloseTerminalFailure {
        kind: GroupConsumerCloseTerminalFailureKind::BrokerRejected,
        broker_code: Some(-731),
    });

    assert!(completion.publish(terminal));
    assert!(!completion.publish(GroupConsumerCloseTerminal::Succeeded));
    assert_eq!(
        completion.observe(),
        GroupConsumerCloseCompletionObservation::Terminal(terminal)
    );
}
