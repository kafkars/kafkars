//! Exact-once seek terminal publication tests.

use super::{
    GroupConsumerSeekCompletion, GroupConsumerSeekCompletionObservation, GroupConsumerSeekTerminal,
    GroupConsumerSeekTerminalFailure, GroupConsumerSeekTerminalFailureKind,
};

#[test]
fn first_terminal_wins_and_preserves_signed_broker_code() {
    let completion = GroupConsumerSeekCompletion::pending();
    let broker = GroupConsumerSeekTerminal::Failed(GroupConsumerSeekTerminalFailure {
        kind: GroupConsumerSeekTerminalFailureKind::BrokerRejected,
        broker_code: Some(-731),
    });

    assert!(completion.publish(broker));
    assert!(!completion.publish(GroupConsumerSeekTerminal::Succeeded));
    assert_eq!(
        completion.observe(),
        GroupConsumerSeekCompletionObservation::Terminal(broker)
    );
}
