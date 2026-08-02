//! Pre-reserved close terminal publication scenarios.

use super::completion::{
    GroupConsumerCloseAuthority, GroupConsumerCloseAuthorityClaim, GroupConsumerCloseCompletion,
    GroupConsumerCloseCompletionObservation, GroupConsumerCloseTerminal,
    GroupConsumerCloseTerminalFailure, GroupConsumerCloseTerminalFailureKind,
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

#[test]
fn close_authority_keeps_the_first_boundary_and_one_completion() {
    let authority = GroupConsumerCloseAuthority::new();
    let first = super::super::registry_test_support::deadline(17);
    let later = super::super::registry_test_support::deadline(29);
    assert!(authority.request(first));
    assert!(!authority.request(later));

    let GroupConsumerCloseAuthorityClaim::Start {
        deadline,
        completion,
    } = authority.claim_requested()
    else {
        panic!("first request must transfer");
    };
    assert_eq!(deadline, first);
    let GroupConsumerCloseAuthorityClaim::Observe {
        completion: observed,
    } = authority.claim_explicit(later)
    else {
        panic!("explicit close must observe the transferred request");
    };
    assert!(std::sync::Arc::ptr_eq(&completion, &observed));
}
