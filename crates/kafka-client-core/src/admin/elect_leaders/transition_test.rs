//! Focused lifecycle tests for the single-attempt election machine.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual transition failures"
)]

use super::{
    ElectLeadersBatch, ElectLeadersEffect, ElectLeadersFailureKind, ElectLeadersInput,
    ElectLeadersMachine, ElectLeadersPlan, ElectLeadersTerminal, LeaderElectionOutcome,
    LeaderElectionTarget, LeaderElectionType,
};
use crate::{Deadline, DeliveryStatus, Moment, OperationId};

#[test]
fn successful_response_preserves_requested_identity_order() {
    let mut machine = machine();
    let start = machine
        .apply(ElectLeadersInput::Start {
            now: Moment::from_tick(10),
        })
        .expect("start");
    assert!(matches!(
        start.into_effect(),
        Some(ElectLeadersEffect::Submit { .. })
    ));
    machine
        .apply(ElectLeadersInput::DriverAccepted)
        .expect("accepted");
    let terminal = machine
        .apply(ElectLeadersInput::BrokerResponded {
            batch: ElectLeadersBatch::new(
                7,
                vec![LeaderElectionOutcome::elected("orders".into(), 1)],
            ),
        })
        .expect("response")
        .into_effect();
    assert!(matches!(
        terminal,
        Some(ElectLeadersEffect::Complete {
            terminal: ElectLeadersTerminal::Elected(_),
            ..
        })
    ));
}

#[test]
fn all_partitions_submit_retains_selection_and_original_deadline() {
    let mut machine = ElectLeadersMachine::new(
        OperationId::from_raw(11),
        Deadline::from_tick(100),
        ElectLeadersPlan::all(LeaderElectionType::Unclean),
    );
    let effect = machine
        .apply(ElectLeadersInput::Start {
            now: Moment::from_tick(10),
        })
        .expect("start")
        .into_effect();
    let Some(ElectLeadersEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = effect
    else {
        panic!("expected submission");
    };

    assert_eq!(operation_id, OperationId::from_raw(11));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert!(plan.selection().selected_targets().is_none());
}

#[test]
fn all_partitions_accepts_only_canonical_cluster_order() {
    let mut canonical = all_machine();
    start_and_accept(&mut canonical);
    let effect = canonical
        .apply(ElectLeadersInput::BrokerResponded {
            batch: ElectLeadersBatch::new(
                7,
                vec![
                    LeaderElectionOutcome::elected("audit".into(), 2),
                    LeaderElectionOutcome::elected("orders".into(), 0),
                    LeaderElectionOutcome::elected("orders".into(), 1),
                ],
            ),
        })
        .expect("canonical response")
        .into_effect();
    assert!(matches!(
        effect,
        Some(ElectLeadersEffect::Complete {
            terminal: ElectLeadersTerminal::Elected(_),
            ..
        })
    ));

    let mut unordered = all_machine();
    start_and_accept(&mut unordered);
    let effect = unordered
        .apply(ElectLeadersInput::BrokerResponded {
            batch: ElectLeadersBatch::new(
                7,
                vec![
                    LeaderElectionOutcome::elected("orders".into(), 0),
                    LeaderElectionOutcome::elected("audit".into(), 2),
                ],
            ),
        })
        .expect("malformed response becomes terminal")
        .into_effect();
    let Some(ElectLeadersEffect::Complete {
        terminal: ElectLeadersTerminal::Failed(failure),
        ..
    }) = effect
    else {
        panic!("expected invalid-response failure");
    };
    assert_eq!(failure.kind(), &ElectLeadersFailureKind::InvalidResponse);
}

#[test]
fn all_partitions_accepts_an_empty_cluster_result() {
    let mut machine = all_machine();
    start_and_accept(&mut machine);
    let effect = machine
        .apply(ElectLeadersInput::BrokerResponded {
            batch: ElectLeadersBatch::new(0, Vec::new()),
        })
        .expect("empty cluster response")
        .into_effect();

    assert!(matches!(
        effect,
        Some(ElectLeadersEffect::Complete {
            terminal: ElectLeadersTerminal::Elected(_),
            ..
        })
    ));
}

#[test]
fn submitted_transport_failure_keeps_driver_certainty() {
    let mut machine = machine();
    machine
        .apply(ElectLeadersInput::Start {
            now: Moment::from_tick(10),
        })
        .expect("start");
    machine
        .apply(ElectLeadersInput::DriverAccepted)
        .expect("accepted");
    let terminal = machine
        .apply(ElectLeadersInput::TransportFailed {
            delivery: DeliveryStatus::PossiblySent,
        })
        .expect("failure")
        .into_effect();
    let Some(ElectLeadersEffect::Complete {
        terminal: ElectLeadersTerminal::Failed(failure),
        ..
    }) = terminal
    else {
        panic!("expected failure");
    };
    assert_eq!(failure.kind(), &ElectLeadersFailureKind::Transport);
    assert_eq!(failure.delivery(), DeliveryStatus::PossiblySent);
}

fn machine() -> ElectLeadersMachine {
    let plan = ElectLeadersPlan::new(
        LeaderElectionType::Preferred,
        vec![LeaderElectionTarget::new("orders".into(), 1)],
    )
    .expect("valid plan");
    ElectLeadersMachine::new(OperationId::from_raw(9), Deadline::from_tick(100), plan)
}

fn all_machine() -> ElectLeadersMachine {
    ElectLeadersMachine::new(
        OperationId::from_raw(10),
        Deadline::from_tick(100),
        ElectLeadersPlan::all(LeaderElectionType::Preferred),
    )
}

fn start_and_accept(machine: &mut ElectLeadersMachine) {
    machine
        .apply(ElectLeadersInput::Start {
            now: Moment::from_tick(10),
        })
        .expect("start");
    machine
        .apply(ElectLeadersInput::DriverAccepted)
        .expect("accepted");
}
