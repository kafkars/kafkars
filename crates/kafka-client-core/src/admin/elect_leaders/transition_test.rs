//! Focused lifecycle tests for the single-attempt election machine.

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
