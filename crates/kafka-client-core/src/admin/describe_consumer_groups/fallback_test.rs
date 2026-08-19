//! Explicit one-time KIP-848 to classic fallback transitions.

#![expect(
    clippy::expect_used,
    reason = "test fixtures require contextual transition failures"
)]

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminClassicConsumerGroupDetails, AdminConsumerGroupDescription,
    AdminConsumerGroupDescriptionDetails, AdminConsumerGroupDescriptionOutcome,
    AdminConsumerGroupDescriptionResult, AdminDescribeConsumerGroupsCallKind,
    AdminDescribeConsumerGroupsEffect, AdminDescribeConsumerGroupsFailureKind,
    AdminDescribeConsumerGroupsInput, AdminDescribeConsumerGroupsMachine,
    AdminDescribeConsumerGroupsMachineError, AdminDescribeConsumerGroupsPlan,
    AdminDescribeConsumerGroupsTerminal,
};

#[test]
fn fallback_reuses_identity_group_deadline_and_then_resets_next_group_to_modern() {
    let mut machine = machine(vec!["alpha", "beta"]);
    let modern = start(&mut machine);
    assert_submit(
        modern,
        "alpha",
        AdminDescribeConsumerGroupsCallKind::Consumer,
    );
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .expect("accept modern");
    let classic = machine
        .apply(AdminDescribeConsumerGroupsInput::FallbackToClassic {
            throttle_time_ms: 13,
            delivery: DeliveryStatus::PossiblySent,
        })
        .expect("fallback")
        .into_effect();
    assert_submit(
        classic,
        "alpha",
        AdminDescribeConsumerGroupsCallKind::ClassicFallback,
    );
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .expect("accept classic");
    let next = machine
        .apply(AdminDescribeConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 5,
            outcome: described("alpha"),
        })
        .expect("classic response")
        .into_effect();
    assert_submit(next, "beta", AdminDescribeConsumerGroupsCallKind::Consumer);
}

#[test]
fn classic_fallback_cannot_fallback_again() {
    let mut machine = machine(vec!["alpha"]);
    drop(start(&mut machine));
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .and_then(|_| {
            machine.apply(AdminDescribeConsumerGroupsInput::FallbackToClassic {
                throttle_time_ms: 0,
                delivery: DeliveryStatus::NotSent,
            })
        })
        .and_then(|_| machine.apply(AdminDescribeConsumerGroupsInput::DriverAccepted))
        .expect("enter submitted classic fallback");
    assert_eq!(
        machine.apply(AdminDescribeConsumerGroupsInput::FallbackToClassic {
            throttle_time_ms: 0,
            delivery: DeliveryStatus::PossiblySent,
        }),
        Err(AdminDescribeConsumerGroupsMachineError::InvalidState)
    );
}

#[test]
fn fallback_failure_preserves_modern_throttle_and_aggregate_delivery() {
    let mut machine = machine(vec!["alpha", "beta", "gamma"]);
    drop(start(&mut machine));
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .and_then(|_| {
            machine.apply(AdminDescribeConsumerGroupsInput::BrokerResponded {
                throttle_time_ms: 3,
                outcome: described("alpha"),
            })
        })
        .and_then(|_| machine.apply(AdminDescribeConsumerGroupsInput::DriverAccepted))
        .and_then(|_| {
            machine.apply(AdminDescribeConsumerGroupsInput::FallbackToClassic {
                throttle_time_ms: 17,
                delivery: DeliveryStatus::PossiblySent,
            })
        })
        .and_then(|_| machine.apply(AdminDescribeConsumerGroupsInput::DriverAccepted))
        .expect("enter submitted classic fallback");
    let terminal = machine
        .apply(AdminDescribeConsumerGroupsInput::TransportFailed {
            delivery: DeliveryStatus::NotSent,
        })
        .expect("classic transport failure")
        .into_effect();
    let Some(AdminDescribeConsumerGroupsEffect::Complete {
        terminal: AdminDescribeConsumerGroupsTerminal::Described(batch),
        ..
    }) = terminal
    else {
        panic!("missing partial terminal");
    };
    let (throttle, outcomes) = batch.into_parts();
    assert_eq!(throttle, 17);
    let (_, beta) = outcomes[1].clone().into_parts();
    let AdminConsumerGroupDescriptionResult::OperationFailed(beta) = beta else {
        panic!("beta was not a mechanism failure");
    };
    assert_eq!(
        beta.kind(),
        AdminDescribeConsumerGroupsFailureKind::Transport
    );
    assert_eq!(beta.delivery(), DeliveryStatus::PossiblySent);
    let (_, gamma) = outcomes[2].clone().into_parts();
    let AdminConsumerGroupDescriptionResult::OperationFailed(gamma) = gamma else {
        panic!("gamma was not explicitly unattempted");
    };
    assert_eq!(
        gamma.kind(),
        AdminDescribeConsumerGroupsFailureKind::NotAttempted
    );
    assert_eq!(gamma.delivery(), DeliveryStatus::NotSent);
}

fn machine(groups: Vec<&str>) -> AdminDescribeConsumerGroupsMachine {
    AdminDescribeConsumerGroupsMachine::new(
        OperationId::from_raw(23),
        Deadline::from_tick(100),
        AdminDescribeConsumerGroupsPlan::new(
            groups.into_iter().map(str::to_owned).collect(),
            false,
        )
        .expect("valid plan"),
    )
}

fn start(
    machine: &mut AdminDescribeConsumerGroupsMachine,
) -> Option<AdminDescribeConsumerGroupsEffect> {
    machine
        .apply(AdminDescribeConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        })
        .expect("start")
        .into_effect()
}

fn assert_submit(
    effect: Option<AdminDescribeConsumerGroupsEffect>,
    expected_group: &str,
    expected_kind: AdminDescribeConsumerGroupsCallKind,
) {
    let Some(AdminDescribeConsumerGroupsEffect::Submit {
        operation_id,
        deadline,
        group_id,
        call_kind,
        ..
    }) = effect
    else {
        panic!("missing submission");
    };
    assert_eq!(operation_id, OperationId::from_raw(23));
    assert_eq!(deadline, Deadline::from_tick(100));
    assert_eq!(group_id, expected_group);
    assert_eq!(call_kind, expected_kind);
}

fn described(group_id: &str) -> AdminConsumerGroupDescriptionOutcome {
    AdminConsumerGroupDescriptionOutcome::described(
        group_id.to_owned(),
        AdminConsumerGroupDescription::new(
            "Stable".to_owned(),
            AdminConsumerGroupDescriptionDetails::Classic(AdminClassicConsumerGroupDetails::new(
                "consumer".to_owned(),
                "range".to_owned(),
            )),
            Vec::new(),
            None,
        ),
    )
}
