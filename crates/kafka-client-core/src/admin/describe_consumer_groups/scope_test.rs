//! Direct-classic selection and modern-first compatibility evidence.

use crate::{Deadline, DeliveryStatus, Moment, OperationId};

use super::{
    AdminClassicConsumerGroupDetails, AdminConsumerGroupDescription,
    AdminConsumerGroupDescriptionDetails, AdminConsumerGroupDescriptionOutcome,
    AdminConsumerGroupDescriptionResult, AdminDescribeConsumerGroupsCallKind,
    AdminDescribeConsumerGroupsEffect, AdminDescribeConsumerGroupsFailureKind,
    AdminDescribeConsumerGroupsInput, AdminDescribeConsumerGroupsMachine,
    AdminDescribeConsumerGroupsMachineError, AdminDescribeConsumerGroupsPlan,
    AdminDescribeConsumerGroupsScope, AdminDescribeConsumerGroupsTerminal,
};

#[test]
fn existing_constructor_remains_modern_first_with_one_classic_compatibility_path() {
    let plan = AdminDescribeConsumerGroupsPlan::new(vec!["alpha".to_owned()], false)
        .unwrap_or_else(|error| panic!("plan: {error}"));
    assert_eq!(plan.scope(), AdminDescribeConsumerGroupsScope::ModernFirst);
    let mut machine = machine(plan);
    assert_eq!(
        submitted_kind(&mut machine),
        AdminDescribeConsumerGroupsCallKind::Consumer
    );
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept modern: {error}"));
    let fallback = machine
        .apply(AdminDescribeConsumerGroupsInput::FallbackToClassic {
            throttle_time_ms: 7,
            delivery: DeliveryStatus::PossiblySent,
        })
        .unwrap_or_else(|error| panic!("fallback: {error}"))
        .into_effect();
    assert_submit(
        fallback,
        "alpha",
        AdminDescribeConsumerGroupsCallKind::ClassicFallback,
    );
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept classic: {error}"));
    assert_eq!(
        machine.apply(AdminDescribeConsumerGroupsInput::FallbackToClassic {
            throttle_time_ms: 0,
            delivery: DeliveryStatus::NotSent,
        }),
        Err(AdminDescribeConsumerGroupsMachineError::InvalidState)
    );
}

#[test]
fn classic_only_starts_and_stays_classic_without_modern_or_fallback_attempts() {
    let plan = AdminDescribeConsumerGroupsPlan::with_scope(
        vec!["alpha".to_owned(), "beta".to_owned()],
        true,
        AdminDescribeConsumerGroupsScope::ClassicOnly,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));
    assert_eq!(plan.scope(), AdminDescribeConsumerGroupsScope::ClassicOnly);
    let mut machine = machine(plan);
    assert_eq!(
        machine.scope(),
        AdminDescribeConsumerGroupsScope::ClassicOnly
    );
    assert_submit(
        start(&mut machine),
        "alpha",
        AdminDescribeConsumerGroupsCallKind::Classic,
    );
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept classic alpha: {error}"));
    assert_eq!(
        machine.apply(AdminDescribeConsumerGroupsInput::FallbackToClassic {
            throttle_time_ms: 0,
            delivery: DeliveryStatus::NotSent,
        }),
        Err(AdminDescribeConsumerGroupsMachineError::InvalidState)
    );
    let next = machine
        .apply(AdminDescribeConsumerGroupsInput::BrokerResponded {
            throttle_time_ms: 5,
            outcome: described("alpha"),
        })
        .unwrap_or_else(|error| panic!("classic alpha response: {error}"))
        .into_effect();
    assert_submit(next, "beta", AdminDescribeConsumerGroupsCallKind::Classic);
}

#[test]
fn classic_only_compatibility_is_terminal_and_never_becomes_a_fallback() {
    let plan = AdminDescribeConsumerGroupsPlan::with_scope(
        vec!["alpha".to_owned()],
        false,
        AdminDescribeConsumerGroupsScope::ClassicOnly,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"));
    let mut machine = machine(plan);
    assert_submit(
        start(&mut machine),
        "alpha",
        AdminDescribeConsumerGroupsCallKind::Classic,
    );
    machine
        .apply(AdminDescribeConsumerGroupsInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept classic: {error}"));
    let terminal = machine
        .apply(AdminDescribeConsumerGroupsInput::ProtocolIncompatible {
            delivery: DeliveryStatus::NotSent,
        })
        .unwrap_or_else(|error| panic!("classic compatibility: {error}"))
        .into_effect();
    let Some(AdminDescribeConsumerGroupsEffect::Complete {
        terminal: AdminDescribeConsumerGroupsTerminal::Described(batch),
        ..
    }) = terminal
    else {
        panic!("compatibility terminal expected");
    };
    let (_, outcomes) = batch.into_parts();
    let (_, result) = outcomes
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("alpha outcome"))
        .into_parts();
    let AdminConsumerGroupDescriptionResult::OperationFailed(failure) = result else {
        panic!("compatibility failure expected");
    };
    assert_eq!(
        failure.kind(),
        AdminDescribeConsumerGroupsFailureKind::Compatibility
    );
    assert_eq!(failure.delivery(), DeliveryStatus::NotSent);
}

fn machine(plan: AdminDescribeConsumerGroupsPlan) -> AdminDescribeConsumerGroupsMachine {
    AdminDescribeConsumerGroupsMachine::new(
        OperationId::from_raw(42),
        Deadline::from_tick(100),
        plan,
    )
}

fn submitted_kind(
    machine: &mut AdminDescribeConsumerGroupsMachine,
) -> AdminDescribeConsumerGroupsCallKind {
    let effect = start(machine);
    let Some(AdminDescribeConsumerGroupsEffect::Submit { call_kind, .. }) = effect else {
        panic!("submission expected");
    };
    call_kind
}

fn start(
    machine: &mut AdminDescribeConsumerGroupsMachine,
) -> Option<AdminDescribeConsumerGroupsEffect> {
    machine
        .apply(AdminDescribeConsumerGroupsInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start: {error}"))
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
        panic!("submission expected");
    };
    assert_eq!(operation_id, OperationId::from_raw(42));
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
