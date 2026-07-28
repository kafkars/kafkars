//! Lifecycle scenarios for deterministic Admin `AlterClientQuotas` ownership.

use crate::{Deadline, Moment, OperationId};

use super::{
    AlterClientQuotaEntity, AlterClientQuotaEntityComponent, AlterClientQuotaEntry,
    AlterClientQuotaOperation, AlterClientQuotasEffect, AlterClientQuotasInput,
    AlterClientQuotasMachine, AlterClientQuotasMachineError, AlterClientQuotasPlan,
    AlterClientQuotasState,
};

#[test]
fn original_absolute_deadline_and_exact_plan_cross_the_only_submit_effect() {
    let mut machine = machine(20);
    let transition = machine
        .apply(AlterClientQuotasInput::Start {
            now: Moment::from_tick(2),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(AlterClientQuotasEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("start must submit");
    };

    assert_eq!(operation_id, OperationId::from_raw(47));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan, plan_fixture());
    assert_eq!(machine.state(), AlterClientQuotasState::AwaitingDriver);
    assert_eq!(
        machine.apply(AlterClientQuotasInput::Start {
            now: Moment::from_tick(3),
        }),
        Err(AlterClientQuotasMachineError::InvalidState)
    );
}

#[test]
fn driver_ownership_is_an_explicit_single_transition() {
    let mut machine = machine(20);
    assert_eq!(
        machine.apply(AlterClientQuotasInput::DriverAccepted),
        Err(AlterClientQuotasMachineError::InvalidState)
    );
    machine
        .apply(AlterClientQuotasInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(AlterClientQuotasInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    assert_eq!(machine.state(), AlterClientQuotasState::Submitted);
    assert_eq!(
        machine.apply(AlterClientQuotasInput::DriverAccepted),
        Err(AlterClientQuotasMachineError::InvalidState)
    );
}

fn machine(deadline: u64) -> AlterClientQuotasMachine {
    AlterClientQuotasMachine::new(
        OperationId::from_raw(47),
        Deadline::from_tick(deadline),
        plan_fixture(),
    )
}

fn plan_fixture() -> AlterClientQuotasPlan {
    AlterClientQuotasPlan::new(
        vec![AlterClientQuotaEntry::new(
            AlterClientQuotaEntity::new(vec![AlterClientQuotaEntityComponent::new(
                "user".to_owned(),
                Some("alice".to_owned()),
            )]),
            vec![AlterClientQuotaOperation::set(
                "producer_byte_rate".to_owned(),
                2048.0,
            )],
        )],
        true,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}
