//! Lifecycle and effect-shape scenarios for SCRAM credential alteration.

use crate::{Deadline, Moment, OperationId};

use super::{
    ALTER_USER_SCRAM_CREDENTIALS_SHA_256, AlterUserScramCredentialChange,
    AlterUserScramCredentialsEffect, AlterUserScramCredentialsInput,
    AlterUserScramCredentialsMachine, AlterUserScramCredentialsMachineError,
    AlterUserScramCredentialsPlan, AlterUserScramCredentialsState,
};

#[test]
fn start_emits_exact_non_secret_plan_and_original_deadline_once() {
    let mut machine = fixture();
    let transition = machine
        .apply(AlterUserScramCredentialsInput::Start {
            now: Moment::from_tick(3),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(AlterUserScramCredentialsEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("start must emit submit");
    };
    assert_eq!(operation_id, OperationId::from_raw(51));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.changes().len(), 1);
    assert_eq!(
        machine.state(),
        AlterUserScramCredentialsState::AwaitingDriver
    );
    assert_eq!(
        machine.apply(AlterUserScramCredentialsInput::Start {
            now: Moment::from_tick(4),
        }),
        Err(AlterUserScramCredentialsMachineError::InvalidState)
    );
}

#[test]
fn only_driver_acceptance_moves_awaiting_work_to_submitted() {
    let mut machine = fixture();
    assert_eq!(
        machine.apply(AlterUserScramCredentialsInput::DriverAccepted),
        Err(AlterUserScramCredentialsMachineError::InvalidState)
    );
    machine
        .apply(AlterUserScramCredentialsInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(AlterUserScramCredentialsInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    assert_eq!(machine.state(), AlterUserScramCredentialsState::Submitted);
}

fn fixture() -> AlterUserScramCredentialsMachine {
    AlterUserScramCredentialsMachine::new(
        OperationId::from_raw(51),
        Deadline::from_tick(20),
        AlterUserScramCredentialsPlan::new(vec![AlterUserScramCredentialChange::deletion(
            "alice".to_owned(),
            ALTER_USER_SCRAM_CREDENTIALS_SHA_256,
        )])
        .unwrap_or_else(|error| panic!("valid fixture: {error}")),
    )
}
