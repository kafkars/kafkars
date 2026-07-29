//! Single-request ownership and original-deadline machine scenarios.

use crate::{Deadline, Moment, OperationId};

use super::{
    UpdateFeature, UpdateFeatureIntent, UpdateFeaturesEffect, UpdateFeaturesInput,
    UpdateFeaturesMachine, UpdateFeaturesMachineError, UpdateFeaturesPlan, UpdateFeaturesState,
};

#[test]
fn start_emits_exact_plan_and_original_deadline_once() {
    let mut machine = fixture();
    let transition = machine
        .apply(UpdateFeaturesInput::Start {
            now: Moment::from_tick(3),
        })
        .unwrap_or_else(|error| panic!("start should succeed: {error}"));
    let Some(UpdateFeaturesEffect::Submit {
        operation_id,
        deadline,
        plan,
    }) = transition.into_effect()
    else {
        panic!("start must emit submit");
    };
    assert_eq!(operation_id, OperationId::from_raw(57));
    assert_eq!(deadline, Deadline::from_tick(20));
    assert_eq!(plan.updates()[0].feature(), "metadata.version");
    assert!(plan.validate_only());
    assert_eq!(machine.state(), UpdateFeaturesState::AwaitingDriver);
    assert_eq!(
        machine.apply(UpdateFeaturesInput::Start {
            now: Moment::from_tick(4),
        }),
        Err(UpdateFeaturesMachineError::InvalidState)
    );
}

#[test]
fn only_driver_acceptance_moves_awaiting_work_to_submitted() {
    let mut machine = fixture();
    assert_eq!(
        machine.apply(UpdateFeaturesInput::DriverAccepted),
        Err(UpdateFeaturesMachineError::InvalidState)
    );
    machine
        .apply(UpdateFeaturesInput::Start {
            now: Moment::from_tick(1),
        })
        .and_then(|_| machine.apply(UpdateFeaturesInput::DriverAccepted))
        .unwrap_or_else(|error| panic!("driver acceptance should succeed: {error}"));
    assert_eq!(machine.state(), UpdateFeaturesState::Submitted);
}

fn fixture() -> UpdateFeaturesMachine {
    UpdateFeaturesMachine::new(
        OperationId::from_raw(57),
        Deadline::from_tick(20),
        UpdateFeaturesPlan::new(
            vec![UpdateFeature::new(
                "metadata.version".to_owned(),
                19,
                UpdateFeatureIntent::Upgrade,
            )],
            true,
        )
        .unwrap_or_else(|error| panic!("valid fixture: {error}")),
    )
}
