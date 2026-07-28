//! Lossless caller-ordered core-to-engine ACL creation translation tests.

use core::num::NonZeroI16;

use kafka_client_core::{
    CreateAclBinding as CoreBinding, CreateAclBrokerError as CoreBrokerError,
    CreateAclResult as CoreResult, CreateAclsEffect as CoreEffect, CreateAclsInput as CoreInput,
    CreateAclsMachine as CoreMachine, CreateAclsPlan as CorePlan,
    CreateAclsTerminal as CoreTerminal, Deadline, DeliveryStatus, Moment, OperationId,
};

use super::{
    CreateAclResult, CreateAclsBatch, CreateAclsDeliveryStatus, CreateAclsFailure,
    CreateAclsFailureKind, CreateAclsOutcome,
    outcome::{CreateAclsTranslationError, translate_terminal_into},
};

#[test]
fn prepared_storage_is_reused_while_values_move_losslessly_in_caller_order() {
    let diagnostic = "future authorization failure".to_owned();
    let diagnostic_pointer = diagnostic.as_ptr();
    let (terminal, string_pointers) = terminal_with_results_and_pointers(vec![
        CoreResult::Created,
        CoreResult::BrokerFailed(CoreBrokerError::new(
            NonZeroI16::new(-31_777).unwrap_or_else(|| panic!("nonzero")),
            Some(diagnostic),
            true,
        )),
    ]);
    let prepared = CreateAclsBatch::try_prepare_outcomes(2)
        .unwrap_or_else(|error| panic!("reserve outcomes: {error}"));
    let pointer = prepared.as_ptr();
    let capacity = prepared.capacity();
    let CreateAclsOutcome::Created(batch) = translate_terminal_into(terminal, prepared)
        .unwrap_or_else(|failure| panic!("translate terminal: {:?}", failure.error()))
    else {
        panic!("created batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 19);
    assert_eq!(batch.outcomes()[0].binding().resource_name(), "first");
    assert_eq!(batch.outcomes()[0].binding().operation(), 3);
    assert_eq!(
        binding_string_pointers(batch.outcomes()[0].binding()),
        string_pointers[0]
    );
    assert_eq!(batch.outcomes()[0].result(), &CreateAclResult::Created);
    assert_eq!(batch.outcomes()[1].binding().resource_name(), "second");
    assert_eq!(batch.outcomes()[1].binding().operation(), 15);
    assert_eq!(
        binding_string_pointers(batch.outcomes()[1].binding()),
        string_pointers[1]
    );
    let CreateAclResult::BrokerFailed(error) = batch.outcomes()[1].result() else {
        panic!("broker failure expected");
    };
    assert_eq!(error.code(), -31_777);
    assert_eq!(error.message(), Some("future authorization failure"));
    assert_eq!(
        error
            .message()
            .unwrap_or_else(|| panic!("diagnostic"))
            .as_ptr(),
        diagnostic_pointer
    );
    assert!(error.message_truncated());

    let (_, outcomes) = batch.into_parts();
    assert_eq!(outcomes.as_ptr(), pointer);
    assert_eq!(outcomes.capacity(), capacity);
    assert_eq!(outcomes.len(), 2);
}

#[test]
fn insufficient_capacity_returns_terminal_and_prepared_owner_unchanged() {
    let terminal = terminal_with_results(vec![CoreResult::Created, CoreResult::Created]);
    let prepared = CreateAclsBatch::try_prepare_outcomes(1)
        .unwrap_or_else(|error| panic!("reserve one outcome: {error}"));
    let pointer = prepared.as_ptr();
    let capacity = prepared.capacity();
    let failure = match translate_terminal_into(terminal, prepared) {
        Ok(_) => panic!("insufficient capacity must fail"),
        Err(failure) => failure,
    };
    assert_eq!(
        failure.error(),
        CreateAclsTranslationError::PreparedOutcomesCapacity {
            required: 2,
            actual: capacity,
        }
    );
    let (_error, terminal, prepared) = failure.into_parts();
    assert!(matches!(terminal, CoreTerminal::Created(_)));
    assert_eq!(prepared.as_ptr(), pointer);
    assert_eq!(prepared.capacity(), capacity);
    assert!(prepared.is_empty());
}

#[test]
fn nonempty_prepared_storage_is_rejected_without_losing_either_input() {
    let first = translate_terminal_into(
        terminal_with_results(vec![CoreResult::Created, CoreResult::Created]),
        CreateAclsBatch::try_prepare_outcomes(2)
            .unwrap_or_else(|error| panic!("reserve outcomes: {error}")),
    )
    .unwrap_or_else(|failure| panic!("first translation: {:?}", failure.error()));
    let CreateAclsOutcome::Created(first) = first else {
        panic!("created batch expected");
    };
    let (_, prepared) = first.into_parts();
    let pointer = prepared.as_ptr();
    let capacity = prepared.capacity();

    let failure = match translate_terminal_into(
        terminal_with_results(vec![CoreResult::Created, CoreResult::Created]),
        prepared,
    ) {
        Ok(_) => panic!("nonempty storage must fail"),
        Err(failure) => failure,
    };
    assert_eq!(
        failure.error(),
        CreateAclsTranslationError::PreparedOutcomesNotEmpty
    );
    let (_error, terminal, prepared) = failure.into_parts();
    assert!(matches!(terminal, CoreTerminal::Created(_)));
    assert_eq!(prepared.as_ptr(), pointer);
    assert_eq!(prepared.capacity(), capacity);
    assert_eq!(prepared.len(), 2);
}

#[test]
fn impossible_reservation_is_fallible() {
    assert!(CreateAclsBatch::try_prepare_outcomes(usize::MAX).is_err());
}

#[test]
fn every_mechanism_failure_and_delivery_certainty_is_translated() {
    for (input, submitted, expected_kind, expected_delivery) in [
        (
            CoreInput::DeadlineElapsed,
            false,
            CreateAclsFailureKind::DeadlineElapsed,
            CreateAclsDeliveryStatus::NotSent,
        ),
        (
            CoreInput::DriverRejected,
            false,
            CreateAclsFailureKind::DriverRejected,
            CreateAclsDeliveryStatus::NotSent,
        ),
        (
            CoreInput::DriverDeadlineElapsed {
                delivery: DeliveryStatus::PossiblySent,
            },
            true,
            CreateAclsFailureKind::DeadlineElapsed,
            CreateAclsDeliveryStatus::PossiblySent,
        ),
        (
            CoreInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
            true,
            CreateAclsFailureKind::Transport,
            CreateAclsDeliveryStatus::PossiblySent,
        ),
        (
            CoreInput::ResponseTooLarge,
            true,
            CreateAclsFailureKind::ResponseTooLarge,
            CreateAclsDeliveryStatus::PossiblySent,
        ),
        (
            CoreInput::ProtocolIncompatible {
                delivery: DeliveryStatus::NotSent,
            },
            true,
            CreateAclsFailureKind::Compatibility,
            CreateAclsDeliveryStatus::NotSent,
        ),
        (
            CoreInput::InvalidResponse,
            true,
            CreateAclsFailureKind::InvalidResponse,
            CreateAclsDeliveryStatus::PossiblySent,
        ),
    ] {
        let failure = translate_failure(input, submitted);
        assert_eq!(failure.kind(), expected_kind);
        assert_eq!(failure.delivery(), expected_delivery);
    }
}

fn terminal_with_results(results: Vec<CoreResult>) -> CoreTerminal {
    terminal_with_results_and_pointers(results).0
}

fn terminal_with_results_and_pointers(
    results: Vec<CoreResult>,
) -> (CoreTerminal, [(*const u8, *const u8, *const u8); 2]) {
    let mut machine = machine();
    let pointers = std::array::from_fn(|index| {
        let binding = &machine
            .plan()
            .unwrap_or_else(|| panic!("machine plan"))
            .bindings()[index];
        (
            binding.resource_name().as_ptr(),
            binding.principal().as_ptr(),
            binding.host().as_ptr(),
        )
    });
    start_and_accept(&mut machine);
    let effect = machine
        .apply(CoreInput::BrokerResponded {
            throttle_time_ms: 19,
            results,
        })
        .unwrap_or_else(|error| panic!("settle results: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("terminal expected"));
    (terminal(effect), pointers)
}

fn translate_failure(input: CoreInput, submitted: bool) -> CreateAclsFailure {
    let mut machine = machine();
    let _submission = machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    if submitted {
        machine
            .apply(CoreInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("accept driver call: {error}"));
    }
    let effect = machine
        .apply(input)
        .unwrap_or_else(|error| panic!("complete machine: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("terminal expected"));
    let prepared = CreateAclsBatch::try_prepare_outcomes(2)
        .unwrap_or_else(|error| panic!("reserve outcomes: {error}"));
    let CreateAclsOutcome::Failed(failure) = translate_terminal_into(terminal(effect), prepared)
        .unwrap_or_else(|failure| panic!("translate failure: {:?}", failure.error()))
    else {
        panic!("failure expected");
    };
    failure
}

fn start_and_accept(machine: &mut CoreMachine) {
    let _submission = machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    machine
        .apply(CoreInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver call: {error}"));
}

fn machine() -> CoreMachine {
    CoreMachine::new(
        OperationId::from_raw(43),
        Deadline::from_tick(100),
        CorePlan::new(vec![binding("first", 3), binding("second", 15)])
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn binding(resource_name: &str, operation: i8) -> CoreBinding {
    CoreBinding::new(
        2,
        resource_name.to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        operation,
        3,
    )
}

fn binding_string_pointers(binding: &super::CreateAclBinding) -> (*const u8, *const u8, *const u8) {
    (
        binding.resource_name().as_ptr(),
        binding.principal().as_ptr(),
        binding.host().as_ptr(),
    )
}

fn terminal(effect: CoreEffect) -> CoreTerminal {
    let CoreEffect::Complete { terminal, .. } = effect else {
        panic!("completion expected");
    };
    terminal
}
