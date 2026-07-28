//! Allocation-free positional core-to-engine ACL deletion translation tests.

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, DeleteAclBrokerError as CoreBrokerError, DeleteAclFilterResult as CoreFilterResult,
    DeleteAclMatchResult as CoreMatchResult, DeleteAclMatchingBinding as CoreMatchingBinding,
    DeleteAclsEffect as CoreEffect, DeleteAclsFilter as CoreFilter, DeleteAclsInput as CoreInput,
    DeleteAclsMachine as CoreMachine, DeleteAclsPlan as CorePlan,
    DeleteAclsTerminal as CoreTerminal, Moment, OperationId,
};

use super::{
    DeleteAclFilterResult, DeleteAclMatchResult, DeleteAclsBatch, DeleteAclsOutcome,
    outcome::{
        DeleteAclsPrepareMatchingError, DeleteAclsTranslationError, translate_terminal_into,
    },
};

#[test]
fn prepared_outer_and_nested_storage_is_reused_for_exact_positional_values() {
    let matching_diagnostic = "future signed match failure".to_owned();
    let matching_diagnostic_pointer = matching_diagnostic.as_ptr();
    let filter_diagnostic = "future signed filter failure".to_owned();
    let filter_diagnostic_pointer = filter_diagnostic.as_ptr();
    let matching = matching(
        "orders",
        CoreMatchResult::BrokerFailed(broker_error(-31_777, Some(matching_diagnostic), true)),
    );
    let matching_string_pointers = (
        matching.resource_name().as_ptr(),
        matching.principal().as_ptr(),
        matching.host().as_ptr(),
    );
    let (terminal, filter_string_pointers) = terminal_with_results(vec![
        CoreFilterResult::Matched(vec![matching]),
        CoreFilterResult::BrokerFailed(broker_error(-731, Some(filter_diagnostic), false)),
    ]);
    let mut prepared = DeleteAclsBatch::try_prepare_outcomes(2)
        .unwrap_or_else(|error| panic!("reserve positional outcomes: {error}"));
    prepared
        .try_prepare_matching([1, 0].into_iter())
        .unwrap_or_else(|error| panic!("reserve matching outcomes: {error:?}"));
    assert!(
        prepared
            .retained_heap_bytes()
            .is_some_and(|retained| retained > 0)
    );
    let outer_pointer = prepared.outcomes().as_ptr();
    let outer_capacity = prepared.outcomes_capacity();
    let nested_pointer = prepared.matching()[0].as_ptr();
    let nested_capacity = prepared.matching()[0].capacity();

    let DeleteAclsOutcome::Deleted(batch) = translate_terminal_into(terminal, prepared)
        .unwrap_or_else(|failure| panic!("translate terminal: {:?}", failure.error()))
    else {
        panic!("deleted batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 19);
    assert_eq!(batch.outcomes().len(), 2);
    assert_eq!(batch.outcomes()[0].filter(), batch.outcomes()[1].filter());
    assert_eq!(
        batch.outcomes()[0]
            .filter()
            .resource_name()
            .unwrap_or_else(|| panic!("resource name"))
            .as_ptr(),
        filter_string_pointers[0]
    );
    let DeleteAclFilterResult::Matched(bindings) = batch.outcomes()[0].result() else {
        panic!("matching bindings expected");
    };
    assert_eq!(bindings.as_ptr(), nested_pointer);
    assert_eq!(bindings.capacity(), nested_capacity);
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].resource_type(), 2);
    assert_eq!(bindings[0].pattern_type(), 3);
    assert_eq!(bindings[0].operation(), 15);
    assert_eq!(bindings[0].permission_type(), 3);
    assert_eq!(
        (
            bindings[0].resource_name().as_ptr(),
            bindings[0].principal().as_ptr(),
            bindings[0].host().as_ptr(),
        ),
        matching_string_pointers
    );
    let DeleteAclMatchResult::BrokerFailed(error) = bindings[0].result() else {
        panic!("matching failure expected");
    };
    assert_eq!(error.code(), -31_777);
    assert_eq!(error.message(), Some("future signed match failure"));
    assert_eq!(
        error
            .message()
            .unwrap_or_else(|| panic!("message"))
            .as_ptr(),
        matching_diagnostic_pointer
    );
    assert!(error.message_truncated());

    let DeleteAclFilterResult::BrokerFailed(error) = batch.outcomes()[1].result() else {
        panic!("filter failure expected");
    };
    assert_eq!(error.code(), -731);
    assert_eq!(error.message(), Some("future signed filter failure"));
    assert_eq!(
        error
            .message()
            .unwrap_or_else(|| panic!("message"))
            .as_ptr(),
        filter_diagnostic_pointer
    );
    assert!(!error.message_truncated());
    let (_, outcomes) = batch.into_parts();
    assert_eq!(outcomes.as_ptr(), outer_pointer);
    assert_eq!(outcomes.capacity(), outer_capacity);
}

#[test]
fn insufficient_nested_capacity_retains_terminal_and_every_prepared_owner() {
    let (terminal, _) = terminal_with_results(vec![
        CoreFilterResult::Matched(vec![matching("orders", CoreMatchResult::Deleted)]),
        CoreFilterResult::Matched(Vec::new()),
    ]);
    let prepared = DeleteAclsBatch::try_prepare_outcomes(2)
        .unwrap_or_else(|error| panic!("reserve outcomes: {error}"));
    let outer_pointer = prepared.outcomes().as_ptr();
    let nested_pointer = prepared.matching()[0].as_ptr();
    let failure = match translate_terminal_into(terminal, prepared) {
        Ok(_) => panic!("insufficient nested capacity must fail"),
        Err(failure) => failure,
    };
    assert_eq!(
        failure.error(),
        DeleteAclsTranslationError::PreparedMatchingCapacity {
            filter_index: 0,
            required: 1,
            actual: 0,
        }
    );
    let (_, terminal, prepared) = failure.into_parts();
    assert!(matches!(terminal, CoreTerminal::Deleted(_)));
    assert_eq!(prepared.outcomes().as_ptr(), outer_pointer);
    assert!(prepared.outcomes().is_empty());
    assert_eq!(prepared.matching()[0].as_ptr(), nested_pointer);
    assert!(prepared.matching()[0].is_empty());
}

#[test]
fn prepared_position_count_and_impossible_reservation_fail_without_collecting() {
    let (terminal, _) = terminal_with_results(vec![
        CoreFilterResult::Matched(Vec::new()),
        CoreFilterResult::Matched(Vec::new()),
    ]);
    let prepared = DeleteAclsBatch::try_prepare_outcomes(1)
        .unwrap_or_else(|error| panic!("reserve one position: {error}"));
    let failure = match translate_terminal_into(terminal, prepared) {
        Ok(_) => panic!("missing prepared position must fail"),
        Err(failure) => failure,
    };
    assert_eq!(
        failure.error(),
        DeleteAclsTranslationError::PreparedOutcomesCapacity {
            required: 2,
            actual: 1,
        }
    );
    assert!(DeleteAclsBatch::try_prepare_outcomes(usize::MAX).is_err());

    let mut prepared = DeleteAclsBatch::try_prepare_outcomes(1)
        .unwrap_or_else(|error| panic!("reserve one position: {error}"));
    assert!(matches!(
        prepared.try_prepare_matching([].into_iter()),
        Err(DeleteAclsPrepareMatchingError::FilterCount {
            expected: 1,
            actual: 0,
        })
    ));
    assert!(matches!(
        prepared.try_prepare_matching([usize::MAX].into_iter()),
        Err(DeleteAclsPrepareMatchingError::Reserve {
            filter_index: 0,
            ..
        })
    ));
}

fn terminal_with_results(results: Vec<CoreFilterResult>) -> (CoreTerminal, [*const u8; 2]) {
    let mut machine = machine();
    let pointers = std::array::from_fn(|index| {
        machine.plan().unwrap_or_else(|| panic!("plan")).filters()[index]
            .resource_name()
            .unwrap_or_else(|| panic!("resource name"))
            .as_ptr()
    });
    start_and_accept(&mut machine);
    let effect = machine
        .apply(CoreInput::BrokerResponded {
            throttle_time_ms: 19,
            results,
        })
        .unwrap_or_else(|error| panic!("settle response: {error}"))
        .into_effect()
        .unwrap_or_else(|| panic!("terminal expected"));
    (terminal(effect), pointers)
}

fn start_and_accept(machine: &mut CoreMachine) {
    let _ = machine
        .apply(CoreInput::Start {
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("start machine: {error}"));
    machine
        .apply(CoreInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("accept driver call: {error}"));
}

fn machine() -> CoreMachine {
    let duplicate = CoreFilter::new(
        2,
        Some("orders".to_owned()),
        3,
        Some("User:alice".to_owned()),
        None,
        15,
        3,
    );
    CoreMachine::new(
        OperationId::from_raw(43),
        Deadline::from_tick(100),
        CorePlan::new(vec![duplicate.clone(), duplicate])
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
    )
}

fn matching(name: &str, result: CoreMatchResult) -> CoreMatchingBinding {
    CoreMatchingBinding::new(
        2,
        name.to_owned(),
        3,
        "User:alice".to_owned(),
        "*".to_owned(),
        15,
        3,
        result,
    )
}

fn broker_error(code: i16, message: Option<String>, truncated: bool) -> CoreBrokerError {
    CoreBrokerError::new(
        NonZeroI16::new(code).unwrap_or_else(|| panic!("nonzero")),
        message,
        truncated,
    )
}

fn terminal(effect: CoreEffect) -> CoreTerminal {
    let CoreEffect::Complete { terminal, .. } = effect else {
        panic!("completion expected");
    };
    terminal
}
