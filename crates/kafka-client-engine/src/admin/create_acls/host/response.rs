//! Allocation-bounded positional response translation into deterministic input.

use core::num::NonZeroI16;

use kafka_client_core::{
    CreateAclBrokerError, CreateAclResult, CreateAclsInput, CreateAclsPlan, DeliveryStatus,
    OperationId,
};

use crate::{
    driver::{
        CreateAclsCall, CreateAclsDriverFailureKind, CreateAclsRawTerminal, CreateAclsTerminalFact,
    },
    protocol::admin::create_acls::{
        CreateAclsResponseFailure, NormalizedCreateAclResultRef, normalize_create_acls_response,
    },
};

use super::{CreateAclsHandoff, CreateAclsHost, CreateAclsHostError, CreateAclsOperation};

impl CreateAclsHost {
    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: CreateAclsCall,
    ) -> Result<(), CreateAclsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(CreateAclsHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != CreateAclsHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
        {
            return Err(CreateAclsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        if !call_matches_operation(&self.operations[index]) {
            return Err(CreateAclsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, CreateAclsInput::DriverAccepted)
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "rejected handoff consumes the exact owned ACL plan evidence"
    )]
    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        plan: CreateAclsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> Result<(), CreateAclsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(CreateAclsHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != CreateAclsHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
        {
            return Err(CreateAclsHostError::InvalidHandoff);
        }
        if !operation_matches_evidence(operation, &plan, request_limit, result_limit) {
            return Err(CreateAclsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, CreateAclsInput::DriverRejected)
    }
}

fn call_matches_operation(operation: &CreateAclsOperation) -> bool {
    let (Some(plan), Some(call)) = (operation.machine.plan(), operation.call.as_ref()) else {
        return false;
    };
    call.matches_evidence(plan, operation.request_limit, operation.result_limit)
}

fn operation_matches_evidence(
    operation: &CreateAclsOperation,
    plan: &CreateAclsPlan,
    request_limit: usize,
    result_limit: usize,
) -> bool {
    operation
        .machine
        .plan()
        .is_some_and(|expected| expected == plan)
        && operation.request_limit == request_limit
        && operation.result_limit == result_limit
}

pub(super) fn terminal_input(
    raw: &CreateAclsRawTerminal,
    prepared_results: &mut Vec<CreateAclResult>,
) -> (CreateAclsInput, usize) {
    let expected_results = raw.response_plan().required_result_capacity();
    let retained_limit = raw.result_limit();
    prepared_results.clear();
    match raw.fact() {
        CreateAclsTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            let mut diagnostic_capacity = 0usize;
            let normalized = normalize_create_acls_response(
                selected_version,
                expected_results,
                response,
                retained_limit,
                |result| {
                    let (core_result, capacity) =
                        core_result(result, prepared_results.len() < prepared_results.capacity())?;
                    diagnostic_capacity = diagnostic_capacity.checked_add(capacity).ok_or(())?;
                    prepared_results.push(core_result);
                    Ok(())
                },
            );
            match normalized {
                Ok((throttle_time_ms, protocol_retained_bytes))
                    if prepared_results.len() == expected_results =>
                {
                    let retained_bytes = protocol_retained_bytes.max(diagnostic_capacity);
                    if retained_bytes > retained_limit {
                        prepared_results.clear();
                        return (CreateAclsInput::ResponseTooLarge, 0);
                    }
                    let results = core::mem::take(prepared_results);
                    (
                        CreateAclsInput::BrokerResponded {
                            throttle_time_ms,
                            results,
                        },
                        retained_bytes,
                    )
                }
                Ok((_throttle_time_ms, _retained_bytes)) => {
                    prepared_results.clear();
                    (CreateAclsInput::InvalidResponse, 0)
                }
                Err(error) => {
                    prepared_results.clear();
                    (protocol_failure(error), 0)
                }
            }
        }
        CreateAclsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            CreateAclsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        CreateAclsTerminalFact::Failed { kind, delivery } => (driver_failure(kind, delivery), 0),
    }
}

fn core_result(
    result: NormalizedCreateAclResultRef<'_>,
    has_capacity: bool,
) -> Result<(CreateAclResult, usize), ()> {
    if !has_capacity {
        return Err(());
    }
    let (error_code, error_message, error_message_truncated) = result.into_parts();
    let Some(code) = NonZeroI16::new(error_code) else {
        return Ok((CreateAclResult::Created, 0));
    };
    let error_message = error_message.map(try_owned_string).transpose()?;
    let retained = error_message.as_ref().map_or(0, String::capacity);
    Ok((
        CreateAclResult::BrokerFailed(CreateAclBrokerError::new(
            code,
            error_message,
            error_message_truncated,
        )),
        retained,
    ))
}

fn try_owned_string(source: &str) -> Result<String, ()> {
    let mut owned = String::new();
    owned.try_reserve_exact(source.len()).map_err(|_error| ())?;
    owned.push_str(source);
    Ok(owned)
}

pub(super) const fn protocol_failure(error: CreateAclsResponseFailure) -> CreateAclsInput {
    match error {
        CreateAclsResponseFailure::UnsupportedApiVersion { .. } => {
            CreateAclsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        CreateAclsResponseFailure::RetainedBytes { .. }
        | CreateAclsResponseFailure::ResultStorage => CreateAclsInput::ResponseTooLarge,
        CreateAclsResponseFailure::EmptyExpectedResults
        | CreateAclsResponseFailure::TooManyExpectedResults { .. }
        | CreateAclsResponseFailure::NegativeThrottleTime { .. }
        | CreateAclsResponseFailure::ResultCount { .. } => CreateAclsInput::InvalidResponse,
    }
}

const fn driver_failure(
    kind: CreateAclsDriverFailureKind,
    delivery: DeliveryStatus,
) -> CreateAclsInput {
    match kind {
        CreateAclsDriverFailureKind::DeadlineElapsed => {
            CreateAclsInput::DriverDeadlineElapsed { delivery }
        }
        CreateAclsDriverFailureKind::Compatibility => {
            CreateAclsInput::ProtocolIncompatible { delivery }
        }
        CreateAclsDriverFailureKind::InvalidResponse => CreateAclsInput::InvalidResponse,
        CreateAclsDriverFailureKind::Transport => CreateAclsInput::TransportFailed { delivery },
    }
}
