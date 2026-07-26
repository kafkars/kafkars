//! Raw-terminal normalization and exact store-reservation settlement.

use kafka_client_core::{
    AssignedConsumerInput, FetchFailure, FetchFence, FetchRecords, Moment, NextFetchOffset,
};

use crate::{
    driver::{FetchTerminal, PartitionFetchRequest, classify_fetch_request_error},
    protocol::fetch::{
        FetchOutcomeFailureClass, classify_fetch_outcome_failure, normalize_fetch_outcome,
    },
};

use super::{
    super::fetch_store::FetchStageKind,
    executor::{ActiveFetchReservation, DirectFetchExecutor},
    fault::{FetchExecutionError, RetainedFetchFault},
    prepared::PreparedFetchExecution,
};

pub(super) struct FetchTerminalFact {
    pub(super) request: PartitionFetchRequest,
    pub(super) input: AssignedConsumerInput,
    pub(super) storage: TerminalStorage,
}

#[derive(Clone, Copy)]
pub(super) enum TerminalStorage {
    Released,
    NonDelivery(FetchFence),
    Deliverable(FetchFence, NextFetchOffset),
}

impl DirectFetchExecutor {
    pub(super) fn normalize_terminal(
        &mut self,
        terminal: FetchTerminal,
        active: ActiveFetchReservation,
    ) -> Result<FetchTerminalFact, FetchExecutionError> {
        let (request, observed_at, selected_version, result) = terminal.into_parts();
        let (proof, output) = active.reservation.into_protocol_parts();
        match result {
            Err(failure) => {
                let failure = classify_fetch_request_error(&failure);
                self.rollback_terminal(request, proof, output, failure)
            }
            Ok(response) => {
                let Some(selected_version) = selected_version else {
                    return self.rollback_terminal(
                        request,
                        proof,
                        output,
                        FetchFailure::Compatibility,
                    );
                };
                let Some(isolation) = request.isolation() else {
                    return self.rollback_terminal(
                        request,
                        proof,
                        output,
                        FetchFailure::Compatibility,
                    );
                };
                let normalized = normalize_fetch_outcome(
                    isolation,
                    request.topic(),
                    request.fence().position().partition().partition().get(),
                    request.next_offset().get(),
                    selected_version,
                    response,
                    request.decode_limits(),
                    output,
                );
                let outcome = match normalized {
                    Ok(outcome) => outcome,
                    Err(rejected) => {
                        let failure = core_outcome_failure(classify_fetch_outcome_failure(
                            rejected.failure(),
                        ));
                        let (_source, output) = rejected.into_parts();
                        return self.rollback_terminal(request, proof, output, failure);
                    }
                };
                let fence = request.fence();
                let kind = match self.store.stage(proof, outcome) {
                    Ok(kind) => kind,
                    Err((error, (proof, outcome))) => {
                        self.fault = Some(RetainedFetchFault::Outcome {
                            _request: request,
                            _proof: proof,
                            _outcome: outcome,
                        });
                        return Err(FetchExecutionError::Store(error));
                    }
                };
                Ok(staged_fact(request, observed_at, kind, fence))
            }
        }
    }

    fn rollback_terminal(
        &mut self,
        request: PartitionFetchRequest,
        proof: super::super::fetch_store::FetchStageProof,
        output: crate::protocol::fetch::FetchOutputReservation,
        failure: FetchFailure,
    ) -> Result<FetchTerminalFact, FetchExecutionError> {
        let bytes = output.bytes();
        if let Err((error, (proof, output))) = self.store.rollback(proof, output) {
            self.fault = Some(RetainedFetchFault::PreparedRollback {
                _prepared: PreparedFetchExecution::from_parts(request, bytes),
                _proof: proof,
                _output: output,
            });
            return Err(FetchExecutionError::Store(error));
        }
        let fence = request.fence();
        Ok(FetchTerminalFact {
            request,
            input: AssignedConsumerInput::FetchFailed { fence, failure },
            storage: TerminalStorage::Released,
        })
    }
}

const fn core_outcome_failure(failure: FetchOutcomeFailureClass) -> FetchFailure {
    match failure {
        FetchOutcomeFailureClass::DriverRejected => FetchFailure::DriverRejected,
        FetchOutcomeFailureClass::Compatibility => FetchFailure::Compatibility,
        FetchOutcomeFailureClass::InvalidResponse => FetchFailure::InvalidResponse,
        FetchOutcomeFailureClass::ResponseTooLarge => FetchFailure::ResponseTooLarge,
    }
}

fn staged_fact(
    request: PartitionFetchRequest,
    observed_at: Moment,
    kind: FetchStageKind,
    fence: FetchFence,
) -> FetchTerminalFact {
    match kind {
        FetchStageKind::BrokerFailure(failure) => FetchTerminalFact {
            request,
            input: AssignedConsumerInput::FetchFailed {
                fence,
                failure: FetchFailure::Broker(failure.code()),
            },
            storage: TerminalStorage::NonDelivery(fence),
        },
        FetchStageKind::Empty(next_offset, throttle_ticks) => FetchTerminalFact {
            request,
            input: AssignedConsumerInput::FetchAdvanced {
                fence,
                records: FetchRecords::NoApplicationRecords,
                next_offset,
                now: observed_at,
                throttle_ticks,
            },
            storage: TerminalStorage::NonDelivery(fence),
        },
        FetchStageKind::Deliverable(next_offset, throttle_ticks) => FetchTerminalFact {
            request,
            input: AssignedConsumerInput::FetchAdvanced {
                fence,
                records: FetchRecords::Deliverable,
                next_offset,
                now: observed_at,
                throttle_ticks,
            },
            storage: TerminalStorage::Deliverable(fence, next_offset),
        },
    }
}
