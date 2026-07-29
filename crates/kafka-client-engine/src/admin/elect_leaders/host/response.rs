//! Translation from raw driver and generated response facts into core input.

use kafka_client_core::{DeliveryStatus, ElectLeadersInput, LeaderElectionTarget};

use crate::{
    driver::{ElectLeadersDriverFailureKind, ElectLeadersTerminal, ElectLeadersTerminalFact},
    protocol::admin::elect_leaders::{
        ElectLeadersProtocolFailure, ElectLeadersSelectionRef, LeaderElectionRef,
        ValidatedElectLeadersResponse, validate_elect_leaders_response,
    },
};

use super::ElectLeadersOperation;

pub(super) fn operation_matches_correlation(
    operation: &ElectLeadersOperation,
    plan: &kafka_client_core::ElectLeadersPlan,
    request_scratch_limit: usize,
    result_limit: usize,
) -> bool {
    &operation.response_plan == plan
        && operation.request_scratch_limit == request_scratch_limit
        && operation.result_limit == result_limit
}

pub(super) fn call_matches_operation(operation: &ElectLeadersOperation) -> bool {
    operation.call.as_ref().is_some_and(|call| {
        call.matches_correlation(
            &operation.response_plan,
            operation.request_scratch_limit,
            operation.result_limit,
        )
    })
}

pub(super) fn terminal_input(terminal: &ElectLeadersTerminal) -> ElectLeadersInput {
    match terminal.fact() {
        ElectLeadersTerminalFact::Failed { kind, delivery } => match kind {
            ElectLeadersDriverFailureKind::DeadlineElapsed => {
                ElectLeadersInput::DriverDeadlineElapsed { delivery }
            }
            ElectLeadersDriverFailureKind::Compatibility => {
                ElectLeadersInput::ProtocolIncompatible { delivery }
            }
            ElectLeadersDriverFailureKind::InvalidResponse => ElectLeadersInput::InvalidResponse,
            ElectLeadersDriverFailureKind::Transport => {
                ElectLeadersInput::TransportFailed { delivery }
            }
        },
        ElectLeadersTerminalFact::Response {
            selected_version: None,
            ..
        } => ElectLeadersInput::InvalidResponse,
        ElectLeadersTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            let targets;
            let selection = match terminal.plan().selection().selected_targets() {
                None => ElectLeadersSelectionRef::AllPartitions,
                Some(selected) => {
                    targets = match change_refs(selected) {
                        Ok(targets) => targets,
                        Err(input) => return input,
                    };
                    ElectLeadersSelectionRef::Selected(&targets)
                }
            };
            match validate_elect_leaders_response(
                terminal.plan().election_type(),
                selection,
                response,
                selected_version,
                terminal.result_limit(),
            ) {
                Ok(ValidatedElectLeadersResponse::BrokerRejected(error)) => {
                    ElectLeadersInput::BrokerRejected { error }
                }
                Ok(ValidatedElectLeadersResponse::Batch(batch)) => {
                    ElectLeadersInput::BrokerResponded { batch }
                }
                Err(error) => protocol_failure(error),
            }
        }
    }
}

fn change_refs(
    selected: &[LeaderElectionTarget],
) -> Result<Vec<LeaderElectionRef<'_>>, ElectLeadersInput> {
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(selected.len())
        .map_err(|_error| ElectLeadersInput::ResponseTooLarge)?;
    targets.extend(
        selected
            .iter()
            .map(|target| LeaderElectionRef::new(target.topic(), target.partition())),
    );
    Ok(targets)
}

const fn protocol_failure(error: ElectLeadersProtocolFailure) -> ElectLeadersInput {
    match error {
        ElectLeadersProtocolFailure::UnsupportedApiVersion { .. } => {
            ElectLeadersInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        ElectLeadersProtocolFailure::RetainedBytes => ElectLeadersInput::ResponseTooLarge,
        _ => ElectLeadersInput::InvalidResponse,
    }
}
