//! Translation from raw driver and generated response facts into core input.

use kafka_client_core::{DeliveryStatus, ElectLeadersInput, ElectLeadersPlan};

use crate::{
    driver::{ElectLeadersDriverFailureKind, ElectLeadersTerminal, ElectLeadersTerminalFact},
    protocol::admin::elect_leaders::{
        ElectLeadersProtocolFailure, LeaderElectionRef, ValidatedElectLeadersResponse,
        validate_elect_leaders_response,
    },
};

pub(super) fn terminal_input(
    terminal: &ElectLeadersTerminal,
    plan: &ElectLeadersPlan,
    result_limit: usize,
) -> ElectLeadersInput {
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
            let targets = match change_refs(plan) {
                Ok(targets) => targets,
                Err(input) => return input,
            };
            match validate_elect_leaders_response(
                plan.election_type(),
                &targets,
                response,
                selected_version,
                result_limit,
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

fn change_refs(plan: &ElectLeadersPlan) -> Result<Vec<LeaderElectionRef<'_>>, ElectLeadersInput> {
    let mut targets = Vec::new();
    targets
        .try_reserve_exact(plan.targets().len())
        .map_err(|_error| ElectLeadersInput::ResponseTooLarge)?;
    targets.extend(
        plan.targets()
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
