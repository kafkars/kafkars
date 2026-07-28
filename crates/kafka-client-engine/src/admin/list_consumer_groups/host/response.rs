//! Exhaustive raw-driver and protocol-terminal translation.

use kafka_client_core::{AdminListConsumerGroupsInput, DeliveryStatus};

use crate::{
    driver::{
        ListConsumerGroupsDriverFailureKind, ListConsumerGroupsRawTerminal,
        ListConsumerGroupsRawTerminalFact,
    },
    protocol::admin::list_consumer_groups::{
        ListConsumerGroupsProtocolFailure, NormalizedListConsumerGroupsDiscovery,
        normalize_list_consumer_groups_discovery, normalize_list_consumer_groups_response,
    },
};

pub(super) fn terminal_input(
    raw: &ListConsumerGroupsRawTerminal,
    retained_bytes: usize,
) -> (AdminListConsumerGroupsInput, usize) {
    match raw.fact() {
        ListConsumerGroupsRawTerminalFact::DiscoveryResponse {
            selected_version,
            response,
        } => match normalize_list_consumer_groups_discovery(
            selected_version,
            response,
            retained_bytes,
        ) {
            Ok(NormalizedListConsumerGroupsDiscovery::Brokers {
                broker_ids,
                retained_bytes,
            }) => (
                AdminListConsumerGroupsInput::BrokersDiscovered { broker_ids },
                retained_bytes,
            ),
            Ok(NormalizedListConsumerGroupsDiscovery::Rejected {
                error,
                retained_bytes,
            }) => (
                AdminListConsumerGroupsInput::DiscoveryRejected { error },
                retained_bytes,
            ),
            Err(error) => (protocol_failure(error), 0),
        },
        ListConsumerGroupsRawTerminalFact::BrokerResponse {
            broker_id,
            selected_version,
            response,
        } => match normalize_list_consumer_groups_response(
            broker_id,
            selected_version,
            response,
            retained_bytes,
        ) {
            Ok(normalized) => {
                let (throttle_time_ms, outcome, retained_bytes) = normalized.into_parts();
                (
                    AdminListConsumerGroupsInput::BrokerResponded {
                        throttle_time_ms,
                        outcome,
                    },
                    retained_bytes,
                )
            }
            Err(error) => (protocol_failure(error), 0),
        },
        ListConsumerGroupsRawTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

const fn protocol_failure(
    failure: ListConsumerGroupsProtocolFailure,
) -> AdminListConsumerGroupsInput {
    match failure {
        ListConsumerGroupsProtocolFailure::Compatibility => {
            AdminListConsumerGroupsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        ListConsumerGroupsProtocolFailure::ResponseTooLarge => {
            AdminListConsumerGroupsInput::ResponseTooLarge
        }
        ListConsumerGroupsProtocolFailure::InvalidResponse => {
            AdminListConsumerGroupsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: ListConsumerGroupsDriverFailureKind,
    delivery: DeliveryStatus,
) -> AdminListConsumerGroupsInput {
    match kind {
        ListConsumerGroupsDriverFailureKind::DeadlineElapsed => {
            AdminListConsumerGroupsInput::DriverDeadlineElapsed { delivery }
        }
        ListConsumerGroupsDriverFailureKind::Compatibility => {
            AdminListConsumerGroupsInput::ProtocolIncompatible { delivery }
        }
        ListConsumerGroupsDriverFailureKind::InvalidResponse => {
            AdminListConsumerGroupsInput::InvalidResponse
        }
        ListConsumerGroupsDriverFailureKind::Transport => {
            AdminListConsumerGroupsInput::TransportFailed { delivery }
        }
    }
}
