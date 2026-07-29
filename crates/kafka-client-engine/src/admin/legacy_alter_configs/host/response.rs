//! Exhaustive generated-free API 33 response and driver-failure translation.

use kafka_client_core::{DeliveryStatus, LegacyAlterConfigsInput, LegacyAlterConfigsPlan};

use crate::{
    driver::{
        LegacyAlterConfigsDriverFailureKind, LegacyAlterConfigsTerminal as DriverTerminal,
        LegacyAlterConfigsTerminalFact,
    },
    protocol::admin::legacy_alter_configs::{
        LegacyAlterConfigsProtocolFailure, normalize_legacy_alter_configs_response_bounded,
    },
};

pub(super) fn terminal_input(
    raw: &DriverTerminal,
    plan: &LegacyAlterConfigsPlan,
    retained_limit: usize,
) -> (LegacyAlterConfigsInput, usize) {
    match raw.fact() {
        LegacyAlterConfigsTerminalFact::Response {
            selected_version,
            response,
        } => match normalize_legacy_alter_configs_response_bounded(
            plan,
            selected_version,
            response,
            retained_limit,
        ) {
            Ok(batch) => (
                LegacyAlterConfigsInput::BrokerResponded { batch },
                retained_limit,
            ),
            Err(error) => (protocol_failure(error), 0),
        },
        LegacyAlterConfigsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

const fn protocol_failure(error: LegacyAlterConfigsProtocolFailure) -> LegacyAlterConfigsInput {
    match error {
        LegacyAlterConfigsProtocolFailure::MissingSelectedVersion
        | LegacyAlterConfigsProtocolFailure::UnsupportedApiVersion => {
            LegacyAlterConfigsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        LegacyAlterConfigsProtocolFailure::RetainedBytes => {
            LegacyAlterConfigsInput::ResponseTooLarge
        }
        LegacyAlterConfigsProtocolFailure::ThrottleTime
        | LegacyAlterConfigsProtocolFailure::ResourceCount
        | LegacyAlterConfigsProtocolFailure::NonPositiveResourceType
        | LegacyAlterConfigsProtocolFailure::UnexpectedResource
        | LegacyAlterConfigsProtocolFailure::MissingResource
        | LegacyAlterConfigsProtocolFailure::DuplicateResource => {
            LegacyAlterConfigsInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: LegacyAlterConfigsDriverFailureKind,
    delivery: DeliveryStatus,
) -> LegacyAlterConfigsInput {
    match kind {
        LegacyAlterConfigsDriverFailureKind::DeadlineElapsed => {
            LegacyAlterConfigsInput::DriverDeadlineElapsed { delivery }
        }
        LegacyAlterConfigsDriverFailureKind::Compatibility => {
            LegacyAlterConfigsInput::ProtocolIncompatible { delivery }
        }
        LegacyAlterConfigsDriverFailureKind::InvalidResponse => {
            LegacyAlterConfigsInput::InvalidResponse
        }
        LegacyAlterConfigsDriverFailureKind::Transport => {
            LegacyAlterConfigsInput::TransportFailed { delivery }
        }
    }
}
