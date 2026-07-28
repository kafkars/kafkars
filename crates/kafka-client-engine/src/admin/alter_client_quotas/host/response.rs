//! Exhaustive normalized-protocol translation into deterministic core input.

use core::num::NonZeroI16;

use kafka_client_core::{
    AlterClientQuotaBrokerError, AlterClientQuotaEntity, AlterClientQuotaEntityComponent,
    AlterClientQuotaEntry, AlterClientQuotaOperationKind, AlterClientQuotaOutcome,
    AlterClientQuotasBatch, AlterClientQuotasInput, AlterClientQuotasPlan, DeliveryStatus,
};

use crate::{
    driver::{
        AlterClientQuotasDriverFailureKind, AlterClientQuotasRawTerminal,
        AlterClientQuotasTerminalFact,
    },
    protocol::admin::alter_client_quotas::{
        AlterClientQuotaAlterationRef, AlterClientQuotaEntityComponentRef,
        AlterClientQuotaOperationKindRef, AlterClientQuotaOperationRef,
        AlterClientQuotasRequestRef, AlterClientQuotasResponseFailure,
        NormalizedAlterClientQuotaOutcome, normalize_alter_client_quotas_response,
    },
};

pub(super) fn terminal_input(
    raw: &AlterClientQuotasRawTerminal,
    retained_limit: usize,
) -> (AlterClientQuotasInput, usize) {
    match raw.fact() {
        AlterClientQuotasTerminalFact::Response {
            selected_version: Some(selected_version),
            response,
        } => {
            let Some(refs) = RequestRefs::new(raw.plan()) else {
                return (AlterClientQuotasInput::ResponseTooLarge, 0);
            };
            let Some(alterations) = refs.alterations() else {
                return (AlterClientQuotasInput::ResponseTooLarge, 0);
            };
            let request =
                AlterClientQuotasRequestRef::new(&alterations, raw.plan().validate_only());
            match normalize_alter_client_quotas_response(
                selected_version,
                request,
                response,
                retained_limit,
            ) {
                Ok(normalized) => {
                    let (throttle_time_ms, outcomes, retained_bytes) = normalized.into_parts();
                    (
                        AlterClientQuotasInput::BrokerResponded {
                            batch: AlterClientQuotasBatch::new(
                                throttle_time_ms,
                                outcomes.into_iter().map(core_outcome).collect(),
                            ),
                        },
                        retained_bytes,
                    )
                }
                Err(error) => (protocol_failure(error), 0),
            }
        }
        AlterClientQuotasTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            AlterClientQuotasInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        AlterClientQuotasTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

fn core_outcome(outcome: NormalizedAlterClientQuotaOutcome) -> AlterClientQuotaOutcome {
    let (components, error_code, error_message, error_message_truncated) = outcome.into_parts();
    let entity = AlterClientQuotaEntity::new(
        components
            .into_iter()
            .map(|component| {
                let (entity_type, entity_name) = component.into_parts();
                AlterClientQuotaEntityComponent::new(entity_type, entity_name)
            })
            .collect(),
    );
    match NonZeroI16::new(error_code) {
        Some(code) => AlterClientQuotaOutcome::failed(
            entity,
            AlterClientQuotaBrokerError::new(code, error_message, error_message_truncated),
        ),
        None => AlterClientQuotaOutcome::altered(entity),
    }
}

pub(super) const fn protocol_failure(
    error: AlterClientQuotasResponseFailure,
) -> AlterClientQuotasInput {
    match error {
        AlterClientQuotasResponseFailure::UnsupportedApiVersion { .. } => {
            AlterClientQuotasInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            }
        }
        AlterClientQuotasResponseFailure::RetainedBytes { .. } => {
            AlterClientQuotasInput::ResponseTooLarge
        }
        AlterClientQuotasResponseFailure::NegativeThrottleTime { .. }
        | AlterClientQuotasResponseFailure::EntryCount { .. }
        | AlterClientQuotasResponseFailure::TooManyEntries { .. }
        | AlterClientQuotasResponseFailure::EmptyEntity
        | AlterClientQuotasResponseFailure::TooManyEntityComponents { .. }
        | AlterClientQuotasResponseFailure::EmptyEntityType
        | AlterClientQuotasResponseFailure::EntityTypeTooLong { .. }
        | AlterClientQuotasResponseFailure::EmptyEntityName
        | AlterClientQuotasResponseFailure::EntityNameTooLong { .. }
        | AlterClientQuotasResponseFailure::DuplicateEntityType
        | AlterClientQuotasResponseFailure::DuplicateResponseEntity
        | AlterClientQuotasResponseFailure::UnexpectedEntity
        | AlterClientQuotasResponseFailure::MissingEntity
        | AlterClientQuotasResponseFailure::InvalidRequest => {
            AlterClientQuotasInput::InvalidResponse
        }
    }
}

const fn driver_failure(
    kind: AlterClientQuotasDriverFailureKind,
    delivery: DeliveryStatus,
) -> AlterClientQuotasInput {
    match kind {
        AlterClientQuotasDriverFailureKind::DeadlineElapsed => {
            AlterClientQuotasInput::DriverDeadlineElapsed { delivery }
        }
        AlterClientQuotasDriverFailureKind::Compatibility => {
            AlterClientQuotasInput::ProtocolIncompatible { delivery }
        }
        AlterClientQuotasDriverFailureKind::InvalidResponse => {
            AlterClientQuotasInput::InvalidResponse
        }
        AlterClientQuotasDriverFailureKind::Transport => {
            AlterClientQuotasInput::TransportFailed { delivery }
        }
    }
}

struct RequestRefs<'a> {
    entities: Vec<Vec<AlterClientQuotaEntityComponentRef<'a>>>,
    operations: Vec<Vec<AlterClientQuotaOperationRef<'a>>>,
}

impl<'a> RequestRefs<'a> {
    fn new(plan: &'a AlterClientQuotasPlan) -> Option<Self> {
        let entities = entity_refs(plan.entries())?;
        let operations = operation_refs(plan.entries())?;
        Some(Self {
            entities,
            operations,
        })
    }

    fn alterations(&'a self) -> Option<Vec<AlterClientQuotaAlterationRef<'a>>> {
        if self.entities.len() != self.operations.len() {
            return None;
        }
        let mut alterations = Vec::new();
        alterations.try_reserve_exact(self.entities.len()).ok()?;
        alterations.extend(
            self.entities
                .iter()
                .zip(&self.operations)
                .map(|(entity, operations)| AlterClientQuotaAlterationRef::new(entity, operations)),
        );
        Some(alterations)
    }
}

fn entity_refs(
    entries: &[AlterClientQuotaEntry],
) -> Option<Vec<Vec<AlterClientQuotaEntityComponentRef<'_>>>> {
    let mut all = Vec::new();
    all.try_reserve_exact(entries.len()).ok()?;
    for entry in entries {
        let mut components = Vec::new();
        components
            .try_reserve_exact(entry.entity().components().len())
            .ok()?;
        components.extend(entry.entity().components().iter().map(|component| {
            AlterClientQuotaEntityComponentRef::new(
                component.entity_type(),
                component.entity_name(),
            )
        }));
        all.push(components);
    }
    Some(all)
}

fn operation_refs(
    entries: &[AlterClientQuotaEntry],
) -> Option<Vec<Vec<AlterClientQuotaOperationRef<'_>>>> {
    let mut all = Vec::new();
    all.try_reserve_exact(entries.len()).ok()?;
    for entry in entries {
        let mut operations = Vec::new();
        operations
            .try_reserve_exact(entry.operations().len())
            .ok()?;
        operations.extend(entry.operations().iter().map(|operation| {
            let kind = match operation.kind() {
                AlterClientQuotaOperationKind::Set(value) => {
                    AlterClientQuotaOperationKindRef::Set(value)
                }
                AlterClientQuotaOperationKind::Remove => AlterClientQuotaOperationKindRef::Remove,
            };
            AlterClientQuotaOperationRef::new(operation.key(), kind)
        }));
        all.push(operations);
    }
    Some(all)
}
