//! Linear ownership of one accepted `AnyBroker` `AlterClientQuotas` call.

mod evidence;

use std::time::Instant;

use kafka_client_core::{
    AlterClientQuotaEntry, AlterClientQuotaOperationKind, AlterClientQuotasPlan,
};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::{AlterClientQuotasRequest, AlterClientQuotasResponse};

use crate::protocol::admin::alter_client_quotas::{
    AlterClientQuotaAlterationRef, AlterClientQuotaEntityComponentRef,
    AlterClientQuotaOperationKindRef, AlterClientQuotaOperationRef,
    AlterClientQuotasRequestFailure, AlterClientQuotasRequestRef, alter_client_quotas_request,
};

use super::{
    super::DriverOwner,
    alter_client_quotas_submission::AlterClientQuotasSubmitError,
    alter_client_quotas_terminal::{
        AlterClientQuotasRawTerminal, RecoveredAlterClientQuotasCall,
        retain_alter_client_quotas_terminal,
    },
};

pub(super) use evidence::AlterClientQuotasEvidence;

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted AlterClientQuotas call must be terminally settled"]
pub(crate) struct AlterClientQuotasCall {
    call: Option<RoutedCall<AlterClientQuotasResponse>>,
    evidence: Option<AlterClientQuotasEvidence>,
}

impl AlterClientQuotasCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: AlterClientQuotasPlan,
        retained_limit: usize,
        deadline: Instant,
    ) -> Result<Self, AlterClientQuotasCallAdmissionFailure> {
        let evidence = AlterClientQuotasEvidence::new(plan, retained_limit);
        let request = match prepare_request(&evidence) {
            Ok(request) => request,
            Err(source) => {
                return Err(AlterClientQuotasCallAdmissionFailure::new(source, evidence));
            }
        };
        let call = match driver.submit_alter_client_quotas(request, deadline) {
            Ok(call) => call,
            Err(source) => {
                return Err(AlterClientQuotasCallAdmissionFailure::new(
                    AlterClientQuotasAdmissionSource::Driver(source),
                    evidence,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    /// Extracts a ready raw terminal without blocking.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AlterClientQuotasRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_alter_client_quotas_terminal(
                    selected_version,
                    result,
                    route_token,
                    evidence,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches(
        &self,
        expected_plan: &AlterClientQuotasPlan,
        expected_retained_limit: usize,
    ) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(expected_plan, expected_retained_limit))
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Result<RecoveredAlterClientQuotasCall, Self> {
        if self.call.is_none() || self.evidence.is_none() {
            return Err(self);
        }
        let Self { call, evidence } = self;
        drop(call);
        Ok(RecoveredAlterClientQuotasCall::new(
            evidence.unwrap_or_else(|| unreachable!("validated exact evidence")),
        ))
    }
}

fn prepare_request(
    evidence: &AlterClientQuotasEvidence,
) -> Result<AlterClientQuotasRequest, AlterClientQuotasAdmissionSource> {
    let entries = evidence.plan().entries();
    let entity_refs =
        prepare_entity_refs(entries).ok_or(AlterClientQuotasAdmissionSource::Preparation)?;
    let operation_refs =
        prepare_operation_refs(entries).ok_or(AlterClientQuotasAdmissionSource::Preparation)?;
    let alterations = prepare_alteration_refs(&entity_refs, &operation_refs)
        .ok_or(AlterClientQuotasAdmissionSource::Preparation)?;
    let source = AlterClientQuotasRequestRef::new(&alterations, evidence.plan().validate_only());
    alter_client_quotas_request(source, evidence.retained_limit())
        .map_err(AlterClientQuotasAdmissionSource::Request)
}

fn prepare_entity_refs(
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

fn prepare_operation_refs(
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

fn prepare_alteration_refs<'a>(
    entities: &'a [Vec<AlterClientQuotaEntityComponentRef<'a>>],
    operations: &'a [Vec<AlterClientQuotaOperationRef<'a>>],
) -> Option<Vec<AlterClientQuotaAlterationRef<'a>>> {
    if entities.len() != operations.len() {
        return None;
    }
    let mut alterations = Vec::new();
    alterations.try_reserve_exact(entities.len()).ok()?;
    alterations.extend(
        entities
            .iter()
            .zip(operations)
            .map(|(entity, operations)| AlterClientQuotaAlterationRef::new(entity, operations)),
    );
    Some(alterations)
}

#[derive(Debug)]
enum AlterClientQuotasAdmissionSource {
    Preparation,
    Request(AlterClientQuotasRequestFailure),
    Driver(AlterClientQuotasSubmitError),
}

/// Definitely-unsent bounded-driver rejection retaining exact attempted evidence.
#[derive(Debug)]
#[must_use = "a rejected AlterClientQuotas call must become operation input"]
pub(crate) struct AlterClientQuotasCallAdmissionFailure {
    source: AlterClientQuotasAdmissionSource,
    evidence: AlterClientQuotasEvidence,
}

impl AlterClientQuotasCallAdmissionFailure {
    const fn new(
        source: AlterClientQuotasAdmissionSource,
        evidence: AlterClientQuotasEvidence,
    ) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_correlation(self) -> (AlterClientQuotasPlan, usize) {
        let Self { source, evidence } = self;
        match source {
            AlterClientQuotasAdmissionSource::Preparation => {}
            AlterClientQuotasAdmissionSource::Request(source) => {
                let _ = source;
            }
            AlterClientQuotasAdmissionSource::Driver(source) => drop(source),
        }
        evidence.into_parts()
    }
}
