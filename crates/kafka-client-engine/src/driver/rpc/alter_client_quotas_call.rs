//! Linear ownership of one accepted AnyBroker `AlterClientQuotas` call.

use std::time::Instant;

use kafka_client_core::{
    AlterClientQuotaEntry, AlterClientQuotaOperationKind, AlterClientQuotasPlan,
};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::AlterClientQuotasResponse;

use crate::protocol::admin::alter_client_quotas::{
    AlterClientQuotaAlterationRef, AlterClientQuotaEntityComponentRef,
    AlterClientQuotaOperationKindRef, AlterClientQuotaOperationRef, AlterClientQuotasRequestRef,
    alter_client_quotas_request,
};

use super::{
    super::DriverOwner,
    alter_client_quotas_terminal::{
        AlterClientQuotasRawTerminal, RecoveredAlterClientQuotasCall,
        retain_alter_client_quotas_terminal,
    },
};

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted AlterClientQuotas call must be terminally settled"]
pub(crate) struct AlterClientQuotasCall {
    call: Option<RoutedCall<AlterClientQuotasResponse>>,
    plan: Option<AlterClientQuotasPlan>,
}

impl AlterClientQuotasCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: AlterClientQuotasPlan,
        retained_limit: usize,
        deadline: Instant,
    ) -> Result<Self, AlterClientQuotasCallAdmissionFailure> {
        let entries = plan.entries();
        let entity_refs =
            prepare_entity_refs(entries).ok_or(AlterClientQuotasCallAdmissionFailure::Request)?;
        let operation_refs = prepare_operation_refs(entries)
            .ok_or(AlterClientQuotasCallAdmissionFailure::Request)?;
        let alterations = prepare_alteration_refs(&entity_refs, &operation_refs)
            .ok_or(AlterClientQuotasCallAdmissionFailure::Request)?;
        let source = AlterClientQuotasRequestRef::new(&alterations, plan.validate_only());
        let request = alter_client_quotas_request(source, retained_limit)
            .map_err(|_source| AlterClientQuotasCallAdmissionFailure::Request)?;
        drop(alterations);
        drop(operation_refs);
        drop(entity_refs);
        let call = driver
            .submit_alter_client_quotas(request, deadline)
            .map_err(|_source| AlterClientQuotasCallAdmissionFailure::Driver)?;
        Ok(Self {
            call: Some(call),
            plan: Some(plan),
        })
    }

    /// Extracts a ready raw terminal without blocking.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AlterClientQuotasRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_alter_client_quotas_terminal(
                    selected_version,
                    result,
                    route_token,
                    self.plan.take()?,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredAlterClientQuotasCall> {
        let Self { call, plan } = self;
        call.map(|call| {
            drop(call);
            drop(plan);
            RecoveredAlterClientQuotasCall::new()
        })
    }
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

/// Definitely-unsent bounded-driver rejection.
#[must_use = "a rejected AlterClientQuotas call must become operation input"]
pub(crate) enum AlterClientQuotasCallAdmissionFailure {
    Request,
    Driver,
}
