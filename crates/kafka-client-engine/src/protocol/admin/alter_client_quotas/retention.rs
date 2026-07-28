//! Checked request, correlation-scratch, and terminal byte accounting.

use core::mem::size_of;

use kafka_wire::{
    AlterClientQuotasRequest,
    alter_client_quotas_request::{
        EntityData as RequestEntityData, EntryData as RequestEntryData, OpData,
    },
};

use super::{
    AlterClientQuotaAlterationRef, AlterClientQuotaEntityComponentRef,
    AlterClientQuotaOperationRef, AlterClientQuotasRequestRef, model::CanonicalEntityComponentRef,
    request_validation::CanonicalAlterationRef,
};

pub(super) const MAX_ALTERATIONS: usize = 1024;
pub(super) const MAX_ENTITY_COMPONENTS: usize = 128;
pub(super) const MAX_OPERATIONS: usize = 128;
pub(super) const MAX_ENTITY_TYPE_BYTES: usize = 256;
pub(super) const MAX_ENTITY_NAME_BYTES: usize = 256;
pub(super) const MAX_QUOTA_KEY_BYTES: usize = 256;
pub(super) const MAX_DIAGNOSTIC_BYTES: usize = 1024;

pub(super) fn request_peak_charge(request: AlterClientQuotasRequestRef<'_>) -> Option<usize> {
    let retained = generated_request_retained_charge(request)?;
    let scratch = request_canonical_scratch_charge(request)?;
    retained
        .checked_add(scratch)?
        .checked_add(caller_reference_scratch_charge(request)?)
}

pub(super) fn generated_request_retained_charge(
    request: AlterClientQuotasRequestRef<'_>,
) -> Option<usize> {
    request.alterations().iter().try_fold(
        size_of::<AlterClientQuotasRequest>().checked_add(
            request
                .alterations()
                .len()
                .checked_mul(size_of::<RequestEntryData>())?,
        )?,
        |bytes, alteration| {
            let bytes = bytes
                .checked_add(
                    alteration
                        .entity()
                        .len()
                        .checked_mul(size_of::<RequestEntityData>())?,
                )?
                .checked_add(
                    alteration
                        .operations()
                        .len()
                        .checked_mul(size_of::<OpData>())?,
                )?;
            alteration
                .entity()
                .iter()
                .try_fold(bytes, |bytes, component| {
                    bytes
                        .checked_add(component.entity_type().len())?
                        .checked_add(component.entity_name().map_or(0, str::len))
                })
                .and_then(|bytes| {
                    alteration
                        .operations()
                        .iter()
                        .try_fold(bytes, |bytes, operation| {
                            bytes.checked_add(operation.key().len())
                        })
                })
        },
    )
}

pub(super) fn request_canonical_scratch_charge(
    request: AlterClientQuotasRequestRef<'_>,
) -> Option<usize> {
    request.alterations().iter().try_fold(
        size_of::<Vec<CanonicalAlterationRef<'static>>>()
            .checked_add(
                request
                    .alterations()
                    .len()
                    .checked_mul(size_of::<CanonicalAlterationRef<'static>>())?,
            )?
            .checked_add(
                request
                    .alterations()
                    .len()
                    .checked_mul(size_of::<&CanonicalAlterationRef<'static>>())?,
            )?,
        |bytes, alteration| {
            bytes
                .checked_add(
                    alteration
                        .entity()
                        .len()
                        .checked_mul(size_of::<CanonicalEntityComponentRef<'static>>())?,
                )?
                .checked_add(
                    alteration
                        .operations()
                        .len()
                        .checked_mul(size_of::<AlterClientQuotaOperationRef<'static>>())?,
                )?
                .checked_add(
                    alteration
                        .operations()
                        .len()
                        .checked_mul(size_of::<&'static str>())?,
                )
        },
    )
}

pub(super) fn caller_reference_scratch_charge(
    request: AlterClientQuotasRequestRef<'_>,
) -> Option<usize> {
    request.alterations().iter().try_fold(
        size_of::<Vec<AlterClientQuotaAlterationRef<'static>>>().checked_add(
            request
                .alterations()
                .len()
                .checked_mul(size_of::<AlterClientQuotaAlterationRef<'static>>())?,
        )?,
        |bytes, alteration| {
            bytes
                .checked_add(size_of::<Vec<AlterClientQuotaEntityComponentRef<'static>>>())?
                .checked_add(
                    alteration
                        .entity()
                        .len()
                        .checked_mul(size_of::<AlterClientQuotaEntityComponentRef<'static>>())?,
                )?
                .checked_add(size_of::<Vec<AlterClientQuotaOperationRef<'static>>>())?
                .checked_add(
                    alteration
                        .operations()
                        .len()
                        .checked_mul(size_of::<AlterClientQuotaOperationRef<'static>>())?,
                )
        },
    )
}
