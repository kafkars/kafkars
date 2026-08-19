//! Checked request, canonicalization-scratch, and result byte accounting.

use core::mem::size_of;

use kafka_client_core::{
    DescribeClientQuotaEntity, DescribeClientQuotaEntityComponent, DescribeClientQuotaValue,
    DescribeClientQuotasBatch, DescribeClientQuotasBrokerError,
};
use kafka_wire::DescribeClientQuotasResponse;

use super::{
    DescribeClientQuotaFilterComponentRef, NormalizedClientQuotaEntityComponent,
    NormalizedClientQuotaEntry, NormalizedClientQuotaValue, NormalizedDescribeClientQuotasResponse,
};

pub(super) const MAX_FILTER_COMPONENTS: usize = 128;
pub(super) const MAX_ENTRIES: usize = 16 * 1024;
pub(super) const MAX_ENTITY_COMPONENTS: usize = 1024 * 1024;
pub(super) const MAX_QUOTA_VALUES: usize = 1024 * 1024;
pub(super) const MAX_ENTITY_TYPE_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_ENTITY_NAME_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_QUOTA_KEY_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_DIAGNOSTIC_BYTES: usize = 1024;

pub(super) fn request_peak_charge(
    components: &[DescribeClientQuotaFilterComponentRef<'_>],
) -> Option<usize> {
    super::request_charge::borrowed_request_peak_charge(components)
}

pub(super) fn response_peak_charge(response: &DescribeClientQuotasResponse) -> Option<usize> {
    let entries = response.entries.as_deref().unwrap_or_default();
    let mut output = size_of::<NormalizedDescribeClientQuotasResponse>()
        .checked_add(
            entries
                .len()
                .checked_mul(size_of::<NormalizedClientQuotaEntry>())?,
        )?
        .checked_add(bounded_diagnostic_len(response.error_message.as_deref()))?;
    let mut scratch = entries
        .len()
        .checked_mul(size_of::<CanonicalEntryRef<'static>>())?;

    for entry in entries {
        output = output
            .checked_add(
                entry
                    .entity
                    .len()
                    .checked_mul(size_of::<NormalizedClientQuotaEntityComponent>())?,
            )?
            .checked_add(
                entry
                    .values
                    .len()
                    .checked_mul(size_of::<NormalizedClientQuotaValue>())?,
            )?;
        scratch = scratch
            .checked_add(
                entry
                    .entity
                    .len()
                    .checked_mul(size_of::<EntityComponentRef<'static>>())?,
            )?
            .checked_add(
                entry
                    .values
                    .len()
                    .checked_mul(size_of::<QuotaValueRef<'static>>())?,
            )?;
        for component in &entry.entity {
            output = output
                .checked_add(component.entity_type.len())?
                .checked_add(
                    component
                        .entity_name
                        .as_ref()
                        .map_or(0, kafka_wire_core::StrBytes::len),
                )?;
        }
        for value in &entry.values {
            output = output.checked_add(value.key.len())?;
        }
    }
    output
        .checked_add(scratch)?
        .checked_add(terminal_peak_charge(response)?)
}

fn terminal_peak_charge(response: &DescribeClientQuotasResponse) -> Option<usize> {
    if response.error_code != 0 {
        return size_of::<DescribeClientQuotasBrokerError>()
            .checked_add(bounded_diagnostic_len(response.error_message.as_deref()));
    }
    let entries = response.entries.as_deref().unwrap_or_default();
    entries.iter().try_fold(
        size_of::<DescribeClientQuotasBatch>().checked_add(
            entries
                .len()
                .checked_mul(size_of::<DescribeClientQuotaEntity>())?,
        )?,
        |bytes, entry| {
            let bytes = bytes
                .checked_add(
                    entry
                        .entity
                        .len()
                        .checked_mul(size_of::<DescribeClientQuotaEntityComponent>())?,
                )?
                .checked_add(
                    entry
                        .values
                        .len()
                        .checked_mul(size_of::<DescribeClientQuotaValue>())?,
                )?;
            let bytes = entry.entity.iter().try_fold(bytes, |bytes, component| {
                bytes.checked_add(component.entity_type.len())?.checked_add(
                    component
                        .entity_name
                        .as_ref()
                        .map_or(0, kafka_wire_core::StrBytes::len),
                )
            })?;
            entry
                .values
                .iter()
                .try_fold(bytes, |bytes, value| bytes.checked_add(value.key.len()))
        },
    )
}

pub(super) fn normalized_retained_charge(
    response: &NormalizedDescribeClientQuotasResponse,
) -> Option<usize> {
    response.entries.iter().try_fold(
        size_of::<NormalizedDescribeClientQuotasResponse>()
            .checked_add(
                response
                    .entries
                    .capacity()
                    .checked_mul(size_of::<NormalizedClientQuotaEntry>())?,
            )?
            .checked_add(response.error_message.as_ref().map_or(0, String::capacity))?,
        |bytes, entry| {
            let bytes = bytes
                .checked_add(
                    entry
                        .entity
                        .capacity()
                        .checked_mul(size_of::<NormalizedClientQuotaEntityComponent>())?,
                )?
                .checked_add(
                    entry
                        .values
                        .capacity()
                        .checked_mul(size_of::<NormalizedClientQuotaValue>())?,
                )?;
            let bytes = entry.entity.iter().try_fold(bytes, |bytes, component| {
                bytes
                    .checked_add(component.entity_type.capacity())?
                    .checked_add(component.entity_name.as_ref().map_or(0, String::capacity))
            })?;
            entry.values.iter().try_fold(bytes, |bytes, value| {
                bytes.checked_add(value.key.capacity())
            })
        },
    )
}

pub(super) fn bounded_diagnostic_len(message: Option<&str>) -> usize {
    let Some(message) = message else {
        return 0;
    };
    floor_char_boundary(message, MAX_DIAGNOSTIC_BYTES.min(message.len()))
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct EntityComponentRef<'a> {
    pub(super) entity_type: &'a str,
    pub(super) entity_name: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct QuotaValueRef<'a> {
    pub(super) key: &'a str,
    pub(super) value: f64,
}

#[derive(Debug, PartialEq)]
pub(super) struct CanonicalEntryRef<'a> {
    pub(super) entity: Vec<EntityComponentRef<'a>>,
    pub(super) values: Vec<QuotaValueRef<'a>>,
}
