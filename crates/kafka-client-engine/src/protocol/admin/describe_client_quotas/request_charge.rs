//! Shared generated-request and canonicalization-scratch charging.

use core::mem::size_of;

use kafka_client_core::{ClientQuotaMatch, DescribeClientQuotasPlan};
use kafka_wire::describe_client_quotas_request::ComponentData;

use super::{DescribeClientQuotaFilterComponentRef, DescribeClientQuotaMatchRef};

pub(crate) fn plan_request_peak_charge(plan: &DescribeClientQuotasPlan) -> Option<usize> {
    peak_charge(
        plan.components().len(),
        plan.components().iter().map(|component| {
            let exact = match component.match_kind() {
                ClientQuotaMatch::Exact(value) => Some(value.as_str()),
                ClientQuotaMatch::Default | ClientQuotaMatch::AnySpecified => None,
            };
            (component.entity_type(), exact)
        }),
    )
}

pub(super) fn borrowed_request_peak_charge(
    components: &[DescribeClientQuotaFilterComponentRef<'_>],
) -> Option<usize> {
    peak_charge(
        components.len(),
        components.iter().map(|component| {
            let exact = match component.match_() {
                DescribeClientQuotaMatchRef::Exact(value) => Some(value),
                DescribeClientQuotaMatchRef::Default
                | DescribeClientQuotaMatchRef::AnySpecified => None,
            };
            (component.entity_type(), exact)
        }),
    )
}

fn peak_charge<'a>(
    component_count: usize,
    mut components: impl Iterator<Item = (&'a str, Option<&'a str>)>,
) -> Option<usize> {
    components.try_fold(
        component_count
            .checked_mul(size_of::<ComponentData>())?
            .checked_add(
                component_count
                    .checked_mul(size_of::<DescribeClientQuotaFilterComponentRef<'static>>())?,
            )?
            .checked_add(component_count.checked_mul(size_of::<&str>())?)?,
        |bytes, (entity_type, exact)| {
            bytes
                .checked_add(entity_type.len())?
                .checked_add(exact.map_or(0, str::len))
        },
    )
}
