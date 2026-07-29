//! Owned normalized client-quota values translated into core vocabulary.

use core::num::NonZeroI16;

use kafka_client_core::{
    DescribeClientQuotaEntity, DescribeClientQuotaEntityComponent, DescribeClientQuotaValue,
    DescribeClientQuotasBatch, DescribeClientQuotasBrokerError, DescribeClientQuotasInput,
};

use crate::protocol::admin::describe_client_quotas::NormalizedClientQuotaEntry;

pub(super) fn normalized_input(
    throttle_time_ms: u32,
    error_code: i16,
    error_message: Option<String>,
    error_message_truncated: bool,
    entries: Vec<NormalizedClientQuotaEntry>,
) -> DescribeClientQuotasInput {
    match NonZeroI16::new(error_code) {
        Some(code) => DescribeClientQuotasInput::BrokerRejected {
            error: DescribeClientQuotasBrokerError::new(
                code,
                error_message,
                error_message_truncated,
            ),
        },
        None => DescribeClientQuotasInput::BrokerResponded {
            batch: DescribeClientQuotasBatch::new(
                throttle_time_ms,
                entries.into_iter().map(core_entry).collect(),
            ),
        },
    }
}

fn core_entry(entry: NormalizedClientQuotaEntry) -> DescribeClientQuotaEntity {
    let (components, values) = entry.into_parts();
    DescribeClientQuotaEntity::new(
        components
            .into_iter()
            .map(|component| {
                let (entity_type, entity_name) = component.into_parts();
                DescribeClientQuotaEntityComponent::new(entity_type, entity_name)
            })
            .collect(),
        values
            .into_iter()
            .map(|value| {
                let (key, value) = value.into_parts();
                DescribeClientQuotaValue::new(key, value)
            })
            .collect(),
    )
}
