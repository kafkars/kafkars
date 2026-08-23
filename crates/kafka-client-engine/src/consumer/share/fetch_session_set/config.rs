//! Bounded core, wire, response, and decode policy for hosted share sessions.

use std::sync::Arc;

use kafka_client_core::{ByteCount, ShareAcquisitionPolicy};

use crate::{
    config::ValidatedShareConsumerFetchConfig,
    protocol::{
        consumer::share_fetch::{
            SHARE_FETCH_MAX_RANGES, ShareFetchRequestSettings, ShareFetchResponseLimits,
        },
        fetch::FetchDecodeLimits,
    },
};

use super::ShareFetchSessionSetOpenError;

/// Immutable bounded settings captured before one broker session opens.
pub(in crate::consumer::share) struct ShareFetchSessionConfig {
    pub(in crate::consumer::share) group: Arc<str>,
    pub(in crate::consumer::share) member: Arc<str>,
    pub(in crate::consumer::share) policy: ShareAcquisitionPolicy,
    pub(in crate::consumer::share) settings: ShareFetchRequestSettings,
    pub(in crate::consumer::share) response_limits: ShareFetchResponseLimits,
    pub(in crate::consumer::share) decode_limits: FetchDecodeLimits,
}

impl ShareFetchSessionConfig {
    pub(in crate::consumer::share) const fn new(
        group: Arc<str>,
        member: Arc<str>,
        policy: ShareAcquisitionPolicy,
        settings: ShareFetchRequestSettings,
        response_limits: ShareFetchResponseLimits,
        decode_limits: FetchDecodeLimits,
    ) -> Self {
        Self {
            group,
            member,
            policy,
            settings,
            response_limits,
            decode_limits,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct CompiledShareFetchSessionConfig {
    policy: ShareAcquisitionPolicy,
    settings: ShareFetchRequestSettings,
    response_limits: ShareFetchResponseLimits,
    decode_limits: FetchDecodeLimits,
}

impl CompiledShareFetchSessionConfig {
    pub(super) fn with_identity(
        self,
        group: Arc<str>,
        member: Arc<str>,
    ) -> ShareFetchSessionConfig {
        ShareFetchSessionConfig::new(
            group,
            member,
            self.policy,
            self.settings,
            self.response_limits,
            self.decode_limits,
        )
    }
}

pub(super) fn compile_session_config(
    config: ValidatedShareConsumerFetchConfig,
) -> Result<CompiledShareFetchSessionConfig, ShareFetchSessionSetOpenError> {
    let decode_limits = FetchDecodeLimits::default();
    let max_records = u64::try_from(decode_limits.max_records)
        .map_err(|_error| ShareFetchSessionSetOpenError::Allocation)?;
    let max_bytes = u64::try_from(decode_limits.max_response_retained_bytes)
        .map_err(|_error| ShareFetchSessionSetOpenError::Allocation)?;
    let policy = ShareAcquisitionPolicy::try_new(
        SHARE_FETCH_MAX_RANGES,
        max_records,
        ByteCount::new(max_bytes),
    )
    .map_err(ShareFetchSessionSetOpenError::Policy)?;
    Ok(CompiledShareFetchSessionConfig {
        policy,
        settings: ShareFetchRequestSettings {
            max_wait_ms: config.max_wait_ms(),
            min_bytes: config.min_bytes(),
            max_bytes: config.max_bytes(),
            max_records: config.max_records(),
            batch_size: config.batch_size(),
        },
        response_limits: ShareFetchResponseLimits::new(
            max_records,
            decode_limits.max_response_retained_bytes,
        ),
        decode_limits,
    })
}
