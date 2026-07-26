//! Raw versioned `OffsetFetch` terminal retained for the execution owner.

use kafka_driver::{ApiVersion, RequestError};
use kafka_wire::OffsetFetchResponse;

use super::key::GroupPositionOffsetFetchKey;

/// Uninterpreted response or driver-authoritative request failure.
#[must_use = "a raw group position terminal owns an unsettled assignment request"]
pub(crate) struct GroupPositionOffsetFetchTerminal {
    key: GroupPositionOffsetFetchKey,
    selected_version: Option<i16>,
    result: Result<OffsetFetchResponse, RequestError>,
}

impl GroupPositionOffsetFetchTerminal {
    pub(crate) const fn key(&self) -> &GroupPositionOffsetFetchKey {
        &self.key
    }

    pub(crate) const fn selected_version(&self) -> Option<i16> {
        self.selected_version
    }

    pub(crate) const fn result(&self) -> &Result<OffsetFetchResponse, RequestError> {
        &self.result
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        GroupPositionOffsetFetchKey,
        Option<i16>,
        Result<OffsetFetchResponse, RequestError>,
    ) {
        (self.key, self.selected_version, self.result)
    }
}

pub(super) fn retain_group_position_offset_fetch_terminal(
    key: GroupPositionOffsetFetchKey,
    selected_version: Option<ApiVersion>,
    result: Result<OffsetFetchResponse, RequestError>,
) -> GroupPositionOffsetFetchTerminal {
    GroupPositionOffsetFetchTerminal {
        key,
        selected_version: selected_version.map(ApiVersion::value),
        result,
    }
}
