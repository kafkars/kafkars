//! Engine-owned scalar facts produced by `DescribeConfigs` normalization.

use core::num::NonZeroI16;

/// One normalized response with broker throttle retained as an observation fact.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NormalizedDescribeConfigsResponse {
    pub(crate) throttle_time_ms: u32,
    pub(crate) resources: Vec<NormalizedConfigResource>,
}

/// One resource result restored to request order.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NormalizedConfigResource {
    pub(crate) resource_type: i8,
    pub(crate) resource_name: String,
    pub(crate) outcome: Result<Vec<NormalizedConfigEntry>, NormalizedConfigResourceError>,
}

/// Exact broker rejection for one requested resource.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NormalizedConfigResourceError {
    pub(crate) code: NonZeroI16,
    pub(crate) message: Option<String>,
    pub(crate) message_truncated: bool,
}

/// One configuration entry without generated wire ownership.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NormalizedConfigEntry {
    pub(crate) name: String,
    pub(crate) value: Option<String>,
    pub(crate) read_only: bool,
    pub(crate) source: i8,
    pub(crate) sensitive: bool,
    pub(crate) synonyms: Vec<NormalizedConfigSynonym>,
    pub(crate) config_type: Option<i8>,
    pub(crate) documentation: Option<String>,
}

/// One configuration synonym without generated wire ownership.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NormalizedConfigSynonym {
    pub(crate) name: String,
    pub(crate) value: Option<String>,
    pub(crate) source: i8,
}
