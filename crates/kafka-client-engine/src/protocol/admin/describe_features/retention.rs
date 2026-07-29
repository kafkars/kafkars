//! Checked retained-capacity accounting for normalized feature facts.

use core::mem::size_of;

use kafka_wire::ApiVersionsResponse;

use super::{
    DescribeFeaturesProtocolFailure, NormalizedDescribeFeaturesFinalizedFeature,
    NormalizedDescribeFeaturesResponse, NormalizedDescribeFeaturesSupportedFeature,
};

pub(super) const fn error_charge() -> usize {
    size_of::<NormalizedDescribeFeaturesResponse>()
}

pub(super) fn success_source_charge(response: &ApiVersionsResponse) -> Option<usize> {
    let supported_owners = response
        .supported_features
        .len()
        .checked_mul(size_of::<NormalizedDescribeFeaturesSupportedFeature>())?;
    let finalized_owners = response
        .finalized_features
        .len()
        .checked_mul(size_of::<NormalizedDescribeFeaturesFinalizedFeature>())?;
    let text = response
        .supported_features
        .iter()
        .map(|feature| feature.name.len())
        .chain(
            response
                .finalized_features
                .iter()
                .map(|feature| feature.name.len()),
        )
        .try_fold(0usize, usize::checked_add)?;
    size_of::<NormalizedDescribeFeaturesResponse>()
        .checked_add(supported_owners)?
        .checked_add(finalized_owners)?
        .checked_add(text)
}

pub(super) fn normalized_success_charge(
    supported: &[NormalizedDescribeFeaturesSupportedFeature],
    finalized: &[NormalizedDescribeFeaturesFinalizedFeature],
) -> Option<usize> {
    let supported_owners = supported
        .len()
        .checked_mul(size_of::<NormalizedDescribeFeaturesSupportedFeature>())?;
    let finalized_owners = finalized
        .len()
        .checked_mul(size_of::<NormalizedDescribeFeaturesFinalizedFeature>())?;
    let text = supported
        .iter()
        .map(NormalizedDescribeFeaturesSupportedFeature::name_capacity)
        .chain(
            finalized
                .iter()
                .map(NormalizedDescribeFeaturesFinalizedFeature::name_capacity),
        )
        .try_fold(0usize, usize::checked_add)?;
    size_of::<NormalizedDescribeFeaturesResponse>()
        .checked_add(supported_owners)?
        .checked_add(finalized_owners)?
        .checked_add(text)
}

pub(super) fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeFeaturesProtocolFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeFeaturesProtocolFailure::RetainedBytes { required, limit })
}
