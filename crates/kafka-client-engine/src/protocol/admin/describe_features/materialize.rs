//! Fallible copying, duplicate rejection, and canonical feature ordering.

use kafka_wire::ApiVersionsResponse;

use super::{
    DescribeFeaturesProtocolFailure, NormalizedDescribeFeaturesFinalizedFeature,
    NormalizedDescribeFeaturesResponse, NormalizedDescribeFeaturesSupportedFeature,
    retention::{ensure_limit, normalized_success_charge},
};

pub(super) fn materialize_success(
    throttle_time_ms: u32,
    response: &ApiVersionsResponse,
    supported_features_complete: bool,
    finalized_features_epoch: Option<i64>,
    source_required: usize,
    retained_limit: usize,
) -> Result<NormalizedDescribeFeaturesResponse, DescribeFeaturesProtocolFailure> {
    let mut supported = Vec::new();
    supported
        .try_reserve_exact(response.supported_features.len())
        .map_err(|_| DescribeFeaturesProtocolFailure::Allocation {
            field: "supported_features",
            requested: response.supported_features.len(),
        })?;
    for feature in &response.supported_features {
        supported.push(NormalizedDescribeFeaturesSupportedFeature::new(
            copy_name("supported_features", feature.name.as_str())?,
            feature.min_version,
            feature.max_version,
        ));
    }
    supported.sort_unstable_by(|left, right| left.name().as_bytes().cmp(right.name().as_bytes()));
    if supported
        .windows(2)
        .any(|pair| pair[0].name() == pair[1].name())
    {
        return Err(DescribeFeaturesProtocolFailure::DuplicateFeatureName {
            field: "supported_features",
        });
    }

    let mut finalized = Vec::new();
    finalized
        .try_reserve_exact(response.finalized_features.len())
        .map_err(|_| DescribeFeaturesProtocolFailure::Allocation {
            field: "finalized_features",
            requested: response.finalized_features.len(),
        })?;
    for feature in &response.finalized_features {
        finalized.push(NormalizedDescribeFeaturesFinalizedFeature::new(
            copy_name("finalized_features", feature.name.as_str())?,
            feature.min_version_level,
            feature.max_version_level,
        ));
    }
    finalized.sort_unstable_by(|left, right| left.name().as_bytes().cmp(right.name().as_bytes()));
    if finalized
        .windows(2)
        .any(|pair| pair[0].name() == pair[1].name())
    {
        return Err(DescribeFeaturesProtocolFailure::DuplicateFeatureName {
            field: "finalized_features",
        });
    }
    let normalized = normalized_success_charge(&supported, &finalized).unwrap_or(usize::MAX);
    ensure_limit(normalized, retained_limit)?;
    Ok(NormalizedDescribeFeaturesResponse::new(
        throttle_time_ms,
        0,
        supported,
        supported_features_complete,
        finalized_features_epoch,
        finalized,
        response.zk_migration_ready,
        source_required.max(normalized),
    ))
}

fn copy_name(field: &'static str, source: &str) -> Result<String, DescribeFeaturesProtocolFailure> {
    let mut name = String::new();
    name.try_reserve_exact(source.len()).map_err(|_| {
        DescribeFeaturesProtocolFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    name.push_str(source);
    Ok(name)
}
