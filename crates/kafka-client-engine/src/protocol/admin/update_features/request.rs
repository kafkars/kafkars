//! Fallible bounded materialization of flexible API-key 57 requests.

use core::mem::size_of;

use kafka_wire::{UpdateFeaturesRequest, update_features_request::FeatureUpdateKey};
use kafka_wire_core::StrBytes;

use super::{
    PreparedUpdateFeaturesRequest, UpdateFeatureMode, UpdateFeaturesRequestPlan,
    version::update_features_version_floor,
};

pub(super) const MAX_UPDATES: usize = 4 * 1024;
pub(super) const MAX_FEATURE_NAME_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_REQUEST_TEXT_BYTES: usize = 1024 * 1024;

/// Invalid update intent, timeout, allocation, or retained-capacity failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateFeaturesRequestFailure {
    EmptyUpdates,
    TooManyUpdates {
        actual: usize,
        max: usize,
    },
    EmptyFeatureName,
    FeatureNameTooLong {
        actual: usize,
        max: usize,
    },
    FeatureBytesExceeded {
        required: usize,
        max: usize,
    },
    DuplicateFeature,
    NegativeMaxVersionLevel {
        actual: i16,
    },
    DeletionRequiresDowngrade,
    NegativeTimeout {
        actual: i32,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Builds one generated request and the exact API-version floor it requires.
///
/// Floor-zero ownership retains separate legacy and modern generated forms
/// because safe downgrade intent moved from `allow_downgrade` to
/// `upgrade_type`; encoding performs no allocation after driver handoff.
pub(crate) fn update_features_request(
    plan: UpdateFeaturesRequestPlan<'_>,
    timeout_ms: i32,
    retained_limit: usize,
) -> Result<(PreparedUpdateFeaturesRequest, i16), UpdateFeaturesRequestFailure> {
    validate_request(plan, timeout_ms)?;
    let minimum_version = update_features_version_floor(plan);
    let copies = if minimum_version == 0 { 2 } else { 1 };
    let required = request_charge(plan, copies).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;

    let legacy = (minimum_version == 0)
        .then(|| materialize_request(plan, timeout_ms, true))
        .transpose()?;
    let modern = materialize_request(plan, timeout_ms, false)?;
    let prepared = PreparedUpdateFeaturesRequest::new(legacy, modern);
    ensure_limit(prepared.retained_heap_bytes(), retained_limit)?;
    Ok((prepared, minimum_version))
}

fn materialize_request(
    plan: UpdateFeaturesRequestPlan<'_>,
    timeout_ms: i32,
    legacy: bool,
) -> Result<UpdateFeaturesRequest, UpdateFeaturesRequestFailure> {
    let mut updates = Vec::new();
    updates
        .try_reserve_exact(plan.updates().len())
        .map_err(|_| UpdateFeaturesRequestFailure::Allocation {
            field: "feature_updates",
            requested: plan.updates().len(),
        })?;
    for update in plan.updates() {
        let mut generated = FeatureUpdateKey::default();
        generated.feature = copy_feature(update.feature())?;
        generated.max_version_level = update.max_version_level();
        if legacy {
            generated.allow_downgrade = update.mode() == UpdateFeatureMode::SafeDowngrade;
        } else {
            generated.upgrade_type = match update.mode() {
                UpdateFeatureMode::Upgrade => 1,
                UpdateFeatureMode::SafeDowngrade => 2,
                UpdateFeatureMode::UnsafeDowngrade => 3,
            };
        }
        updates.push(generated);
    }
    let mut request = UpdateFeaturesRequest::default();
    request.timeout_ms = timeout_ms;
    request.feature_updates = updates;
    request.validate_only = plan.validate_only();
    Ok(request)
}

fn validate_request(
    plan: UpdateFeaturesRequestPlan<'_>,
    timeout_ms: i32,
) -> Result<(), UpdateFeaturesRequestFailure> {
    if timeout_ms < 0 {
        return Err(UpdateFeaturesRequestFailure::NegativeTimeout { actual: timeout_ms });
    }
    if plan.updates().is_empty() {
        return Err(UpdateFeaturesRequestFailure::EmptyUpdates);
    }
    if plan.updates().len() > MAX_UPDATES {
        return Err(UpdateFeaturesRequestFailure::TooManyUpdates {
            actual: plan.updates().len(),
            max: MAX_UPDATES,
        });
    }
    let mut feature_bytes = 0usize;
    for (index, update) in plan.updates().iter().copied().enumerate() {
        validate_update(update)?;
        feature_bytes = feature_bytes
            .checked_add(update.feature().len())
            .unwrap_or(usize::MAX);
        if feature_bytes > MAX_REQUEST_TEXT_BYTES {
            return Err(UpdateFeaturesRequestFailure::FeatureBytesExceeded {
                required: feature_bytes,
                max: MAX_REQUEST_TEXT_BYTES,
            });
        }
        if plan.updates()[..index]
            .iter()
            .any(|prior| prior.feature() == update.feature())
        {
            return Err(UpdateFeaturesRequestFailure::DuplicateFeature);
        }
    }
    Ok(())
}

fn validate_update(
    update: super::UpdateFeatureRef<'_>,
) -> Result<(), UpdateFeaturesRequestFailure> {
    if update.feature().is_empty() {
        return Err(UpdateFeaturesRequestFailure::EmptyFeatureName);
    }
    if update.feature().len() > MAX_FEATURE_NAME_BYTES {
        return Err(UpdateFeaturesRequestFailure::FeatureNameTooLong {
            actual: update.feature().len(),
            max: MAX_FEATURE_NAME_BYTES,
        });
    }
    if update.max_version_level() < 0 {
        return Err(UpdateFeaturesRequestFailure::NegativeMaxVersionLevel {
            actual: update.max_version_level(),
        });
    }
    if update.max_version_level() == 0 && update.mode() == UpdateFeatureMode::Upgrade {
        return Err(UpdateFeaturesRequestFailure::DeletionRequiresDowngrade);
    }
    Ok(())
}

fn request_charge(plan: UpdateFeaturesRequestPlan<'_>, copies: usize) -> Option<usize> {
    let owners_per_request = plan
        .updates()
        .len()
        .checked_mul(size_of::<FeatureUpdateKey>())?;
    let text_per_request = plan.updates().iter().try_fold(0usize, |bytes, update| {
        bytes.checked_add(update.feature().len())
    })?;
    size_of::<PreparedUpdateFeaturesRequest>()
        .checked_add(copies.checked_mul(size_of::<UpdateFeaturesRequest>())?)?
        .checked_add(copies.checked_mul(owners_per_request)?)?
        .checked_add(copies.checked_mul(text_per_request)?)
}

fn copy_feature(source: &str) -> Result<StrBytes, UpdateFeaturesRequestFailure> {
    let mut feature = String::new();
    feature.try_reserve_exact(source.len()).map_err(|_| {
        UpdateFeaturesRequestFailure::Allocation {
            field: "feature",
            requested: source.len(),
        }
    })?;
    feature.push_str(source);
    Ok(feature.into())
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), UpdateFeaturesRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(UpdateFeaturesRequestFailure::RetainedBytes { required, limit })
}
