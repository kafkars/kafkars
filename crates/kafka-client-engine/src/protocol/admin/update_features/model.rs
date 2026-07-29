//! Generated-free borrowed update intent and normalized API-key 57 facts.

/// Explicit upgrade or downgrade intent for one finalized Kafka feature.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UpdateFeatureMode {
    /// Raise or establish the finalized level without downgrade permission.
    Upgrade,
    /// Permit only a broker-classified lossless downgrade or deletion.
    SafeDowngrade,
    /// Explicitly permit a potentially lossy downgrade or deletion.
    UnsafeDowngrade,
}

/// One borrowed feature update retained canonically by the operation owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct UpdateFeatureRef<'a> {
    feature: &'a str,
    max_version_level: i16,
    mode: UpdateFeatureMode,
}

impl<'a> UpdateFeatureRef<'a> {
    pub(crate) const fn new(
        feature: &'a str,
        max_version_level: i16,
        mode: UpdateFeatureMode,
    ) -> Self {
        Self {
            feature,
            max_version_level,
            mode,
        }
    }

    pub(crate) const fn feature(self) -> &'a str {
        self.feature
    }

    pub(crate) const fn max_version_level(self) -> i16 {
        self.max_version_level
    }

    pub(crate) const fn mode(self) -> UpdateFeatureMode {
        self.mode
    }
}

/// One caller-ordered update batch and its explicit validation-only intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct UpdateFeaturesRequestPlan<'a> {
    updates: &'a [UpdateFeatureRef<'a>],
    validate_only: bool,
}

impl<'a> UpdateFeaturesRequestPlan<'a> {
    pub(crate) const fn new(updates: &'a [UpdateFeatureRef<'a>], validate_only: bool) -> Self {
        Self {
            updates,
            validate_only,
        }
    }

    pub(crate) const fn updates(self) -> &'a [UpdateFeatureRef<'a>] {
        self.updates
    }

    pub(crate) const fn validate_only(self) -> bool {
        self.validate_only
    }
}

/// Exact signed Kafka error with a bounded optional diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedUpdateFeaturesError {
    code: i16,
    message: Option<String>,
    message_truncated: bool,
}

impl NormalizedUpdateFeaturesError {
    pub(super) const fn new(code: i16, message: Option<String>, message_truncated: bool) -> Self {
        Self {
            code,
            message,
            message_truncated,
        }
    }

    pub(crate) fn into_parts(self) -> (i16, Option<String>, bool) {
        (self.code, self.message, self.message_truncated)
    }

    pub(super) fn retained_message_bytes(&self) -> usize {
        self.message.as_ref().map_or(0, String::capacity)
    }
}

/// One caller-ordered successful or broker-rejected feature update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedUpdateFeatureResult {
    feature: String,
    error: Option<NormalizedUpdateFeaturesError>,
}

impl NormalizedUpdateFeatureResult {
    pub(super) const fn new(feature: String, error: Option<NormalizedUpdateFeaturesError>) -> Self {
        Self { feature, error }
    }

    pub(crate) fn into_parts(self) -> (String, Option<NormalizedUpdateFeaturesError>) {
        (self.feature, self.error)
    }

    pub(super) fn retained_text_bytes(&self) -> Option<usize> {
        self.feature.capacity().checked_add(
            self.error
                .as_ref()
                .map_or(0, NormalizedUpdateFeaturesError::retained_message_bytes),
        )
    }
}

/// Whole-response distinction between a top-level rejection and ordered results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NormalizedUpdateFeaturesOutcome {
    /// The controller rejected the complete batch with one exact signed code.
    TopLevelError(NormalizedUpdateFeaturesError),
    /// Caller-ordered v0-v1 results or synthesized v2 all-success results.
    Results(Vec<NormalizedUpdateFeatureResult>),
}

/// Bounded normalized response retained above the generated protocol seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedUpdateFeaturesResponse {
    throttle_time_ms: u32,
    outcome: NormalizedUpdateFeaturesOutcome,
    retained_bytes: usize,
}

impl NormalizedUpdateFeaturesResponse {
    pub(super) const fn new(
        throttle_time_ms: u32,
        outcome: NormalizedUpdateFeaturesOutcome,
        retained_bytes: usize,
    ) -> Self {
        Self {
            throttle_time_ms,
            outcome,
            retained_bytes,
        }
    }

    pub(crate) fn into_parts(self) -> (u32, NormalizedUpdateFeaturesOutcome, usize) {
        (self.throttle_time_ms, self.outcome, self.retained_bytes)
    }
}
