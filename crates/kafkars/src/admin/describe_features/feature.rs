//! Stable generated-free Kafka feature version ranges.

/// One broker-supported Kafka feature range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupportedFeature {
    name: String,
    min_version_level: i16,
    max_version_level: i16,
}

impl SupportedFeature {
    pub(crate) const fn new(name: String, min_version_level: i16, max_version_level: i16) -> Self {
        Self {
            name,
            min_version_level,
            max_version_level,
        }
    }

    /// Returns the exact Kafka feature name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the lowest version level supported by the responding broker.
    pub const fn min_version_level(&self) -> i16 {
        self.min_version_level
    }

    /// Returns the highest version level supported by the responding broker.
    pub const fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// Consumes the value into its stable generated-free parts.
    pub fn into_parts(self) -> (String, i16, i16) {
        (self.name, self.min_version_level, self.max_version_level)
    }
}

/// One cluster-wide finalized Kafka feature range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizedFeature {
    name: String,
    min_version_level: i16,
    max_version_level: i16,
}

impl FinalizedFeature {
    pub(crate) const fn new(name: String, min_version_level: i16, max_version_level: i16) -> Self {
        Self {
            name,
            min_version_level,
            max_version_level,
        }
    }

    /// Returns the exact Kafka feature name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the cluster-wide finalized minimum version level.
    pub const fn min_version_level(&self) -> i16 {
        self.min_version_level
    }

    /// Returns the cluster-wide finalized maximum version level.
    pub const fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// Consumes the value into its stable generated-free parts.
    pub fn into_parts(self) -> (String, i16, i16) {
        (self.name, self.min_version_level, self.max_version_level)
    }
}
