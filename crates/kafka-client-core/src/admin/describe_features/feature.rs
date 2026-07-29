//! Generated-type-free supported and finalized Kafka feature facts.

/// One broker-supported feature range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesSupportedFeature {
    name: String,
    min_version: i16,
    max_version: i16,
}

impl DescribeFeaturesSupportedFeature {
    /// Creates one protocol-normalized supported-feature fact.
    pub const fn new(name: String, min_version: i16, max_version: i16) -> Self {
        Self {
            name,
            min_version,
            max_version,
        }
    }

    /// Returns the exact feature name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the broker's minimum supported level.
    pub const fn min_version(&self) -> i16 {
        self.min_version
    }

    /// Returns the broker's maximum supported level.
    pub const fn max_version(&self) -> i16 {
        self.max_version
    }

    /// Consumes this fact into stable scalar parts.
    pub fn into_parts(self) -> (String, i16, i16) {
        (self.name, self.min_version, self.max_version)
    }

    pub(crate) const fn range_is_well_formed(&self) -> bool {
        self.min_version >= 0 && self.min_version <= self.max_version
    }

    pub(crate) fn name_capacity(&self) -> usize {
        self.name.capacity()
    }
}

/// One cluster-wide finalized feature range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeFeaturesFinalizedFeature {
    name: String,
    min_version_level: i16,
    max_version_level: i16,
}

impl DescribeFeaturesFinalizedFeature {
    /// Creates one protocol-normalized finalized-feature fact.
    pub const fn new(name: String, min_version_level: i16, max_version_level: i16) -> Self {
        Self {
            name,
            min_version_level,
            max_version_level,
        }
    }

    /// Returns the exact feature name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the cluster-wide minimum finalized level.
    pub const fn min_version_level(&self) -> i16 {
        self.min_version_level
    }

    /// Returns the cluster-wide maximum finalized level.
    pub const fn max_version_level(&self) -> i16 {
        self.max_version_level
    }

    /// Consumes this fact into stable scalar parts.
    pub fn into_parts(self) -> (String, i16, i16) {
        (self.name, self.min_version_level, self.max_version_level)
    }

    pub(crate) const fn range_is_well_formed(&self) -> bool {
        self.min_version_level >= 0 && self.min_version_level <= self.max_version_level
    }

    pub(crate) fn name_capacity(&self) -> usize {
        self.name.capacity()
    }
}
