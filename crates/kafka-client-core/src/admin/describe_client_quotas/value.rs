//! Wire-free entity and quota-value facts returned by Kafka.

use core::cmp::Ordering;

/// One type/name component of a concrete client-quota entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeClientQuotaEntityComponent {
    entity_type: String,
    entity_name: Option<String>,
}

impl DescribeClientQuotaEntityComponent {
    /// Creates one protocol-normalized entity component for core validation.
    pub const fn new(entity_type: String, entity_name: Option<String>) -> Self {
        Self {
            entity_type,
            entity_name,
        }
    }

    /// Returns the stable entity-type identity.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns the concrete name, or `None` for the default entity.
    pub fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }

    /// Consumes this component into adapter-owned stable parts.
    pub fn into_parts(self) -> (String, Option<String>) {
        (self.entity_type, self.entity_name)
    }

    pub(crate) fn deterministic_cmp(&self, other: &Self) -> Ordering {
        self.entity_type
            .as_bytes()
            .cmp(other.entity_type.as_bytes())
            .then_with(|| optional_bytes_cmp(self.entity_name(), other.entity_name()))
    }
}

/// One quota key and finite numeric value for an entity.
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeClientQuotaValue {
    key: String,
    value: f64,
}

impl DescribeClientQuotaValue {
    /// Creates one protocol-normalized quota value for core validation.
    pub const fn new(key: String, value: f64) -> Self {
        Self { key, value }
    }

    /// Returns the stable quota configuration key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns Kafka's finite quota value.
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Consumes this quota into adapter-owned stable parts.
    pub fn into_parts(self) -> (String, f64) {
        (self.key, self.value)
    }

    pub(crate) fn deterministic_cmp(&self, other: &Self) -> Ordering {
        self.key.as_bytes().cmp(other.key.as_bytes())
    }
}

/// One concrete client-quota entity and all values Kafka returned for it.
#[derive(Clone, Debug, PartialEq)]
pub struct DescribeClientQuotaEntity {
    components: Vec<DescribeClientQuotaEntityComponent>,
    values: Vec<DescribeClientQuotaValue>,
}

impl DescribeClientQuotaEntity {
    /// Creates one protocol-normalized entity for core validation.
    pub const fn new(
        components: Vec<DescribeClientQuotaEntityComponent>,
        values: Vec<DescribeClientQuotaValue>,
    ) -> Self {
        Self { components, values }
    }

    /// Returns components in deterministic entity-type order.
    pub fn components(&self) -> &[DescribeClientQuotaEntityComponent] {
        &self.components
    }

    /// Returns quota values in deterministic key order.
    pub fn values(&self) -> &[DescribeClientQuotaValue] {
        &self.values
    }

    /// Consumes this entity into adapter-owned stable parts.
    pub fn into_parts(
        self,
    ) -> (
        Vec<DescribeClientQuotaEntityComponent>,
        Vec<DescribeClientQuotaValue>,
    ) {
        (self.components, self.values)
    }

    pub(crate) fn canonicalize(&mut self) {
        self.components
            .sort_unstable_by(DescribeClientQuotaEntityComponent::deterministic_cmp);
        self.values
            .sort_unstable_by(DescribeClientQuotaValue::deterministic_cmp);
    }

    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        self.components == other.components
    }

    pub(crate) fn deterministic_cmp(&self, other: &Self) -> Ordering {
        lexicographic_components_cmp(&self.components, &other.components)
    }
}

fn optional_bytes_cmp(left: Option<&str>, right: Option<&str>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => left.as_bytes().cmp(right.as_bytes()),
    }
}

fn lexicographic_components_cmp(
    left: &[DescribeClientQuotaEntityComponent],
    right: &[DescribeClientQuotaEntityComponent],
) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        let ordering = left.deterministic_cmp(right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}
