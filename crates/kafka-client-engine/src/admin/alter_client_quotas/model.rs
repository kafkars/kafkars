//! Engine-owned, wire-free intent for one Admin `AlterClientQuotas` batch.

use kafka_client_core::{
    AlterClientQuotaEntity as CoreEntity, AlterClientQuotaEntityComponent as CoreComponent,
    AlterClientQuotaEntry as CoreEntry, AlterClientQuotaOperation as CoreOperation,
    AlterClientQuotasPlan as CorePlan, AlterClientQuotasPlanError as CorePlanError,
};

/// One canonical component identifying a client-quota entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotaEntityComponent {
    entity_type: String,
    entity_name: Option<String>,
}

impl AlterClientQuotaEntityComponent {
    /// Creates inert entity-component intent.
    pub const fn new(entity_type: String, entity_name: Option<String>) -> Self {
        Self {
            entity_type,
            entity_name,
        }
    }

    /// Returns Kafka's entity-type name.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns the explicit name, or `None` for Kafka's default entity.
    pub fn entity_name(&self) -> Option<&str> {
        self.entity_name.as_deref()
    }

    /// Consumes this component into stable scalar parts.
    pub fn into_parts(self) -> (String, Option<String>) {
        (self.entity_type, self.entity_name)
    }

    fn into_core(self) -> CoreComponent {
        let (entity_type, entity_name) = self.into_parts();
        CoreComponent::new(
            canonical_string(entity_type),
            entity_name.map(canonical_string),
        )
    }
}

/// One client-quota entity, canonically identified by unique component types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterClientQuotaEntity {
    components: Vec<AlterClientQuotaEntityComponent>,
}

impl AlterClientQuotaEntity {
    /// Creates inert entity intent. Validation is deferred to submission.
    pub const fn new(components: Vec<AlterClientQuotaEntityComponent>) -> Self {
        Self { components }
    }

    /// Returns entity components in caller order before submission.
    pub fn components(&self) -> &[AlterClientQuotaEntityComponent] {
        &self.components
    }

    /// Consumes this entity into its components.
    pub fn into_components(self) -> Vec<AlterClientQuotaEntityComponent> {
        self.components
    }

    fn canonicalize(mut self) -> Self {
        self.components.shrink_to_fit();
        self
    }

    fn into_core(self) -> CoreEntity {
        CoreEntity::new(
            self.into_components()
                .into_iter()
                .map(AlterClientQuotaEntityComponent::into_core)
                .collect(),
        )
    }
}

/// One quota-key mutation.
#[derive(Clone, Debug, PartialEq)]
pub enum AlterClientQuotaOperation {
    /// Sets a quota key to one finite numeric value.
    Set {
        /// Kafka's quota configuration key.
        key: String,
        /// The exact finite replacement value.
        value: f64,
    },
    /// Removes a quota key from this entity.
    Remove {
        /// Kafka's quota configuration key.
        key: String,
    },
}

impl AlterClientQuotaOperation {
    /// Creates inert set intent.
    pub fn set(key: String, value: f64) -> Self {
        Self::Set { key, value }
    }

    /// Creates inert removal intent.
    pub fn remove(key: String) -> Self {
        Self::Remove { key }
    }

    /// Returns Kafka's quota configuration key.
    pub fn key(&self) -> &str {
        match self {
            Self::Set { key, .. } | Self::Remove { key } => key,
        }
    }

    /// Consumes this operation into core-owned policy.
    fn into_core(self) -> CoreOperation {
        match self {
            Self::Set { key, value } => CoreOperation::set(canonical_string(key), value),
            Self::Remove { key } => CoreOperation::remove(canonical_string(key)),
        }
    }
}

/// One entity and its nonempty quota-key mutation set.
#[derive(Clone, Debug, PartialEq)]
pub struct AlterClientQuotaEntry {
    entity: AlterClientQuotaEntity,
    operations: Vec<AlterClientQuotaOperation>,
}

impl AlterClientQuotaEntry {
    /// Creates inert entity-alteration intent.
    pub const fn new(
        entity: AlterClientQuotaEntity,
        operations: Vec<AlterClientQuotaOperation>,
    ) -> Self {
        Self { entity, operations }
    }

    /// Returns the target entity.
    pub const fn entity(&self) -> &AlterClientQuotaEntity {
        &self.entity
    }

    /// Returns caller-ordered quota-key operations.
    pub fn operations(&self) -> &[AlterClientQuotaOperation] {
        &self.operations
    }

    /// Consumes this entry into its entity and operations.
    pub fn into_parts(self) -> (AlterClientQuotaEntity, Vec<AlterClientQuotaOperation>) {
        (self.entity, self.operations)
    }

    fn canonicalize(mut self) -> Self {
        self.entity = self.entity.canonicalize();
        self.operations.shrink_to_fit();
        self
    }

    fn into_core(self) -> CoreEntry {
        let (entity, operations) = self.into_parts();
        CoreEntry::new(
            entity.into_core(),
            operations
                .into_iter()
                .map(AlterClientQuotaOperation::into_core)
                .collect(),
        )
    }
}

/// One bounded, wire-free quota-alteration request.
#[derive(Clone, Debug, PartialEq)]
pub struct AlterClientQuotasRequest {
    entries: Vec<AlterClientQuotaEntry>,
    validate_only: bool,
}

impl AlterClientQuotasRequest {
    /// Creates inert request intent.
    pub const fn new(entries: Vec<AlterClientQuotaEntry>, validate_only: bool) -> Self {
        Self {
            entries,
            validate_only,
        }
    }

    /// Returns caller-ordered entity alterations.
    pub fn entries(&self) -> &[AlterClientQuotaEntry] {
        &self.entries
    }

    /// Returns whether Kafka should validate without applying the mutations.
    pub const fn validate_only(&self) -> bool {
        self.validate_only
    }

    /// Consumes this request into stable scalar parts.
    pub fn into_parts(self) -> (Vec<AlterClientQuotaEntry>, bool) {
        (self.entries, self.validate_only)
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.entries = self
            .entries
            .into_iter()
            .map(AlterClientQuotaEntry::canonicalize)
            .collect();
        self.entries.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, CorePlanError> {
        let (entries, validate_only) = self.into_parts();
        CorePlan::new(
            entries
                .into_iter()
                .map(AlterClientQuotaEntry::into_core)
                .collect(),
            validate_only,
        )
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
