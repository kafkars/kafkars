//! Stable inert client-quota entity filter values.

/// How one client-quota entity type is selected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientQuotaMatch {
    /// Selects the entity with this exact non-default name.
    Exact(String),
    /// Selects the unnamed default entity.
    Default,
    /// Selects every explicitly named entity of this type.
    AnySpecified,
}

impl ClientQuotaMatch {
    /// Selects one exact entity name.
    pub fn exact(name: impl Into<String>) -> Self {
        Self::Exact(name.into())
    }

    /// Selects the unnamed default entity.
    pub const fn default_entity() -> Self {
        Self::Default
    }

    /// Selects every explicitly named entity.
    pub const fn any_specified() -> Self {
        Self::AnySpecified
    }
}

/// One bounded entity-type component of a client-quota filter.
///
/// Construction is inert. Entity-type and exact-name bounds are checked only
/// when the surrounding builder is submitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientQuotaFilterComponent {
    entity_type: String,
    selection: ClientQuotaMatch,
}

impl ClientQuotaFilterComponent {
    /// Creates one inert entity-type selection.
    pub fn new(entity_type: impl Into<String>, selection: ClientQuotaMatch) -> Self {
        Self {
            entity_type: entity_type.into(),
            selection,
        }
    }

    /// Selects one exact non-default entity name.
    pub fn exact(entity_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self::new(entity_type, ClientQuotaMatch::Exact(name.into()))
    }

    /// Selects the unnamed default entity for this type.
    pub fn default_entity(entity_type: impl Into<String>) -> Self {
        Self::new(entity_type, ClientQuotaMatch::Default)
    }

    /// Selects every explicitly named entity for this type.
    pub fn any_specified(entity_type: impl Into<String>) -> Self {
        Self::new(entity_type, ClientQuotaMatch::AnySpecified)
    }

    /// Returns Kafka's entity-type name.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns this component's entity selection.
    pub const fn selection(&self) -> &ClientQuotaMatch {
        &self.selection
    }

    pub(crate) fn into_parts(self) -> (String, ClientQuotaMatch) {
        (self.entity_type, self.selection)
    }
}
