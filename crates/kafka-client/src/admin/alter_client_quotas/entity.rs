//! Stable canonical identity for one client-quota entity.

use super::super::ClientQuotaEntityComponent;

/// One client-quota entity identified by canonical entity-type components.
///
/// Construction is inert. Component bounds and uniqueness are checked only
/// when the surrounding alteration builder is submitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientQuotaEntity {
    components: Vec<ClientQuotaEntityComponent>,
}

impl ClientQuotaEntity {
    /// Creates one inert entity and restores canonical component order.
    pub fn new<I>(components: I) -> Self
    where
        I: IntoIterator<Item = ClientQuotaEntityComponent>,
    {
        let mut components: Vec<_> = components.into_iter().collect();
        components.sort_by(|left, right| {
            left.entity_type()
                .as_bytes()
                .cmp(right.entity_type().as_bytes())
                .then_with(|| {
                    left.entity_name()
                        .map(str::as_bytes)
                        .cmp(&right.entity_name().map(str::as_bytes))
                })
        });
        Self { components }
    }

    /// Returns components in entity-type then entity-name byte order.
    pub fn components(&self) -> &[ClientQuotaEntityComponent] {
        &self.components
    }

    /// Consumes the entity into its canonical components.
    pub fn into_components(self) -> Vec<ClientQuotaEntityComponent> {
        self.components
    }
}
