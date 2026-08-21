//! Linear public client-quota alterations translated only at engine submission.

use crate::admin::{
    ClientQuotaAlteration, ClientQuotaAlterationOperation, ClientQuotaEntity,
    ClientQuotaEntityComponent,
};

use super::engine::{
    Entity as EngineEntity, EntityComponent as EngineEntityComponent, Entry as EngineEntry,
    Operation as EngineOperation, Request as EngineRequest,
};

/// Client-quota alterations retained by the inert public builder.
pub(crate) struct AlterClientQuotasAdminRequest {
    alterations: Vec<ClientQuotaAlteration>,
    validate_only: bool,
}

impl AlterClientQuotasAdminRequest {
    pub(crate) const fn new(alterations: Vec<ClientQuotaAlteration>) -> Self {
        Self {
            alterations,
            validate_only: false,
        }
    }

    pub(crate) const fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(
            self.alterations
                .into_iter()
                .map(translate_alteration)
                .collect(),
            self.validate_only,
        )
    }
}

impl std::fmt::Debug for AlterClientQuotasAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AlterClientQuotasAdminRequest")
            .field("alterations", &self.alterations)
            .field("validate_only", &self.validate_only)
            .finish()
    }
}

fn translate_alteration(alteration: ClientQuotaAlteration) -> EngineEntry {
    let (entity, operations) = alteration.into_parts();
    EngineEntry::new(
        translate_entity(entity),
        operations.into_iter().map(translate_operation).collect(),
    )
}

fn translate_entity(entity: ClientQuotaEntity) -> EngineEntity {
    EngineEntity::new(
        entity
            .into_components()
            .into_iter()
            .map(translate_component)
            .collect(),
    )
}

fn translate_component(component: ClientQuotaEntityComponent) -> EngineEntityComponent {
    let (entity_type, entity_name) = component.into_parts();
    EngineEntityComponent::new(entity_type, entity_name)
}

fn translate_operation(operation: ClientQuotaAlterationOperation) -> EngineOperation {
    match operation {
        ClientQuotaAlterationOperation::Set { key, value } => EngineOperation::set(key, value),
        ClientQuotaAlterationOperation::Remove { key } => EngineOperation::remove(key),
    }
}
