//! Stable client-quota entity identity tests.

use super::ClientQuotaEntity;
use crate::admin::ClientQuotaEntityComponent;

#[test]
fn entity_components_have_one_canonical_public_view() {
    let entity = ClientQuotaEntity::new([
        ClientQuotaEntityComponent::new("user".to_owned(), Some("alice".to_owned())),
        ClientQuotaEntityComponent::new("client-id".to_owned(), Some("writer".to_owned())),
        ClientQuotaEntityComponent::new("client-id".to_owned(), None),
    ]);

    assert_eq!(entity.components()[0].entity_type(), "client-id");
    assert_eq!(entity.components()[0].entity_name(), None);
    assert_eq!(entity.components()[1].entity_name(), Some("writer"));
    assert_eq!(entity.components()[2].entity_type(), "user");
}

#[test]
fn construction_defers_empty_and_duplicate_component_validation() {
    let entity = ClientQuotaEntity::new([
        ClientQuotaEntityComponent::new(String::new(), None),
        ClientQuotaEntityComponent::new(String::new(), None),
    ]);

    assert_eq!(entity.into_components().len(), 2);
}
