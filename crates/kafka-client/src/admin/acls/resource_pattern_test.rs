//! Owned resource-pattern construction and validation tests.

use super::{AclPatternType, AclResourceType, ResourcePattern};

#[test]
fn pattern_owns_exact_resource_values() {
    let mut source = "orders.".to_owned();
    let pattern = ResourcePattern::new(
        AclResourceType::TOPIC,
        source.clone(),
        AclPatternType::PREFIXED,
    );
    source.clear();

    assert_eq!(pattern.resource_type(), AclResourceType::TOPIC);
    assert_eq!(pattern.name(), "orders.");
    assert_eq!(pattern.pattern_type(), AclPatternType::PREFIXED);
    assert!(pattern.is_valid_for_binding());
}

#[test]
fn concrete_pattern_rejects_empty_names_and_filter_only_codes() {
    assert!(
        !ResourcePattern::new(AclResourceType::TOPIC, "", AclPatternType::LITERAL)
            .is_valid_for_binding()
    );
    assert!(
        !ResourcePattern::new(AclResourceType::ANY, "orders", AclPatternType::LITERAL)
            .is_valid_for_binding()
    );
    assert!(
        !ResourcePattern::new(AclResourceType::TOPIC, "orders", AclPatternType::MATCH)
            .is_valid_for_binding()
    );
}
