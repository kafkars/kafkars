//! Stable configuration-value ownership scenarios.

use super::{ConfigEntry, ConfigSynonym};

#[test]
fn nullable_versioned_and_signed_configuration_facts_remain_exact() {
    let entry = ConfigEntry::new(
        String::from("cleanup.policy"),
        None,
        true,
        -7,
        true,
        vec![ConfigSynonym::new(
            String::from("default"),
            Some(String::from("delete")),
            5,
        )],
        Some(-3),
        Some(String::from("policy docs")),
    );
    assert_eq!(entry.name(), "cleanup.policy");
    assert_eq!(entry.value(), None);
    assert!(entry.read_only());
    assert_eq!(entry.source(), -7);
    assert!(entry.sensitive());
    assert_eq!(entry.config_type(), Some(-3));
    assert_eq!(entry.documentation(), Some("policy docs"));
    assert_eq!(entry.synonyms()[0].name(), "default");
    assert_eq!(entry.synonyms()[0].value(), Some("delete"));
    assert_eq!(entry.synonyms()[0].source(), 5);
}
