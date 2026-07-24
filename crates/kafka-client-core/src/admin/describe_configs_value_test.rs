//! Ownership-moving configuration value scenarios for execution adapters.

use super::{DescribeConfigEntry, DescribeConfigSynonym};

#[test]
fn normalized_values_move_into_adapter_parts_without_semantic_loss() {
    let synonym = DescribeConfigSynonym::new("default".to_owned(), Some("delete".to_owned()), -3);
    let entry = DescribeConfigEntry::new(
        "cleanup.policy".to_owned(),
        Some("compact".to_owned()),
        true,
        5,
        false,
        vec![synonym],
        Some(2),
        Some("cleanup docs".to_owned()),
    );
    let (name, value, read_only, source, sensitive, synonyms, config_type, documentation) =
        entry.into_parts();
    assert_eq!(name, "cleanup.policy");
    assert_eq!(value.as_deref(), Some("compact"));
    assert!(read_only);
    assert_eq!(source, 5);
    assert!(!sensitive);
    assert_eq!(config_type, Some(2));
    assert_eq!(documentation.as_deref(), Some("cleanup docs"));
    assert_eq!(
        synonyms
            .into_iter()
            .next()
            .map(DescribeConfigSynonym::into_parts),
        Some(("default".to_owned(), Some("delete".to_owned()), -3))
    );
}
