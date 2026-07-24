//! Lossless translation scenarios for engine `DescribeConfigs` terminals.

use core::num::NonZeroI16;

use kafka_client_core::{
    DescribeConfigBrokerError, DescribeConfigEntry as CoreEntry, DescribeConfigOutcome,
    DescribeConfigSynonym as CoreSynonym, DescribeConfigsBatch as CoreBatch,
    DescribeConfigsTerminal,
};

use super::{DescribeConfigsOutcome, translate::translate_terminal};

#[test]
fn throttle_versioned_fields_and_signed_resource_codes_cross_exactly() {
    let success = DescribeConfigOutcome::described(
        2,
        "orders",
        vec![CoreEntry::new(
            "cleanup.policy".to_owned(),
            Some("compact".to_owned()),
            true,
            -3,
            false,
            vec![CoreSynonym::new("default".to_owned(), None, 5)],
            Some(2),
            Some("docs".to_owned()),
        )],
    );
    let code = NonZeroI16::new(-32_123).unwrap_or_else(|| panic!("code is nonzero"));
    let failed = DescribeConfigOutcome::failed(
        2,
        "audit",
        DescribeConfigBrokerError::new(code, Some("future".to_owned()), false),
    );
    let DescribeConfigsOutcome::Configs(batch) = translate_terminal(
        DescribeConfigsTerminal::Configs(CoreBatch::new(77, vec![success, failed])),
    ) else {
        panic!("config batch expected");
    };
    assert_eq!(batch.throttle_time_ms(), 77);
    let mut resources = batch.into_resources().into_iter();
    let Some(success_resource) = resources.next() else {
        panic!("successful resource expected");
    };
    let (_, _, success) = success_resource.into_parts();
    let entries = success.unwrap_or_else(|error| panic!("configs expected: {error:?}"));
    let Some(entry) = entries.into_iter().next() else {
        panic!("configuration entry expected");
    };
    let (name, value, read_only, source, sensitive, synonyms, config_type, documentation) =
        entry.into_parts();
    assert_eq!(name, "cleanup.policy");
    assert_eq!(value.as_deref(), Some("compact"));
    assert!(read_only);
    assert_eq!(source, -3);
    assert!(!sensitive);
    assert_eq!(synonyms.len(), 1);
    assert_eq!(config_type, Some(2));
    assert_eq!(documentation.as_deref(), Some("docs"));

    let Some(failed_resource) = resources.next() else {
        panic!("failed resource expected");
    };
    let (_, _, failed) = failed_resource.into_parts();
    let error = failed
        .err()
        .unwrap_or_else(|| panic!("broker error expected"));
    let (code, message, truncated) = error.into_parts();
    assert_eq!(code, -32_123);
    assert_eq!(message.as_deref(), Some("future"));
    assert!(!truncated);
}
