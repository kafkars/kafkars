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
    let resources = batch.into_resources();
    let (_, _, success) = resources[0].clone().into_parts();
    let entries = success.unwrap_or_else(|error| panic!("configs expected: {error:?}"));
    assert_eq!(entries[0].config_type(), Some(2));
    assert_eq!(entries[0].documentation(), Some("docs"));
    assert_eq!(entries[0].source(), -3);
    let (_, _, failed) = resources[1].clone().into_parts();
    let error = failed
        .err()
        .unwrap_or_else(|| panic!("broker error expected"));
    assert_eq!(error.code(), -32_123);
    assert_eq!(error.message(), Some("future"));
}
