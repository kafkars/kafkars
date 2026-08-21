//! Client-quota result ownership and canonical-order tests.

use std::time::Duration;

use super::{
    ClientQuotaEntityComponent, ClientQuotaEntry, ClientQuotaValue, DescribeClientQuotasResult,
};

#[test]
fn throttle_and_canonical_entry_order_remain_explicit() {
    let entries = vec![
        entry("client-id", "alpha", "request_percentage"),
        entry("user", "alice", "producer_byte_rate"),
    ];
    let result = DescribeClientQuotasResult::new(Duration::from_millis(7), entries);

    assert_eq!(result.throttle_time(), Duration::from_millis(7));
    assert_eq!(
        result.entries()[0].components()[0].entity_name(),
        Some("alpha")
    );
    assert_eq!(result.entries()[1].values()[0].key(), "producer_byte_rate");
    assert_eq!(result.into_entries().len(), 2);
}

fn entry(entity_type: &str, entity_name: &str, key: &str) -> ClientQuotaEntry {
    ClientQuotaEntry::new(
        vec![ClientQuotaEntityComponent::new(
            entity_type.to_owned(),
            Some(entity_name.to_owned()),
        )],
        vec![ClientQuotaValue::new(key.to_owned(), 1.0)],
    )
}
