//! Stable client-quota entry ownership tests.

use super::{ClientQuotaEntityComponent, ClientQuotaEntry, ClientQuotaValue};

#[test]
fn default_entity_and_numeric_quota_facts_remain_lossless() {
    let entry = ClientQuotaEntry::new(
        vec![
            ClientQuotaEntityComponent::new("client-id".to_owned(), None),
            ClientQuotaEntityComponent::new("user".to_owned(), Some("alice".to_owned())),
        ],
        vec![ClientQuotaValue::new(
            "producer_byte_rate".to_owned(),
            12_500.5,
        )],
    );

    assert_eq!(entry.components()[0].entity_name(), None);
    assert_eq!(entry.components()[1].entity_name(), Some("alice"));
    assert_eq!(entry.values()[0].key(), "producer_byte_rate");
    assert_eq!(entry.values()[0].value(), 12_500.5);
}

#[test]
fn owned_parts_can_be_reclaimed_without_engine_values() {
    let component = ClientQuotaEntityComponent::new("ip".to_owned(), Some("127.0.0.1".to_owned()));
    let value = ClientQuotaValue::new("connection_creation_rate".to_owned(), 9.0);

    assert_eq!(
        component.into_parts(),
        ("ip".to_owned(), Some("127.0.0.1".to_owned()))
    );
    assert_eq!(
        value.into_parts(),
        ("connection_creation_rate".to_owned(), 9.0)
    );
}
