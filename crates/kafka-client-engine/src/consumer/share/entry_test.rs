//! Share-member identity and lossless entry-construction scenarios.

use std::sync::Arc;

use kafka_client_core::GroupId;

use super::{
    entry::{ShareConsumerEntry, ShareConsumerEntryBuildError},
    entry_identity::encode_member,
};

#[test]
fn generated_member_is_stable_kafka_uuid_spelling_for_the_entry_lifetime() {
    let fetch = crate::EngineShareConsumerFetchConfig::new(
        std::time::Duration::from_millis(250),
        2,
        4096,
        32,
        8,
        std::time::Duration::from_secs(9),
    );
    let entry = ShareConsumerEntry::try_new(
        group_id(),
        Arc::from("share-a"),
        Some(Arc::from("r1")),
        vec![Arc::from("orders")],
        fetch,
    )
    .unwrap_or_else(|failure| panic!("entry: {:?}", failure.kind));
    let member = entry.member();
    assert_eq!(member.len(), 22);
    assert!(
        member
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    );
    assert!(core::ptr::eq(entry.member().as_ref(), member.as_ref()));
    assert_eq!(entry.fetch_config(), fetch);
    assert_eq!(entry.fetch().config().max_records(), 32);
    assert_eq!(entry.fetch().config().batch_size(), 8);
}

#[test]
fn kafka_uuid_spelling_is_unpadded_url_safe_base64() {
    let encoded = encode_member([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
        .unwrap_or_else(|()| panic!("member spelling"));
    assert_eq!(encoded.as_ref(), "AAECAwQFBgcICQoLDA0ODw");
}

#[test]
fn invalid_entry_returns_the_exact_unconsumed_names() {
    let group: Arc<str> = Arc::from("share-a");
    let topic: Arc<str> = Arc::from("orders");
    let failure = ShareConsumerEntry::try_new(
        group_id(),
        Arc::clone(&group),
        None,
        vec![Arc::clone(&topic), Arc::clone(&topic)],
        crate::EngineShareConsumerFetchConfig::default(),
    )
    .err()
    .unwrap_or_else(|| panic!("duplicate topics must reject"));
    assert_eq!(failure.kind, ShareConsumerEntryBuildError::DuplicateTopic);
    assert!(Arc::ptr_eq(&failure.group, &group));
    assert!(Arc::ptr_eq(&failure.topics[0], &topic));
    assert!(Arc::ptr_eq(&failure.topics[1], &topic));
}

fn group_id() -> GroupId {
    GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id"))
}
