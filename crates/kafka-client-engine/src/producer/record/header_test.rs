//! Shared stored-header pointer identity and semantic ownership scenarios.

use bytes::Bytes;

use super::header::{ProducerHeader, ProducerSourceOwner};

#[test]
fn stored_header_reuses_shared_validated_name_bytes() {
    let name = Bytes::from("trace".to_owned());
    let pointer = name.as_ptr();
    let header = ProducerHeader::from_shared(name, None, ProducerSourceOwner::none());
    let materialized = header.materialization_view();
    let (materialized_name, value) = materialized.into_parts();

    assert_eq!(materialized_name.as_ptr(), pointer);
    assert_eq!(materialized_name.as_ref(), b"trace");
    assert_eq!(value, None);
}
