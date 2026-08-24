//! Shared producer header-name validation and pointer-identity scenarios.

use bytes::Bytes;

use super::header::ProducerHeader;

#[test]
fn shared_name_is_validated_and_retained_without_copying() {
    let name = Bytes::from("trace".to_owned());
    let pointer = name.as_ptr();
    let header = ProducerHeader::try_from_shared_name(name, None)
        .unwrap_or_else(|error| panic!("valid shared name: {error}"));

    assert_eq!(header.name(), "trace");
    assert_eq!(header.shared_name_bytes().as_ptr(), pointer);
    assert!(ProducerHeader::try_from_shared_name(Bytes::from_static(b"\xff"), None).is_err());
}
