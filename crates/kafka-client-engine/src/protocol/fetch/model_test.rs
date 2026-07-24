//! Engine record descriptors preserve byte-level Kafka value distinctions.

use bytes::Bytes;

use super::model::{FetchHeader, FetchRecord};

#[test]
fn null_empty_and_duplicate_headers_are_distinct_engine_values() {
    let record = FetchRecord {
        attributes: 0,
        offset: 4,
        timestamp: Some(7),
        key: None,
        value: Some(Bytes::new()),
        headers: vec![
            FetchHeader {
                key: Bytes::from_static(b"trace"),
                value: None,
            },
            FetchHeader {
                key: Bytes::from_static(b"trace"),
                value: Some(Bytes::new()),
            },
        ],
    };

    assert!(record.key.is_none());
    assert_eq!(record.value.as_deref(), Some(&b""[..]));
    assert_eq!(record.headers[0].key, record.headers[1].key);
    assert!(record.headers[0].value.is_none());
    assert_eq!(record.headers[1].value.as_deref(), Some(&b""[..]));
}
