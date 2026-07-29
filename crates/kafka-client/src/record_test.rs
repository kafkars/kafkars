//! Public record construction evidence for nullable ordered duplicate headers.

use bytes::Bytes;

use super::{Header, Record};

#[test]
fn prebuilt_headers_preserve_null_empty_nonempty_storage_and_duplicate_order() {
    let empty = Bytes::new();
    let nonempty = Bytes::from_static(b"trace-value");
    let record = Record::to("orders")
        .with_header(Header::null("trace"))
        .with_header(Header::new("trace", empty.clone()))
        .with_header(Header::new("trace", nonempty.clone()));

    let headers = record.headers();
    assert_eq!(headers.len(), 3);
    assert_eq!(headers[0].name(), "trace");
    assert_eq!(headers[0].value(), None);
    assert_eq!(headers[1].name(), "trace");
    assert_eq!(headers[1].value(), Some(&empty));
    assert_eq!(headers[2].name(), "trace");
    assert_eq!(headers[2].value(), Some(&nonempty));
}

#[test]
fn header_is_nonnull_prebuilt_header_shorthand() {
    let value = Bytes::from_static(b"trace-value");
    let shorthand = Record::to("orders").header("trace", value.clone());
    let prebuilt = Record::to("orders").with_header(Header::new("trace", value));

    assert_eq!(shorthand, prebuilt);
}
