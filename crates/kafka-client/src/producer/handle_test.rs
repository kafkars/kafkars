//! Public producer scenarios for exact immediate-admission rejection.

use bytes::Bytes;

use crate::{
    Client, DeliveryStatus, ErrorKind,
    record::{Header, Record, RecordParts},
};

#[test]
fn try_send_rejection_returns_exact_nullable_ordered_bytes_storage() {
    let client = build_client();
    let result = client.producer().build();
    let Ok(producer) = result else {
        panic!("producer construction should remain local")
    };
    let retained = Bytes::from(vec![9, 8, 7]);
    let record = Record::from_parts(RecordParts {
        topic: "orders".to_owned(),
        partition: None,
        timestamp_milliseconds: Some(42),
        key: None,
        value: Some(Bytes::new()),
        headers: vec![
            Header::from_parts("trace".to_owned(), None),
            Header::from_parts("trace".to_owned(), Some(Bytes::new())),
            Header::from_parts("trace".to_owned(), Some(retained.clone())),
        ],
    });

    let Err(rejection) = producer.try_send(record) else {
        panic!("automatic partitioning must remain unavailable")
    };
    assert_eq!(rejection.error().kind(), ErrorKind::InvalidRecord);
    assert_eq!(
        rejection.error().delivery_status(),
        Some(DeliveryStatus::NotSent)
    );
    let (returned, error) = rejection.into_parts();

    assert_eq!(error.kind(), ErrorKind::InvalidRecord);
    assert_eq!(returned.topic(), "orders");
    assert_eq!(returned.explicit_partition(), None);
    assert_eq!(returned.timestamp(), Some(42));
    assert_eq!(returned.key_bytes(), None);
    assert_eq!(returned.value_bytes(), Some(&Bytes::new()));
    assert_eq!(returned.headers()[0].value(), None);
    assert_eq!(returned.headers()[1].value(), Some(&Bytes::new()));
    assert_eq!(
        returned.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(retained.as_ptr())
    );
}

fn build_client() -> Client {
    let result = Client::builder().bootstrap_servers(["127.0.0.1:1"]).build();
    let Ok(client) = result else {
        panic!("valid local client configuration should build")
    };
    client
}
