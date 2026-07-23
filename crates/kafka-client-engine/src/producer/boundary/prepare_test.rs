//! Shared explicit-record preparation ownership and deadline scenarios.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;

use super::{
    ProducerSendCapture, ProducerSendOptions, ProducerTrySendErrorKind,
    PublicProducerRecord as ProducerRecord, prepare::prepare_explicit,
};
use crate::clock::MonotonicClock;

#[test]
fn preparation_preserves_the_one_captured_deadline_and_defaulted_record() {
    let clock = Arc::new(MonotonicClock::new());
    let capture =
        ProducerSendCapture::capture(&clock, ProducerSendOptions::new(Duration::from_secs(30)))
            .unwrap_or_else(|error| panic!("boundary capture should succeed: {error}"));
    let absolute = capture.absolute_deadline();
    let prepared = prepare_explicit(
        capture,
        ProducerRecord::to("orders")
            .partition(3)
            .value(Bytes::from_static(b"value")),
    )
    .unwrap_or_else(|error| panic!("valid record should prepare: {error}"));
    let (_attempted_at, deadline, stored) = prepared.into_parts();

    assert_eq!(deadline.transport(), absolute);
    assert_eq!(ProducerRecord::from_stored(stored).timestamp(), None);
}

#[test]
fn preparation_validation_returns_the_exact_public_record() {
    let clock = MonotonicClock::new();
    let capture =
        ProducerSendCapture::capture(&clock, ProducerSendOptions::new(Duration::from_secs(1)))
            .unwrap_or_else(|error| panic!("boundary capture should succeed: {error}"));
    let value = Bytes::from_static(b"same-allocation");
    let expected = value.clone();
    let result = prepare_explicit(capture, ProducerRecord::to("orders").value(value));
    let Err(error) = result else {
        panic!("missing explicit partition should reject")
    };

    assert_eq!(
        error.kind(),
        ProducerTrySendErrorKind::MissingExplicitPartition
    );
    assert_eq!(error.into_record().value_bytes(), Some(&expected));
}
