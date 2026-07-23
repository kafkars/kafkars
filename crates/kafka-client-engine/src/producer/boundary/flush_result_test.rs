//! Accepted producer flush result ownership scenarios.

use kafka_client_core::Moment;

use super::flush_result::ProducerTryFlushAccepted;
use crate::producer::{
    host_limits_test::{start, valid_limits},
    ingress::ProducerPortFlushAccepted,
};

#[test]
fn accepted_flush_transfers_one_runtime_neutral_observer() {
    let mut host = start(valid_limits());
    let admitted = host
        .try_admit_flush(Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("flush should be accepted: {error:?}"));
    let port = ProducerPortFlushAccepted::from_admitted_for_test(admitted);
    let accepted = ProducerTryFlushAccepted::from_port(port);

    assert!(accepted.fault().is_none());
    assert_eq!(accepted.into_observer().wait(), Ok(()));
}
