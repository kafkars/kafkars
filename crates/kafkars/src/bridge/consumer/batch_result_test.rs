//! Exhaustive immediate batch-observation error translation.

use kafka_client_engine::AssignedConsumerTryTakeBatchErrorKind as Kind;

use super::batch_result::translate_assigned_batch_observation_kind;
use crate::ErrorKind;

#[test]
fn every_engine_batch_observation_kind_has_one_stable_facade_category() {
    let cases = [
        (Kind::Contended, ErrorKind::Backpressure),
        (Kind::Closed, ErrorKind::State),
        (Kind::Pending, ErrorKind::Backpressure),
        (Kind::HostUnavailable, ErrorKind::Internal),
        (Kind::InternalInvariant, ErrorKind::Internal),
    ];

    for (engine, facade) in cases {
        assert_eq!(
            translate_assigned_batch_observation_kind(engine).kind(),
            facade
        );
    }
}
