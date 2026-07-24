//! Test-only interpretation of one successful broker identity fact.

use kafka_client_core::{Moment, ProducerInput};

use super::{ProducerHost, ingress::ProducerShardData};

pub(crate) fn acquire_host_if_pending(host: &mut ProducerHost, now: Moment) {
    let Some(submission) = host
        .take_identity_submission()
        .unwrap_or_else(|error| panic!("identity handoff should agree: {error}"))
    else {
        return;
    };
    let (generation, _deadline) = submission.into_parts();
    host.apply_one_driver_input(
        now,
        ProducerInput::ProducerIdentityAcquired {
            generation,
            producer_id: 1,
            producer_epoch: 0,
            now,
        },
    )
    .unwrap_or_else(|error| panic!("test identity should apply: {error}"));
}

pub(crate) fn acquire_shard_if_pending(data: &mut ProducerShardData, now: Moment) {
    let Some(submission) = data
        .take_identity_submission()
        .unwrap_or_else(|error| panic!("identity handoff should agree: {error}"))
    else {
        return;
    };
    let (generation, _deadline) = submission.into_parts();
    data.apply_produce_driver_input(
        now,
        ProducerInput::ProducerIdentityAcquired {
            generation,
            producer_id: 1,
            producer_epoch: 0,
            now,
        },
    )
    .unwrap_or_else(|error| panic!("test identity should apply: {error}"));
}
