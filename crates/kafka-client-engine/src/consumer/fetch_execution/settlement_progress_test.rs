//! Empty-progress delivery authorization and lease evidence.

use kafka_client_core::{AssignedConsumerEffect, Moment};

use crate::protocol::fetch::fixture::encoded_empty_progress_batch_for_test;

use super::{
    DirectFetchExecutor,
    settlement_test::{
        OFFSET, OUTPUT_BYTES, TerminalFixture, assignment, fetch_fence, install, offset, prepared,
    },
};

#[test]
fn compacted_empty_progress_publishes_one_zero_byte_delivery() {
    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(
        &mut executor,
        prepared(effect),
        TerminalFixture::Success(Some(encoded_empty_progress_batch_for_test(OFFSET))),
    );

    let transition = executor
        .poll(&mut machine, Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("settle empty progress: {error:?}"))
        .unwrap_or_else(|| panic!("empty progress transition"));
    assert!(matches!(
        transition.effects(),
        [
            AssignedConsumerEffect::AuthorizeFetchDelivery {
                fence: authorized,
                next_offset,
            },
            AssignedConsumerEffect::FetchReady { .. },
        ] if *authorized == fence && *next_offset == offset(11)
    ));

    let delivery = executor
        .take_ready()
        .unwrap_or_else(|error| panic!("take empty progress: {error:?}"))
        .unwrap_or_else(|| panic!("empty progress delivery"));
    assert!(
        delivery
            .outcome()
            .outcome()
            .data_batches()
            .unwrap_or(&[])
            .is_empty()
    );
    assert_eq!(delivery.outcome().retained_bytes(), 0);
    executor
        .reclaim(delivery)
        .unwrap_or_else(|failure| panic!("reclaim progress: {:?}", failure.into_parts().0));
}
