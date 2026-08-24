//! Empty-response and staged-lock retirement evidence.

use kafka_client_core::Moment;

use crate::protocol::consumer::share_fetch::ShareFetchSuccess;

use super::{
    ShareFetchSettlementTurn,
    settlement_test::{owner, stage},
};

#[test]
fn empty_success_cannot_invent_an_application_batch() {
    let mut owner = owner();
    stage(
        &mut owner,
        ShareFetchSuccess {
            throttle_time_ms: 7,
            acquisition_lock_timeout_ms: Some(30_000),
            topics: Vec::new(),
            endpoints: Vec::new(),
            retained_records: 0,
            retained_bytes: 0,
        },
    );
    assert!(matches!(
        owner.settle_terminal(Moment::from_tick(7)),
        Ok(ShareFetchSettlementTurn::Empty)
    ));
    assert!(
        owner
            .take_delivery(Moment::from_tick(8))
            .unwrap_or_else(|error| panic!("empty delivery observation: {error:?}"))
            .is_none()
    );
    assert!(!owner.has_staged_delivery());
    assert_eq!(
        owner.machine().phase(),
        kafka_client_core::ShareFetchSessionPhase::Ready
    );
    assert_eq!(owner.machine().fence().session_epoch().get(), 1);
}
