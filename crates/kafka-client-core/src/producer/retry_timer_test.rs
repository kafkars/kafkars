//! Retry-timer generation fencing before any producer mutation.

use crate::{BatchTimerGeneration, Moment, ProducerInput};

use super::scenario_support::retry::{submitted, transient_failure};

#[test]
fn stale_retry_timer_is_harmless_and_preserves_the_current_attempt() {
    let (mut producer, _operation_id, execution) = submitted(1, 2, 30);
    transient_failure(&mut producer, execution, 2);
    let before = format!("{producer:?}");

    let stale = producer
        .apply(ProducerInput::BatchTimerFired {
            batch_id: execution.batch_id(),
            generation: BatchTimerGeneration::from_raw(1),
            now: Moment::from_tick(4),
        })
        .unwrap_or_else(|error| panic!("stale retry timer failed: {error}"));

    assert!(stale.effects().is_empty());
    assert_eq!(format!("{producer:?}"), before);
}
