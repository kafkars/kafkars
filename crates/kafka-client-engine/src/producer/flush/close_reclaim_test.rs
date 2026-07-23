//! Producer close observer abandonment and exact shared-barrier reclamation.

use std::{
    thread,
    time::{Duration, Instant},
};

use kafka_client_core::Moment;

use super::super::{
    host_limits_test::{start, valid_limits},
    reclaim::CompletionReclaimOutcome,
    terminal_backlog::ProducerTerminalOwner,
};

#[test]
fn abandoned_close_reclaims_its_exact_flush_binding_and_shared_capacity() {
    let mut host = start(valid_limits());
    let close = host
        .try_admit_close(Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("empty close should be accepted: {error:?}"));
    let flush_id = close.flush_id();
    let completion_id = host
        .flush_bindings
        .completion(flush_id)
        .unwrap_or_else(|| panic!("accepted close must retain its completion binding"));
    drop(close);

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match host.reclaim_one(Moment::from_tick(0)) {
            Ok(Some(outcome)) => {
                assert_eq!(
                    outcome,
                    CompletionReclaimOutcome::Reclaimed {
                        owner: ProducerTerminalOwner::Flush(flush_id),
                        completion_id,
                    }
                );
                break;
            }
            Ok(None) => {
                assert!(Instant::now() < deadline, "close should become reclaimable");
                thread::yield_now();
            }
            Err(error) => panic!("close reclaim should succeed: {error}"),
        }
    }

    assert_eq!(host.stats().core_flush_slots, 0);
    assert_eq!(host.flush_bindings.completion(flush_id), None);
}
