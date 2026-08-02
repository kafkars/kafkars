//! Evidence for exact deadline equality in one broker-aggregated Produce call.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;

use crate::clock::OperationDeadline;

use super::produce_call_batch::deadline_matches;

#[test]
fn aggregate_request_requires_the_same_core_and_transport_deadline() {
    let transport = Instant::now();
    let shared = deadline(40, transport);

    assert!(deadline_matches(shared, shared));
    assert!(!deadline_matches(shared, deadline(41, transport)));
    assert!(!deadline_matches(
        shared,
        deadline(
            40,
            transport
                .checked_add(Duration::from_nanos(1))
                .unwrap_or_else(|| panic!("test transport deadline")),
        ),
    ));
}

fn deadline(tick: u64, transport: Instant) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), transport)
}
