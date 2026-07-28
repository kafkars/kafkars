//! Fallible normalized-to-core conversion and exact-error scenarios.

use kafka_client_core::{AlterReplicaLogDirResult, AlterReplicaLogDirsInput};

use crate::protocol::admin::alter_replica_log_dirs::{
    NormalizedAlterReplicaLogDirOutcome, NormalizedAlterReplicaLogDirsResponse,
};

use super::response::normalized_input;

#[test]
fn normalized_response_preserves_caller_order_and_exact_signed_codes() {
    let normalized = NormalizedAlterReplicaLogDirsResponse::fixture(
        2,
        0,
        vec![
            NormalizedAlterReplicaLogDirOutcome::fixture("zeta".to_owned(), 3, 0),
            NormalizedAlterReplicaLogDirOutcome::fixture("alpha".to_owned(), 1, -17),
        ],
        211,
    );

    let (input, retained_bytes) =
        normalized_input(7, normalized).unwrap_or_else(|()| panic!("convert"));
    assert!(retained_bytes > 0);
    let AlterReplicaLogDirsInput::BrokerResponded {
        throttle_time_ms,
        outcomes,
    } = input
    else {
        panic!("broker response expected");
    };
    assert_eq!(throttle_time_ms, 0);
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0].broker_id(), 7);
    assert_eq!(outcomes[0].topic(), "zeta");
    assert_eq!(outcomes[0].partition(), 3);
    assert_eq!(outcomes[0].result(), &AlterReplicaLogDirResult::Altered);
    let AlterReplicaLogDirResult::BrokerFailed(error) = outcomes[1].result() else {
        panic!("exact broker error expected");
    };
    assert_eq!(error.code(), -17);
}
