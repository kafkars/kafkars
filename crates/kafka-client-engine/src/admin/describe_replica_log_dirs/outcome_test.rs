//! Stable engine outcome translation scenarios.

use kafka_client_core::{
    DescribeReplicaLogDirsBatch, DescribeReplicaLogDirsReplica,
    DescribeReplicaLogDirsReplicaOutcome, DescribeReplicaLogDirsTerminal,
    ReplicaLogDirInfo as CoreInfo, ReplicaLogDirLocation as CoreLocation,
};

use super::{
    DescribeReplicaLogDirsEngineReplicaResult, DescribeReplicaLogDirsOutcome,
    outcome::translate_terminal,
};

#[test]
fn translation_preserves_target_order_and_optional_locations() {
    let terminal = DescribeReplicaLogDirsTerminal::Described(DescribeReplicaLogDirsBatch::new(
        12,
        vec![DescribeReplicaLogDirsReplicaOutcome::described(
            DescribeReplicaLogDirsReplica::new("orders".to_owned(), 2, 7),
            CoreInfo::new(
                Some(CoreLocation::new("/logs/current".to_owned(), -1)),
                None,
            ),
        )],
    ));

    let DescribeReplicaLogDirsOutcome::Described(batch) = translate_terminal(terminal);
    let (throttle, outcomes) = batch.into_parts();
    assert_eq!(throttle, 12);
    let (target, result) = outcomes
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("one outcome"))
        .into_parts();
    assert_eq!(target.topic(), "orders");
    assert_eq!(target.partition(), 2);
    assert_eq!(target.broker_id(), 7);
    let DescribeReplicaLogDirsEngineReplicaResult::Described(info) = result else {
        panic!("expected described result");
    };
    let (current, future) = info.into_parts();
    assert_eq!(
        current
            .unwrap_or_else(|| panic!("current placement"))
            .into_parts(),
        ("/logs/current".to_owned(), -1)
    );
    assert!(future.is_none());
}
