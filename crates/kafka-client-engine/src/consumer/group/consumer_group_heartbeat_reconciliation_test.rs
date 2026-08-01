//! KIP-848 assignment reconciliation through retirement and acknowledgement.

use kafka_client_core::Moment;

use crate::clock::MonotonicClock;

use super::{
    classic_group_position::ClassicGroupPositionExecutionState,
    consumer_group_assignment_retirement::{
        ConsumerGroupAssignmentRetirementTurn, retire_entry_assignment,
    },
    consumer_group_heartbeat_settlement::{ConsumerGroupHeartbeatSettlementTurn, settle_success},
    consumer_group_heartbeat_settlement_test::{
        installed_modern_entry, success_with, success_without_assignment,
    },
    consumer_group_heartbeat_submission::prepare_request,
};

#[test]
fn changed_member_epoch_retires_then_installs_the_new_assignment() {
    let (mut entry, topic_id) = installed_modern_entry();
    let clock = MonotonicClock::new();
    let schedule = entry
        .consumer
        .as_ref()
        .and_then(|consumer| consumer.machine().schedule())
        .unwrap_or_else(|| panic!("heartbeat schedule"));
    let now = Moment::from_tick(schedule.deadline().tick());
    entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .prepare_due_heartbeat(now, &clock)
        .unwrap_or_else(|error| panic!("prepare heartbeat: {error:?}"));
    assert_eq!(
        settle_success(&mut entry, now, success_with(2, 1)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    assert!(entry.consumer_revocation.is_some());
    assert!(entry.consumer_reconciliation.is_some());
    assert_eq!(
        entry
            .catalog
            .consumer_group_member_epoch()
            .map(|epoch| epoch.get()),
        Some(2)
    );
    assert_eq!(
        entry
            .catalog
            .live_assignment()
            .and_then(|assignment| assignment.partitions().first())
            .map(|partition| partition.partition().get()),
        Some(0)
    );

    assert_eq!(
        retire_entry_assignment(&mut entry, now, &clock),
        Ok(ConsumerGroupAssignmentRetirementTurn::Progress)
    );
    assert_eq!(
        retire_entry_assignment(&mut entry, now, &clock),
        Ok(ConsumerGroupAssignmentRetirementTurn::Progress)
    );
    assert!(entry.catalog.live_assignment().is_none());
    assert!(entry.consumer_reconciliation.is_some());
    let ack = entry
        .consumer
        .as_ref()
        .and_then(|execution| execution.prepared())
        .unwrap_or_else(|| panic!("empty-owned acknowledgement"));
    assert_eq!(ack.assignment_generation(), None);
    let request = prepare_request(&entry)
        .unwrap_or_else(|()| panic!("prepare empty-owned acknowledgement"))
        .into_generated_request();
    assert_eq!(request.member_epoch, 2);
    assert!(request.topic_partitions.as_ref().is_some_and(Vec::is_empty));
    assert_eq!(
        settle_success(&mut entry, now, success_without_assignment(2)),
        Ok(ConsumerGroupHeartbeatSettlementTurn::Progress)
    );
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("replacement assignment"));
    assert_eq!(assignment.assignment_generation().get(), 2);
    assert_eq!(assignment.partitions()[0].topic_id(), topic_id);
    assert_eq!(assignment.partitions()[0].partition().get(), 1);
    assert!(entry.consumer_reconciliation.is_none());
    let ClassicGroupPositionExecutionState::Prepared(position) = entry.position.state() else {
        panic!("replacement position resolution")
    };
    assert_eq!(position.key().operation_deadline(), ack.deadline());
    assert!(matches!(
        entry.catalog.take_event(),
        Some(crate::consumer::GroupConsumerEvent::PartitionsLost(_))
    ));
    assert!(matches!(
        entry.catalog.take_event(),
        Some(crate::consumer::GroupConsumerEvent::PartitionsAssigned(_))
    ));
}
