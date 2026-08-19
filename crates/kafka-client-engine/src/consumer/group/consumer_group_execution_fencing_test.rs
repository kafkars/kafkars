//! Engine and catalog settlement evidence for recoverable KIP-848 member fencing.

use kafka_client_core::{
    ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
    Moment,
};

use crate::clock::MonotonicClock;

use super::{
    consumer_group_assignment_retirement::{
        ConsumerGroupAssignmentRetirementTurn, retire_entry_assignment,
        stage_consumer_group_revocation,
    },
    consumer_group_heartbeat_settlement_test::installed_modern_entry,
    consumer_group_heartbeat_submission::{consumer_group_heartbeat_is_ready, prepare_request},
};

#[test]
fn fenced_member_is_lost_then_rejoins_at_epoch_zero_with_the_same_identity() {
    for error_code in [25, 110] {
        let (mut entry, _topic_id) = installed_modern_entry();
        let member_id = entry
            .catalog
            .current_member_id()
            .unwrap_or_else(|| panic!("installed member identity"));
        let member = entry
            .catalog
            .current_member()
            .cloned()
            .unwrap_or_else(|| panic!("installed member spelling"));
        let cycle = entry
            .consumer
            .as_ref()
            .and_then(super::consumer_group_execution::ConsumerGroupExecution::cycle)
            .unwrap_or_else(|| panic!("installed membership cycle"));
        let schedule = entry
            .consumer
            .as_ref()
            .and_then(|consumer| consumer.machine().schedule())
            .unwrap_or_else(|| panic!("heartbeat schedule"));
        let now = Moment::from_tick(schedule.deadline().tick());
        let clock = MonotonicClock::new();
        entry
            .consumer
            .as_mut()
            .unwrap_or_else(|| panic!("modern execution"))
            .prepare_due_heartbeat(now, &clock)
            .unwrap_or_else(|error| panic!("prepare heartbeat: {error:?}"));

        let revoked = entry
            .consumer
            .as_mut()
            .unwrap_or_else(|| panic!("modern execution"))
            .recover_current_fenced_membership(
                now,
                &clock,
                ConsumerGroupHeartbeatFailure::Broker(error_code),
            )
            .unwrap_or_else(|error| panic!("recover broker {error_code}: {error:?}"));
        stage_consumer_group_revocation(&mut entry, revoked)
            .unwrap_or_else(|error| panic!("stage fenced revocation: {error:?}"));
        let execution = entry
            .consumer
            .as_ref()
            .unwrap_or_else(|| panic!("modern execution"));
        let prepared = execution
            .prepared()
            .unwrap_or_else(|| panic!("prepared recovery Join"));
        assert_eq!(prepared.kind(), ConsumerGroupHeartbeatRequestKind::Join);
        assert_eq!(prepared.member_id(), Some(member_id));
        assert_eq!(prepared.member_epoch(), None);
        assert_eq!(prepared.assignment_generation(), None);
        assert_eq!(
            execution.machine().phase(),
            ConsumerGroupHeartbeatPhase::Joining
        );
        assert_eq!(
            execution.cycle(),
            cycle.checked_next(),
            "fenced recovery advances the local assignment fence once"
        );
        assert!(!consumer_group_heartbeat_is_ready(&entry));

        for _ in 0..2 {
            assert_eq!(
                retire_entry_assignment(&mut entry, now, &clock),
                Ok(ConsumerGroupAssignmentRetirementTurn::Progress)
            );
        }
        assert!(entry.consumer_revocation.is_none());
        assert!(entry.catalog.live_assignment().is_none());
        assert_eq!(entry.catalog.current_member_id(), Some(member_id));
        assert_eq!(entry.catalog.current_member(), Some(&member));
        assert_eq!(entry.catalog.consumer_group_member_epoch(), None);
        assert!(matches!(
            entry.catalog.take_event(),
            Some(crate::consumer::GroupConsumerEvent::PartitionsLost(_))
        ));
        assert!(consumer_group_heartbeat_is_ready(&entry));

        let request = prepare_request(&entry)
            .unwrap_or_else(|()| panic!("materialize recovery Join"))
            .into_generated_request();
        assert_eq!(request.member_id.as_str(), member.as_ref());
        assert_eq!(request.member_epoch, 0);
        assert!(request.subscribed_topic_names.is_some());
        assert!(request.topic_partitions.is_none());
    }
}
