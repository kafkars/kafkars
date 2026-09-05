//! Engine and catalog settlement evidence for recoverable KIP-848 member fencing.

use kafka_client_core::{
    AssignmentGeneration, ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatPhase,
    ConsumerGroupHeartbeatRequestKind, GroupAssignmentPartition, GroupId, LiveGroupAssignment,
    MemberId, Moment,
};

use crate::clock::MonotonicClock;
use crate::driver::ConsumerGroupHeartbeatRoute;

use super::{
    consumer_group_assignment_retirement::{
        ConsumerGroupAssignmentRetirementTurn, retire_entry_assignment,
        stage_consumer_group_revocation,
    },
    consumer_group_execution_fencing::consumer_group_heartbeat_is_ready,
    consumer_group_heartbeat_settlement_test::installed_modern_entry,
    consumer_group_heartbeat_submission::prepare_request,
    registry::GroupConsumerRegistry,
};

#[test]
fn missing_coordinator_route_preserves_steady_retry_ownership_and_expiry() {
    for expire in [false, true] {
        let (mut entry, _topic_id) = installed_modern_entry();
        let assignment = entry.catalog.live_assignment().map(assignment_facts);
        let execution = entry
            .consumer
            .as_mut()
            .unwrap_or_else(|| panic!("execution"));
        let due = execution
            .machine()
            .schedule()
            .unwrap_or_else(|| panic!("schedule"));
        let now = Moment::from_tick(due.deadline().tick());
        execution
            .prepare_due_heartbeat(now, &MonotonicClock::new())
            .unwrap_or_else(|error| panic!("steady heartbeat: {error:?}"));
        let rejected = execution.prepared().unwrap_or_else(|| panic!("prepared"));
        let mut registry =
            GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
        registry.entries.push(entry);
        registry
            .settle_consumer_group_rediscovery(
                0,
                if expire {
                    Moment::from_tick(rejected.deadline().core().tick())
                } else {
                    now
                },
                ConsumerGroupHeartbeatFailure::CoordinatorUnavailable,
                ConsumerGroupHeartbeatRoute::without_token_for_test(),
            )
            .unwrap_or_else(|error| panic!("route-less retry: {error:?}"));
        assert_eq!(
            registry
                .coordinator_invalidations
                .as_ref()
                .unwrap_or_else(|| panic!("invalidation registry"))
                .retained_count(),
            0
        );
        let entry = &mut registry.entries[0];
        let execution = entry
            .consumer
            .as_mut()
            .unwrap_or_else(|| panic!("execution"));
        if expire {
            assert!(execution.prepared().is_none());
            assert_eq!(
                execution.machine().phase(),
                ConsumerGroupHeartbeatPhase::Fatal
            );
            assert_eq!(
                execution.machine().fatal().map(|fatal| fatal.failure()),
                Some(ConsumerGroupHeartbeatFailure::DeadlineElapsed)
            );
            assert!(entry.consumer_revocation.is_some());
        } else {
            let replacement = execution
                .prepared()
                .unwrap_or_else(|| panic!("replacement"));
            let retry = execution
                .machine()
                .retry_schedule()
                .unwrap_or_else(|| panic!("retry"));
            assert_ne!(replacement.attempt(), rejected.attempt());
            assert_eq!(
                replacement.kind(),
                ConsumerGroupHeartbeatRequestKind::Steady
            );
            assert_eq!(replacement.deadline(), rejected.deadline());
            assert_eq!(replacement.member_id(), rejected.member_id());
            assert_eq!(replacement.member_epoch(), rejected.member_epoch());
            assert_eq!(
                replacement.assignment_generation(),
                rejected.assignment_generation()
            );
            assert!(retry.not_before().tick() > now.tick());
            assert!(retry.not_before() <= retry.deadline());
            assert!(!consumer_group_heartbeat_is_ready(entry));
            assert_eq!(
                entry.catalog.live_assignment().map(assignment_facts),
                assignment
            );
            entry
                .consumer
                .as_mut()
                .unwrap_or_else(|| panic!("execution"))
                .prepare_due_coordinator_load_retry(Moment::from_tick(retry.not_before().tick()))
                .unwrap_or_else(|error| panic!("retry due: {error:?}"));
            assert!(consumer_group_heartbeat_is_ready(entry));
        }
    }
}

fn assignment_facts(
    assignment: &LiveGroupAssignment,
) -> (
    GroupId,
    MemberId,
    AssignmentGeneration,
    Vec<GroupAssignmentPartition>,
) {
    (
        assignment.group_id(),
        assignment.member_id(),
        assignment.assignment_generation(),
        assignment.partitions().to_vec(),
    )
}

#[test]
fn fenced_unactivated_member_rejoins_without_a_false_loss_event() {
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
        assert!(entry.catalog.take_event().is_none());
        assert!(consumer_group_heartbeat_is_ready(&entry));

        let request = prepare_request(&entry)
            .unwrap_or_else(|()| panic!("materialize recovery Join"))
            .into_generated_request();
        assert_eq!(request.member_id.as_str(), member.as_ref());
        assert_eq!(request.member_epoch, 0);
        assert!(request.subscribed_topic_names.is_some());
        assert_eq!(request.topic_partitions.as_deref(), Some(&[][..]));
    }
}
