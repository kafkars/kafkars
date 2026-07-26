//! Fair reactor-native execution and ordered terminal shutdown.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{Deadline, Moment};

use crate::{
    admin::{
        AlterConsumerGroupOffsetsShardOwner, CreatePartitionsShardOwner, CreateTopicsShardOwner,
        DeleteConsumerGroupOffsetsShardOwner, DeleteTopicsShardOwner, DescribeClusterShardOwner,
        DescribeConfigsShardOwner, DescribeTopicsShardOwner, IncrementalAlterConfigsShardOwner,
        ListConsumerGroupOffsetsShardOwner,
    },
    clock::MonotonicClock,
    driver::{
        DescribeClusterCalls, DescribeConfigsCalls, DescribeTopicsCalls, DriverOwner, DriverTurn,
        IncrementalAlterConfigsCalls, TrackedCreatePartitionsCalls, TrackedCreateTopicsCalls,
        TrackedDeleteTopicsCalls, TrackedProduceCalls, TrackedProducerIdentityCalls,
    },
    producer::{
        host_turn::{ProducerTurnBudget, ProducerTurnOutcome},
        ingress::ProducerShardOwner,
    },
};

use super::{
    EngineHostControl, EngineHostError, admin, assigned_consumer, cleanup, group_consumer,
    notifier_shutdown::NotifierShutdownOwner, produce_turn,
};

// Admission and shutdown report wake failure without revoking ownership. Until
// that path can synchronously terminalize, this cap preserves deadline and
// shutdown liveness after an operating-system wake failure.
const HOST_PARK_LIMIT: Duration = Duration::from_millis(100);
const BLOCKED_RETRY_DELAY: Duration = HOST_PARK_LIMIT;
const SHUTDOWN_TURN_ATTEMPTS: usize = 64;

pub(crate) struct EngineHostResources {
    pub(super) driver: Option<DriverOwner>,
    pub(super) producer: ProducerShardOwner,
    pub(super) admin_notifier: crate::admin::AdminCompletionNotifier,
    pub(super) assigned_consumer_notifier: crate::consumer::AssignedConsumerCompletionNotifier,
    pub(super) create_topics: CreateTopicsShardOwner,
    pub(super) delete_topics: DeleteTopicsShardOwner,
    pub(super) describe_cluster: DescribeClusterShardOwner,
    pub(super) create_partitions: CreatePartitionsShardOwner,
    pub(super) describe_topics: DescribeTopicsShardOwner,
    pub(super) describe_configs: DescribeConfigsShardOwner,
    pub(super) incremental_alter_configs: IncrementalAlterConfigsShardOwner,
    pub(super) list_consumer_group_offsets: ListConsumerGroupOffsetsShardOwner,
    pub(super) delete_consumer_group_offsets: DeleteConsumerGroupOffsetsShardOwner,
    pub(super) alter_consumer_group_offsets: AlterConsumerGroupOffsetsShardOwner,
    pub(super) assigned_consumer: crate::consumer::AssignedConsumerShardOwner,
    pub(super) group_consumers: crate::consumer::GroupConsumerShardOwner,
    pub(super) clock: Arc<MonotonicClock>,
    pub(super) control: Arc<EngineHostControl>,
    pub(super) budget: ProducerTurnBudget,
    pub(super) produce_calls: TrackedProduceCalls,
    pub(super) producer_identity_calls: TrackedProducerIdentityCalls,
    pub(super) create_topics_calls: TrackedCreateTopicsCalls,
    pub(super) delete_topics_calls: TrackedDeleteTopicsCalls,
    pub(super) describe_cluster_calls: DescribeClusterCalls,
    pub(super) create_partitions_calls: TrackedCreatePartitionsCalls,
    pub(super) describe_topics_calls: DescribeTopicsCalls,
    pub(super) describe_configs_calls: DescribeConfigsCalls,
    pub(super) incremental_alter_configs_calls: IncrementalAlterConfigsCalls,
}

impl Drop for EngineHostResources {
    fn drop(&mut self) {
        super::admission_close::close_all(self);
    }
}

pub(crate) struct EngineHostExit {
    pub(super) notifier: NotifierShutdownOwner,
    pub(super) failure: Option<EngineHostError>,
}

pub(crate) fn run(resources: &mut EngineHostResources) -> Result<EngineHostExit, EngineHostError> {
    let mut driver_more_work = false;
    loop {
        #[cfg(test)]
        if resources.control.failure_requested() {
            return Err(EngineHostError::ForcedTestFailure);
        }
        let producer_now = resources.clock.now().map_err(EngineHostError::Clock)?;
        let producer = produce_turn::drive(resources, producer_now)?;
        // Producer execution may encode, submit, and apply completions before
        // admin receives its turn. Recapture time so admin operations never
        // compute broker timeouts from a stale pre-producer observation.
        let admin = admin::drive(resources)?;
        let assigned_now = resources.clock.now().map_err(EngineHostError::Clock)?;
        let assigned = assigned_consumer::drive(resources, assigned_now)?;
        let group_now = resources.clock.now().map_err(EngineHostError::Clock)?;
        let group = group_consumer::drive(resources, group_now)?;
        #[cfg(test)]
        if producer.driver_progress && resources.control.await_failure_after_produce_admission() {
            return Err(EngineHostError::ForcedTestFailure);
        }
        if resources.control.shutdown_requested()
            && producer.unsettled == 0
            && admin.unsettled == 0
            && assigned.unsettled == 0
            && assigned.close_completed
            && group.unsettled == 0
        {
            break;
        }
        let wait_now = resources.clock.now().map_err(EngineHostError::Clock)?;
        let wait = producer_wait(
            wait_now,
            producer.outcome,
            driver_more_work || producer.driver_progress || admin.driver_progress,
        );
        let wait = admin.next_deadline.map_or(wait, |deadline| {
            wait.min(duration_until(wait_now, deadline))
        });
        let wait = assigned_consumer_wait(wait_now, wait, &assigned);
        let wait = group_consumer_wait(wait_now, wait, &group);
        resources.control.record_driver_turn();
        let driver = resources
            .driver
            .as_mut()
            .ok_or(EngineHostError::DriverOwnerMissing)?;
        let driver_turn_more = match driver.turn(wait).map_err(EngineHostError::Driver)? {
            DriverTurn::Idle => false,
            DriverTurn::Progress { more_work } => more_work,
            DriverTurn::Shutdown => return Err(EngineHostError::DriverStopped),
        };
        let completion_now = resources.clock.now().map_err(EngineHostError::Clock)?;
        let completion_progress = produce_turn::apply_completions(
            &resources.producer,
            &mut resources.producer_identity_calls,
            &mut resources.produce_calls,
            completion_now,
        )?;
        let admin_completion_progress = admin::apply_completions(resources)?;
        driver_more_work = driver_turn_more || completion_progress || admin_completion_progress;
    }

    shutdown_driver(resources)?;
    cleanup::prepare_notification_stop(resources)?;
    let (notifier, failure) = cleanup::begin_notification_shutdown(resources)?;
    Ok(EngineHostExit {
        notifier: NotifierShutdownOwner::new(notifier),
        failure,
    })
}

pub(super) fn shutdown_driver(resources: &mut EngineHostResources) -> Result<(), EngineHostError> {
    let driver = resources
        .driver
        .as_mut()
        .ok_or(EngineHostError::DriverOwnerMissing)?;
    let turns = driver
        .shutdown_with_turn_limit(SHUTDOWN_TURN_ATTEMPTS, HOST_PARK_LIMIT)
        .map_err(EngineHostError::Driver)?;
    for _turn in 0..turns {
        resources.control.record_driver_turn();
    }
    Ok(())
}

pub(super) fn producer_wait(
    now: Moment,
    outcome: Option<ProducerTurnOutcome>,
    driver_more_work: bool,
) -> Duration {
    if driver_more_work {
        return Duration::ZERO;
    }
    let Some(outcome) = outcome else {
        return HOST_PARK_LIMIT;
    };
    if outcome.runnable_work {
        return Duration::ZERO;
    }
    let deadline_wait = outcome.next_deadline.map_or(HOST_PARK_LIMIT, |deadline| {
        duration_until(now, deadline).min(HOST_PARK_LIMIT)
    });
    if outcome.blocked_work {
        deadline_wait.min(BLOCKED_RETRY_DELAY)
    } else {
        deadline_wait
    }
}

pub(super) fn assigned_consumer_wait(
    now: Moment,
    current: Duration,
    progress: &assigned_consumer::AssignedConsumerProgress,
) -> Duration {
    if progress.progressed {
        return Duration::ZERO;
    }
    let deadline_wait = progress.next_deadline.map_or(HOST_PARK_LIMIT, |deadline| {
        duration_until(now, deadline).min(HOST_PARK_LIMIT)
    });
    let wait = current.min(deadline_wait);
    if progress.blocked_work {
        wait.min(BLOCKED_RETRY_DELAY)
    } else {
        wait
    }
}

pub(super) fn group_consumer_wait(
    now: Moment,
    current: Duration,
    progress: &group_consumer::GroupConsumerProgress,
) -> Duration {
    if progress.progressed {
        return Duration::ZERO;
    }
    let wait = progress.next_deadline.map_or(current, |deadline| {
        current.min(duration_until(now, deadline))
    });
    if progress.blocked_work {
        wait.min(BLOCKED_RETRY_DELAY)
    } else {
        wait
    }
}

fn duration_until(now: Moment, deadline: Deadline) -> Duration {
    Duration::from_nanos(deadline.tick().saturating_sub(now.tick()))
}
