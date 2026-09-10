//! Causal metadata polling, delayed retry submission, and deadline ownership.

use kafka_client_core::{Deadline, Moment};
use kafka_driver::{RouteFailureToken, RouteKind};

use crate::clock::OperationDeadline;

use super::super::{super::DriverOwner, topic_view::TopicPartitionCountCall};
use super::{
    CreateTopicsVisibility, CreateTopicsVisibilityPoll, DirectTopicPartitionCountCall,
    VisibilityCall,
    retry::{MAX_VISIBILITY_ATTEMPTS_PER_TOPIC, VisibilityRetry, visibility_failure_is_transient},
    target::CreateTopicVisibilityTarget,
};

impl CreateTopicsVisibility {
    pub(super) fn start(
        driver: &DriverOwner,
        token: Option<RouteFailureToken>,
        deadline: OperationDeadline,
        targets: Vec<CreateTopicVisibilityTarget>,
        now: Moment,
    ) -> Result<Self, ()> {
        if deadline.core().is_elapsed_at(now) {
            return Err(());
        }
        let token = token.ok_or(())?;
        if token.kind() != RouteKind::Controller {
            drop(token);
            return Err(());
        }
        let first = targets.first().ok_or(())?;
        let call = TopicPartitionCountCall::submit_after_outcome(
            driver,
            first.topic.clone(),
            token,
            deadline.transport(),
        )
        .map_err(|_error| ())?;
        Ok(Self {
            targets,
            current: 0,
            deadline,
            call: Some(VisibilityCall::Causal(call)),
            retry: None,
            attempts: 1,
        })
    }

    pub(super) fn poll(&mut self, driver: &DriverOwner, now: Moment) -> CreateTopicsVisibilityPoll {
        if self.deadline.core().is_elapsed_at(now) {
            return CreateTopicsVisibilityPoll::Failed;
        }
        if let Some(retry) = self.retry {
            if !retry.is_due(now) {
                return CreateTopicsVisibilityPoll::Pending;
            }
            return self.submit_retry(driver);
        }
        let Some(result) = self.call.as_mut().and_then(VisibilityCall::try_terminal) else {
            return CreateTopicsVisibilityPoll::Pending;
        };
        drop(self.call.take());
        let Some(target) = self.targets.get(self.current) else {
            return CreateTopicsVisibilityPoll::Failed;
        };
        match result {
            Ok(partition_count) if partition_count == target.expected_partition_count => {}
            Ok(_partition_count) => return self.schedule_retry(now),
            Err(failure) if visibility_failure_is_transient(failure) => {
                return self.schedule_retry(now);
            }
            Err(_failure) => return CreateTopicsVisibilityPoll::Failed,
        }
        self.current += 1;
        let Some(next) = self.targets.get(self.current) else {
            return CreateTopicsVisibilityPoll::Confirmed;
        };
        self.attempts = 1;
        match DirectTopicPartitionCountCall::submit(
            driver,
            next.topic.clone(),
            self.deadline.transport(),
        ) {
            Ok(call) => {
                self.call = Some(VisibilityCall::Direct(call));
                CreateTopicsVisibilityPoll::Progressed
            }
            Err(_error) => CreateTopicsVisibilityPoll::Failed,
        }
    }

    fn schedule_retry(&mut self, now: Moment) -> CreateTopicsVisibilityPoll {
        if self.attempts >= MAX_VISIBILITY_ATTEMPTS_PER_TOPIC {
            return CreateTopicsVisibilityPoll::Failed;
        }
        let Some(retry) = VisibilityRetry::schedule(now, self.deadline.core(), self.attempts)
        else {
            return CreateTopicsVisibilityPoll::Failed;
        };
        self.retry = Some(retry);
        CreateTopicsVisibilityPoll::Progressed
    }

    fn submit_retry(&mut self, driver: &DriverOwner) -> CreateTopicsVisibilityPoll {
        let Some(target) = self.targets.get(self.current) else {
            return CreateTopicsVisibilityPoll::Failed;
        };
        let submission = DirectTopicPartitionCountCall::submit(
            driver,
            target.topic.clone(),
            self.deadline.transport(),
        );
        match submission {
            Ok(call) => {
                self.call = Some(VisibilityCall::Direct(call));
                self.retry = None;
                self.attempts += 1;
                CreateTopicsVisibilityPoll::Progressed
            }
            Err(_error) => CreateTopicsVisibilityPoll::Failed,
        }
    }

    pub(super) fn next_deadline(&self) -> Deadline {
        self.retry.map_or(self.deadline.core(), |retry| {
            retry.not_before().min(self.deadline.core())
        })
    }
}
