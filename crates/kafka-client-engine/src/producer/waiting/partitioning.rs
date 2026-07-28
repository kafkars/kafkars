//! Linear metadata lookup ownership for the FIFO waiting head.

use std::sync::Arc;

use kafka_client_core::{
    ProducerWaiterId, ProducerWaitingTerminal, TopicId,
    partitioning::{TopicPartitionFacts, TopicPartitionSource, select_java_keyed_topic_partition},
};

use crate::clock::OperationDeadline;

use crate::producer::{ProducerHost, ProducerHostInvariantError};

/// Exact waiting identity transferred to one driver topic-view lookup.
#[derive(Debug)]
pub(crate) struct ProducerPartitioningRequest {
    waiter_id: ProducerWaiterId,
    topic_id: TopicId,
    topic: Arc<str>,
    deadline: OperationDeadline,
}

impl ProducerPartitioningRequest {
    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) const fn deadline(&self) -> OperationDeadline {
        self.deadline
    }
}

/// Metadata terminal normalized before producer waiting settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerPartitioningFailure {
    DeadlineElapsed,
    MetadataUnavailable { broker_code: Option<i16> },
}

impl ProducerHost {
    pub(crate) fn take_partitioning_request(
        &mut self,
    ) -> Result<Option<ProducerPartitioningRequest>, ProducerHostInvariantError> {
        let Some(id) = self
            .waiting_policy
            .front()
            .map(kafka_client_core::ProducerWaiter::id)
        else {
            return Ok(None);
        };
        let Some(entry) = self.waiting.entries.front() else {
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        };
        if entry.id != id {
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        }
        if !entry.record.needs_partition() || entry.partitioning {
            return Ok(None);
        }
        let operation_id = entry.operation_id;
        let topic_id = entry.topic_id;
        let topic = Arc::clone(entry.record.topic());
        let Some(deadline) = self.bindings.deadline(operation_id) else {
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        };
        self.waiting.entries[0].partitioning = true;
        Ok(Some(ProducerPartitioningRequest {
            waiter_id: id,
            topic_id,
            topic,
            deadline,
        }))
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "restoration consumes the exact linear metadata request and prevents reuse"
    )]
    pub(crate) fn restore_partitioning_request(
        &mut self,
        request: ProducerPartitioningRequest,
    ) -> Result<(), ProducerHostInvariantError> {
        let Some(index) = self
            .waiting
            .entries
            .iter()
            .position(|entry| entry.id == request.waiter_id)
        else {
            return Ok(());
        };
        self.correlate_partitioning(index, &request)?;
        self.waiting.entries[index].partitioning = false;
        Ok(())
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "settlement consumes the exact linear metadata request and prevents reuse"
    )]
    pub(crate) fn apply_partitioning_view(
        &mut self,
        request: ProducerPartitioningRequest,
        source: &dyn TopicPartitionSource,
    ) -> Result<bool, ProducerHostInvariantError> {
        let Some(index) = self
            .waiting
            .entries
            .iter()
            .position(|entry| entry.id == request.waiter_id)
        else {
            return Ok(false);
        };
        self.correlate_partitioning(index, &request)?;
        let facts = TopicPartitionFacts::new(source);
        let key = self.waiting.entries[index].record.key_bytes().cloned();
        let selection = match key {
            Some(key) => select_java_keyed_topic_partition(&key, facts).map_err(|_| ()),
            None => self
                .store
                .select_sticky_partition(request.topic_id, facts)
                .map_err(|error| self.poison(ProducerHostInvariantError::Store(error)))?
                .map_err(|_| ()),
        };
        let Ok(selection) = selection else {
            self.settle_waiter(
                request.waiter_id,
                ProducerWaitingTerminal::MetadataUnavailable { broker_code: None },
            )?;
            return Ok(true);
        };
        let entry = &mut self.waiting.entries[index];
        if !entry.record.assign_partition(selection.partition()) {
            return Err(self.poison(ProducerHostInvariantError::WaitingOwnership));
        }
        entry.partitioning = false;
        Ok(true)
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "settlement consumes the exact linear metadata request and prevents reuse"
    )]
    pub(crate) fn apply_partitioning_failure(
        &mut self,
        request: ProducerPartitioningRequest,
        failure: ProducerPartitioningFailure,
    ) -> Result<bool, ProducerHostInvariantError> {
        let Some(index) = self
            .waiting
            .entries
            .iter()
            .position(|entry| entry.id == request.waiter_id)
        else {
            return Ok(false);
        };
        self.correlate_partitioning(index, &request)?;
        let terminal = match failure {
            ProducerPartitioningFailure::DeadlineElapsed => {
                ProducerWaitingTerminal::DeadlineElapsed
            }
            ProducerPartitioningFailure::MetadataUnavailable { broker_code } => {
                ProducerWaitingTerminal::MetadataUnavailable { broker_code }
            }
        };
        self.settle_waiter(request.waiter_id, terminal)
    }

    fn correlate_partitioning(
        &mut self,
        index: usize,
        request: &ProducerPartitioningRequest,
    ) -> Result<(), ProducerHostInvariantError> {
        let entry = &self.waiting.entries[index];
        let correlated = entry.id == request.waiter_id
            && entry.topic_id == request.topic_id
            && entry.record.topic().as_ref() == request.topic.as_ref()
            && entry.partitioning
            && entry.record.needs_partition()
            && self.bindings.deadline(entry.operation_id) == Some(request.deadline);
        if correlated {
            Ok(())
        } else {
            Err(self.poison(ProducerHostInvariantError::WaitingOwnership))
        }
    }
}

impl super::model::ProducerWaitingStore {
    pub(super) fn front_needs_partition(
        &self,
        id: ProducerWaiterId,
    ) -> Result<bool, ProducerHostInvariantError> {
        let Some(entry) = self.entries.front() else {
            return Err(ProducerHostInvariantError::WaitingOwnership);
        };
        if entry.id != id {
            return Err(ProducerHostInvariantError::WaitingOwnership);
        }
        Ok(entry.record.needs_partition())
    }
}
