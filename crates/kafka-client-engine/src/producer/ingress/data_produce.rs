//! Produce routing, handoff, and driver-accounting bridges under the shard lock.

use kafka_client_core::{Moment, partitioning::TopicMetadataGeneration};

use crate::{
    clock::OperationDeadline,
    producer::{
        ProducerHostInvariantError,
        execution::{
            PreparedProduceHandoffError, PreparedProduceRouteCandidate, PreparedProduceRouteKey,
            PreparedProduceRouteWindow, PreparedProduceSubmission,
        },
    },
};

use super::data::ProducerShardData;

impl ProducerShardData {
    /// Transfers at most one driver-ready request for focused host tests.
    #[cfg(test)]
    pub(crate) fn take_produce_submission(
        &mut self,
    ) -> Result<Option<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        self.host.execution.take_next_driver_submission()
    }

    /// Borrows the key of the next admission-order prepared submission.
    pub(crate) fn next_produce_route_key(&self) -> Option<PreparedProduceRouteKey> {
        self.host.execution.next_driver_route_key()
    }

    /// Snapshots a bounded route window without moving materialized bytes.
    pub(crate) fn next_produce_route_window(
        &self,
        max_candidates: usize,
    ) -> Result<Option<PreparedProduceRouteWindow>, PreparedProduceHandoffError> {
        self.host.execution.next_driver_route_window(max_candidates)
    }

    /// Atomically detaches one freshly selected exact-broker group.
    pub(crate) fn take_routed_produce_submissions(
        &mut self,
        key: &PreparedProduceRouteKey,
        candidates: &[PreparedProduceRouteCandidate],
    ) -> Result<Vec<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        self.host
            .execution
            .take_driver_submission_group(key, candidates)
    }

    /// Preflights persistent retry identity before any prepared owner detaches.
    pub(crate) fn preflight_produce_route_identity(
        &self,
        key: &PreparedProduceRouteKey,
        generation: TopicMetadataGeneration,
        candidates: &[PreparedProduceRouteCandidate],
    ) -> Result<bool, ProducerHostInvariantError> {
        let Some(expected) = key.replacement_topic_uuid() else {
            return Ok(true);
        };
        self.host
            .store
            .can_record_retry_topic_identity(
                candidates
                    .iter()
                    .map(PreparedProduceRouteCandidate::execution),
                expected,
                generation,
            )
            .map_err(ProducerHostInvariantError::Store)
    }

    /// Persists retry identity and mirrors it into the detached request owners.
    pub(crate) fn finalize_produce_route_identity(
        &mut self,
        key: &PreparedProduceRouteKey,
        generation: TopicMetadataGeneration,
        submissions: &mut [PreparedProduceSubmission],
    ) -> Result<bool, ProducerHostInvariantError> {
        let Some(expected) = key.replacement_topic_uuid() else {
            return Ok(true);
        };
        if !self
            .host
            .store
            .record_retry_topic_identity(
                submissions.iter().map(PreparedProduceSubmission::execution),
                expected,
                generation,
            )
            .map_err(ProducerHostInvariantError::Store)?
        {
            return Ok(false);
        }
        Ok(submissions
            .iter_mut()
            .all(|submission| submission.record_retry_topic_identity(expected, generation)))
    }

    /// Borrows the exact deadline of the next driver-ready Produce owner.
    pub(crate) fn next_produce_submission_deadline(&self) -> Option<OperationDeadline> {
        self.host.execution.next_submission_deadline()
    }

    /// Reports same-deadline preparation that can still join a ready submission.
    pub(crate) fn has_pending_produce_submission_at(&self, deadline: OperationDeadline) -> bool {
        self.host.has_pending_produce_submission_at(deadline)
    }

    /// Applies one transport-owned fact while this shard is locked.
    pub(crate) fn apply_produce_driver_input(
        &mut self,
        now: Moment,
        input: kafka_client_core::ProducerInput,
    ) -> Result<(), ProducerHostInvariantError> {
        self.host.apply_one_driver_input(now, input)
    }

    /// Records one driver-accepted Produce request and its exact member shape.
    pub(crate) fn record_produce_request(
        &mut self,
        batches: usize,
        records: u64,
        encoded_bytes: usize,
        in_flight_requests: usize,
        broker_in_flight_requests: usize,
    ) {
        self.host.produce_requests = self.host.produce_requests.saturating_add(1);
        self.host.produce_batches = self
            .host
            .produce_batches
            .saturating_add(u64::try_from(batches).unwrap_or(u64::MAX));
        self.host.produce_records = self.host.produce_records.saturating_add(records);
        self.host.produce_encoded_bytes = self
            .host
            .produce_encoded_bytes
            .saturating_add(u64::try_from(encoded_bytes).unwrap_or(u64::MAX));
        self.host.peak_produce_in_flight_requests = self
            .host
            .peak_produce_in_flight_requests
            .max(in_flight_requests);
        self.host.peak_produce_in_flight_requests_per_broker = self
            .host
            .peak_produce_in_flight_requests_per_broker
            .max(broker_in_flight_requests);
    }
}
