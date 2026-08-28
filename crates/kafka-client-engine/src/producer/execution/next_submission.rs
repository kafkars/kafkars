//! Borrow-only selection of one bounded topic-routing candidate window.

use std::collections::HashSet;

#[cfg(test)]
use super::handoff::PreparedProduceSubmission;
use super::{
    PreparedEntry, PreparedExecution, PreparedProduceRouteCandidate, PreparedProduceRouteKey,
    PreparedProduceRouteWindow, handoff::PreparedProduceHandoffError,
};

impl PreparedExecution {
    /// Borrows the key of the next admission-order prepared submission.
    pub(crate) fn next_driver_route_key(&self) -> Option<PreparedProduceRouteKey> {
        self.entries
            .values()
            .find(|entry| entry.submission.is_some())
            .map(route_key)
    }

    /// Snapshots one bounded same-key candidate window without moving bytes.
    pub(crate) fn next_driver_route_window(
        &self,
        max_candidates: usize,
    ) -> Result<Option<PreparedProduceRouteWindow>, PreparedProduceHandoffError> {
        if max_candidates == 0 {
            return Ok(None);
        }
        let mut ready = self
            .entries
            .values()
            .filter(|entry| entry.submission.is_some());
        let Some(first) = ready.next() else {
            return Ok(None);
        };
        let key = route_key(first);
        let requested = self.submission_count().min(max_candidates);
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(requested)
            .map_err(|_| PreparedProduceHandoffError::GroupingCapacity { requested })?;
        let mut partitions = HashSet::new();
        partitions
            .try_reserve(requested)
            .map_err(|_| PreparedProduceHandoffError::GroupingCapacity { requested })?;
        let mut encoded_bytes = 0usize;

        for candidate in std::iter::once(first).chain(ready) {
            if candidates.len() == max_candidates {
                break;
            }
            if !entry_matches_key(candidate, &key)
                || !partitions.insert(candidate.materialized.partition())
            {
                break;
            }
            let Some(next_bytes) =
                encoded_bytes.checked_add(candidate.materialized.retained_record_bytes())
            else {
                break;
            };
            if next_bytes > self.max_request_bytes {
                break;
            }
            encoded_bytes = next_bytes;
            let submission = candidate
                .submission
                .unwrap_or_else(|| unreachable!("selected route candidate remains armed"));
            candidates.push(PreparedProduceRouteCandidate::new(
                candidate.execution,
                submission.operation_id,
                candidate.materialized.partition(),
            ));
        }
        if candidates.is_empty() {
            return Err(PreparedProduceHandoffError::RequestByteLimit {
                execution: first.execution,
                encoded_bytes: first.materialized.retained_record_bytes(),
                limit: self.max_request_bytes,
            });
        }
        Ok(Some(PreparedProduceRouteWindow::new(key, candidates)))
    }

    /// Transfers the next route window for focused legacy handoff tests.
    #[cfg(test)]
    pub(crate) fn take_next_driver_submissions(
        &mut self,
    ) -> Result<Vec<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        let Some(window) = self.next_driver_route_window(self.submission_count())? else {
            return Ok(Vec::new());
        };
        let (key, candidates) = window.into_parts();
        self.take_driver_submission_group(&key, &candidates)
    }

    /// Transfers the lowest armed `BatchId` for focused handoff tests.
    #[cfg(test)]
    pub(crate) fn take_next_driver_submission(
        &mut self,
    ) -> Result<Option<PreparedProduceSubmission>, PreparedProduceHandoffError> {
        let Some(execution) = self
            .entries
            .values()
            .find(|entry| entry.submission.is_some())
            .map(|entry| entry.execution)
        else {
            return Ok(None);
        };
        self.take_driver_submission(execution).map(Some)
    }
}

pub(super) fn entry_matches_key(entry: &PreparedEntry, key: &PreparedProduceRouteKey) -> bool {
    let Some(submission) = entry.submission else {
        return false;
    };
    let expected = entry.materialized.expected_topic_uuid();
    let replacement = entry.execution.generation().get() > 1 && expected.is_some();
    entry.materialized.topic_name_for_identity() == key.topic()
        && submission.deadline == key.deadline()
        && expected == key.expected_topic_uuid()
        && replacement == key.replacement()
        && (!replacement
            || entry.materialized.validated_topic_generation() == key.validated_generation())
}

fn route_key(entry: &PreparedEntry) -> PreparedProduceRouteKey {
    let submission = entry
        .submission
        .unwrap_or_else(|| unreachable!("route key requires one armed entry"));
    let expected_topic_uuid = entry.materialized.expected_topic_uuid();
    let replacement = entry.execution.generation().get() > 1 && expected_topic_uuid.is_some();
    PreparedProduceRouteKey::new(
        entry.materialized.topic_owner(),
        submission.deadline,
        expected_topic_uuid,
        replacement,
        if replacement {
            entry.materialized.validated_topic_generation()
        } else {
            None
        },
    )
}
