//! Fallible startup acquisition for the bounded retained-record index.

use std::collections::TryReserveError;

use super::RecordStore;

impl RecordStore {
    pub(in crate::producer) fn try_new_with_topic_limits(
        records: usize,
        bytes: usize,
        topics: usize,
        topic_bytes: usize,
    ) -> Result<Self, TryReserveError> {
        let mut store = Self::new_with_topic_limits(records, bytes, topics, topic_bytes);
        store.slots.try_reserve(records)?;
        Ok(store)
    }
}
