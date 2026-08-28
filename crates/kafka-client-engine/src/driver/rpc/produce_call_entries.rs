//! O(1) cursor ownership for exact executions in one tracked Produce call.

use std::sync::Arc;

use kafka_client_core::{BatchExecutionId, Deadline};

#[derive(Debug)]
pub(super) struct TrackedProduceEntry {
    pub(super) execution: BatchExecutionId,
    pub(super) deadline: Deadline,
    pub(super) topic: Arc<str>,
    pub(super) partition: i32,
}

#[derive(Debug)]
pub(super) enum TrackedProduceEntries {
    Single(TrackedProduceEntry),
    Batch {
        entries: Vec<TrackedProduceEntry>,
        next: usize,
    },
}

impl TrackedProduceEntries {
    pub(super) fn batch(entries: Vec<TrackedProduceEntry>) -> Self {
        debug_assert!(!entries.is_empty());
        Self::Batch { entries, next: 0 }
    }

    pub(super) fn first(&self) -> &TrackedProduceEntry {
        match self {
            Self::Single(entry) => entry,
            Self::Batch { entries, next } => entries
                .get(*next)
                .unwrap_or_else(|| unreachable!("tracked Produce batch is nonempty")),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Batch { entries, next } => entries.len().saturating_sub(*next),
        }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &TrackedProduceEntry> {
        match self {
            Self::Single(entry) => EitherEntries::Single(std::iter::once(entry)),
            Self::Batch { entries, next } => EitherEntries::Batch(entries[*next..].iter()),
        }
    }

    pub(super) fn advance(&mut self) -> bool {
        match self {
            Self::Single(_) => false,
            Self::Batch { entries, next } => {
                if next.saturating_add(1) < entries.len() {
                    *next = next.saturating_add(1);
                    true
                } else {
                    false
                }
            }
        }
    }
}

enum EitherEntries<'a> {
    Single(std::iter::Once<&'a TrackedProduceEntry>),
    Batch(std::slice::Iter<'a, TrackedProduceEntry>),
}

impl<'a> Iterator for EitherEntries<'a> {
    type Item = &'a TrackedProduceEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(iter) => iter.next(),
            Self::Batch(iter) => iter.next(),
        }
    }
}
