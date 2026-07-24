//! Declarative boundary for concrete admin operation owners.

mod create_topics;

#[cfg(test)]
pub(super) use create_topics::AdminProgress;
pub(super) use create_topics::{apply_completions, drive};
