//! Declarative sequencing boundary for concrete admin operation owners.

mod create_topics;
mod delete_topics;
mod schedule;
#[cfg(test)]
mod schedule_test;

#[cfg(test)]
pub(super) use schedule::AdminProgress;
pub(super) use schedule::{apply_completions, drive};
