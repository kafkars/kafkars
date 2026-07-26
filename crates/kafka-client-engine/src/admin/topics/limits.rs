//! Sole count and retained-byte limits for the topic-description owner.

pub(crate) const DESCRIBE_TOPICS_CAPACITY: usize = 32;
pub(super) const DESCRIBE_TOPICS_RETAINED_BYTES: usize = 4 * 1024 * 1024;

pub(super) const fn all_topics_retained_charge() -> usize {
    DESCRIBE_TOPICS_RETAINED_BYTES
}
