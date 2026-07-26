//! Declarative sequencing boundary for concrete admin operation owners.

mod create_partitions;
mod create_topics;
mod delete_consumer_group_offsets;
#[cfg(test)]
mod delete_consumer_group_offsets_schedule_test;
#[cfg(test)]
mod delete_consumer_group_offsets_test;
mod delete_topics;
mod describe_cluster;
mod describe_configs;
mod describe_topics;
mod incremental_alter_configs;
#[cfg(test)]
mod incremental_alter_configs_schedule_test;
mod list_consumer_group_offsets;
#[cfg(test)]
mod list_consumer_group_offsets_test;
pub(super) mod recovery;
mod schedule;
#[cfg(test)]
mod schedule_test;
#[cfg(test)]
mod schedule_time_test;

#[cfg(test)]
pub(super) use schedule::AdminProgress;
pub(super) use schedule::{apply_completions, drive};
