//! Declarative sequencing boundary for concrete admin operation owners.

mod alter_consumer_group_offsets;
#[cfg(test)]
mod alter_consumer_group_offsets_schedule_test;
#[cfg(test)]
mod alter_consumer_group_offsets_test;
mod alter_partition_reassignments;
mod alter_replica_log_dirs;
mod create_partitions;
mod create_topics;
mod delete_consumer_group_offsets;
#[cfg(test)]
mod delete_consumer_group_offsets_schedule_test;
#[cfg(test)]
mod delete_consumer_group_offsets_test;
mod delete_records;
mod delete_topics;
mod describe_cluster;
mod describe_configs;
mod describe_log_dirs;
mod describe_topics;
mod group_offset_alter_schedule;
mod incremental_alter_configs;
#[cfg(test)]
mod incremental_alter_configs_schedule_test;
mod list_consumer_group_offsets;
#[cfg(test)]
mod list_consumer_group_offsets_test;
mod list_offsets;
mod list_offsets_schedule;
#[cfg(test)]
mod list_offsets_schedule_test;
mod list_partition_reassignments;
pub(super) mod recovery;
mod schedule;
#[cfg(test)]
mod schedule_configs_test;
mod schedule_deadline;
#[cfg(test)]
mod schedule_test;
#[cfg(test)]
mod schedule_time_test;

#[cfg(test)]
pub(super) use schedule::AdminProgress;
pub(super) use schedule::{apply_completions, drive};
