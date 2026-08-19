//! Explicit fair sequencing of concrete admin owners.

use std::sync::Arc;

use kafka_client_core::{Deadline, Moment};

use super::{
    super::{EngineHostError, EngineHostResources},
    abort_partition_transaction, add_raft_voter, alter_client_quotas, alter_consumer_group_offsets,
    alter_partition_reassignments, alter_replica_log_dirs, alter_share_group_offsets,
    alter_user_scram_credentials, create_acls, create_delegation_token, create_partitions,
    create_topics, delete_acls, delete_consumer_group_offsets, delete_consumer_groups,
    delete_records, delete_share_group_offsets, delete_topics, describe_acls,
    describe_client_quotas, describe_cluster, describe_configs, describe_consumer_groups,
    describe_delegation_tokens, describe_features, describe_log_dirs, describe_metadata_quorum,
    describe_producers, describe_replica_log_dirs, describe_share_group, describe_streams_group,
    describe_topic_partitions, describe_topics, describe_transactions,
    describe_user_scram_credentials, elect_leaders, expire_delegation_token, fence_producers,
    group_offset_alter_schedule::drive_group_offset_delete_then_capture_alter,
    incremental_alter_configs, legacy_alter_configs, list_client_metrics_resources,
    list_config_resources, list_consumer_group_offsets, list_consumer_groups, list_offsets,
    list_offsets_schedule, list_partition_reassignments, list_share_group_offsets,
    list_transactions, remove_consumer_group_members, remove_raft_voter, renew_delegation_token,
    schedule_broker::extend_with_broker_operations,
    schedule_configs::extend_with_legacy_alter_configs,
    schedule_deadline::earliest,
    unregister_broker, update_features,
};

pub(super) use super::schedule_configs::combine;

pub(in crate::engine_host) struct AdminProgress {
    pub(in crate::engine_host) unsettled: usize,
    pub(in crate::engine_host) driver_progress: bool,
    pub(in crate::engine_host) next_deadline: Option<Deadline>,
}

#[allow(
    clippy::too_many_lines,
    reason = "the fair scheduler names every concrete owner and clock capture in deterministic order"
)]
pub(in crate::engine_host) fn drive(
    resources: &mut EngineHostResources,
) -> Result<AdminProgress, EngineHostError> {
    // Contention in one concrete owner must not hide runnable work in another.
    let clock = Arc::clone(&resources.clock);
    let create_now = clock.now().map_err(EngineHostError::Clock)?;
    let (create, delete_now) = drive_create_then_capture_delete(
        create_now,
        |now| create_topics::drive(resources, now),
        || clock.now().map_err(EngineHostError::Clock),
    )?;
    let (delete, describe_now) = drive_delete_then_capture_describe(
        delete_now,
        |now| delete_topics::drive(resources, now),
        || clock.now().map_err(EngineHostError::Clock),
    )?;
    let (describe, topics_now) = drive_describe_then_capture_topics(
        describe_now,
        |now| describe_cluster::drive(resources, now),
        || clock.now().map_err(EngineHostError::Clock),
    )?;
    let topics = describe_topics::drive(resources, topics_now)?;
    let configs_now = clock.now().map_err(EngineHostError::Clock)?;
    let configs = describe_configs::drive(resources, configs_now)?;
    let partitions_now = clock.now().map_err(EngineHostError::Clock)?;
    let partitions = create_partitions::drive(resources, partitions_now)?;
    let alter_configs_now = clock.now().map_err(EngineHostError::Clock)?;
    let alter_configs = incremental_alter_configs::drive(resources, alter_configs_now)?;
    let legacy_alter_configs_now = clock.now().map_err(EngineHostError::Clock)?;
    let legacy_alter_configs = legacy_alter_configs::drive(resources, legacy_alter_configs_now)?;
    let group_offsets_now = clock.now().map_err(EngineHostError::Clock)?;
    let (group_offsets, group_offset_delete_now) = drive_group_offsets_then_capture_delete(
        group_offsets_now,
        |now| list_consumer_group_offsets::drive(resources, now),
        || clock.now().map_err(EngineHostError::Clock),
    )?;
    let (group_offset_delete, group_offset_alter_now) =
        drive_group_offset_delete_then_capture_alter(
            group_offset_delete_now,
            |now| delete_consumer_group_offsets::drive(resources, now),
            || clock.now().map_err(EngineHostError::Clock),
        )?;
    let group_offset_alter =
        alter_consumer_group_offsets::drive(resources, group_offset_alter_now)?;
    let delete_share_group_offsets_now = clock.now().map_err(EngineHostError::Clock)?;
    let share_group_offset_deletions =
        delete_share_group_offsets::drive(resources, delete_share_group_offsets_now)?;
    let list_share_group_offsets_now = clock.now().map_err(EngineHostError::Clock)?;
    let share_group_offset_listings =
        list_share_group_offsets::drive(resources, list_share_group_offsets_now)?;
    let alter_share_group_offsets_now = clock.now().map_err(EngineHostError::Clock)?;
    let share_group_offset_alterations =
        alter_share_group_offsets::drive(resources, alter_share_group_offsets_now)?;
    let describe_share_group_now = clock.now().map_err(EngineHostError::Clock)?;
    let share_group_descriptions =
        describe_share_group::drive(resources, describe_share_group_now)?;
    let describe_streams_group_now = clock.now().map_err(EngineHostError::Clock)?;
    let streams_group_descriptions =
        describe_streams_group::drive(resources, describe_streams_group_now)?;
    let list_offsets_now = clock.now().map_err(EngineHostError::Clock)?;
    let list_offsets_progress = list_offsets::drive(resources, list_offsets_now)?;
    let describe_producers_now = clock.now().map_err(EngineHostError::Clock)?;
    let producer_descriptions = describe_producers::drive(resources, describe_producers_now)?;
    let describe_topic_partitions_now = clock.now().map_err(EngineHostError::Clock)?;
    let topic_partition_descriptions =
        describe_topic_partitions::drive(resources, describe_topic_partitions_now)?;
    let list_reassignments_now = clock.now().map_err(EngineHostError::Clock)?;
    let list_reassignments =
        list_partition_reassignments::drive(resources, list_reassignments_now)?;
    let alter_reassignments_now = clock.now().map_err(EngineHostError::Clock)?;
    let alter_reassignments =
        alter_partition_reassignments::drive(resources, alter_reassignments_now)?;
    let elect_leaders_now = clock.now().map_err(EngineHostError::Clock)?;
    let elections = elect_leaders::drive(resources, elect_leaders_now)?;
    let delete_records_now = clock.now().map_err(EngineHostError::Clock)?;
    let record_deletions = delete_records::drive(resources, delete_records_now)?;
    let abort_partition_transaction_now = clock.now().map_err(EngineHostError::Clock)?;
    let partition_transaction_aborts =
        abort_partition_transaction::drive(resources, abort_partition_transaction_now)?;
    let describe_groups_now = clock.now().map_err(EngineHostError::Clock)?;
    let group_descriptions = describe_consumer_groups::drive(resources, describe_groups_now)?;
    let describe_transactions_now = clock.now().map_err(EngineHostError::Clock)?;
    let transaction_descriptions =
        describe_transactions::drive(resources, describe_transactions_now)?;
    let fence_producers_now = clock.now().map_err(EngineHostError::Clock)?;
    let producer_fencings = fence_producers::drive(resources, fence_producers_now)?;
    let list_transactions_now = clock.now().map_err(EngineHostError::Clock)?;
    let transaction_listings = list_transactions::drive(resources, list_transactions_now)?;
    let remove_members_now = clock.now().map_err(EngineHostError::Clock)?;
    let member_removals = remove_consumer_group_members::drive(resources, remove_members_now)?;
    let delete_groups_now = clock.now().map_err(EngineHostError::Clock)?;
    let group_deletions = delete_consumer_groups::drive(resources, delete_groups_now)?;
    let list_groups_now = clock.now().map_err(EngineHostError::Clock)?;
    let group_listings = list_consumer_groups::drive(resources, list_groups_now)?;
    let describe_log_dirs_now = clock.now().map_err(EngineHostError::Clock)?;
    let log_directories = describe_log_dirs::drive(resources, describe_log_dirs_now)?;
    let describe_replica_log_dirs_now = clock.now().map_err(EngineHostError::Clock)?;
    let replica_log_directories =
        describe_replica_log_dirs::drive(resources, describe_replica_log_dirs_now)?;
    let alter_log_dirs_now = clock.now().map_err(EngineHostError::Clock)?;
    let log_directory_alterations = alter_replica_log_dirs::drive(resources, alter_log_dirs_now)?;
    let describe_acls_now = clock.now().map_err(EngineHostError::Clock)?;
    let acl_descriptions = describe_acls::drive(resources, describe_acls_now)?;
    let describe_client_quotas_now = clock.now().map_err(EngineHostError::Clock)?;
    let quota_descriptions = describe_client_quotas::drive(resources, describe_client_quotas_now)?;
    let alter_client_quotas_now = clock.now().map_err(EngineHostError::Clock)?;
    let quota_alterations = alter_client_quotas::drive(resources, alter_client_quotas_now)?;
    let alter_user_scram_credentials_now = clock.now().map_err(EngineHostError::Clock)?;
    let scram_alterations =
        alter_user_scram_credentials::drive(resources, alter_user_scram_credentials_now)?;
    let describe_user_scram_credentials_now = clock.now().map_err(EngineHostError::Clock)?;
    let scram_descriptions =
        describe_user_scram_credentials::drive(resources, describe_user_scram_credentials_now)?;
    let describe_metadata_quorum_now = clock.now().map_err(EngineHostError::Clock)?;
    let metadata_quorum = describe_metadata_quorum::drive(resources, describe_metadata_quorum_now)?;
    let update_features_now = clock.now().map_err(EngineHostError::Clock)?;
    let feature_updates = update_features::drive(resources, update_features_now)?;
    let describe_features_now = clock.now().map_err(EngineHostError::Clock)?;
    let feature_descriptions = describe_features::drive(resources, describe_features_now)?;
    let unregister_broker_now = clock.now().map_err(EngineHostError::Clock)?;
    let broker_unregistrations = unregister_broker::drive(resources, unregister_broker_now)?;
    let add_raft_voter_now = clock.now().map_err(EngineHostError::Clock)?;
    let voter_additions = add_raft_voter::drive(resources, add_raft_voter_now)?;
    let remove_raft_voter_now = clock.now().map_err(EngineHostError::Clock)?;
    let voter_removals = remove_raft_voter::drive(resources, remove_raft_voter_now)?;
    let list_client_metrics_resources_now = clock.now().map_err(EngineHostError::Clock)?;
    let client_metrics_resources =
        list_client_metrics_resources::drive(resources, list_client_metrics_resources_now)?;
    let list_config_resources_now = clock.now().map_err(EngineHostError::Clock)?;
    let config_resources = list_config_resources::drive(resources, list_config_resources_now)?;
    let create_acls_now = clock.now().map_err(EngineHostError::Clock)?;
    let acl_creations = create_acls::drive(resources, create_acls_now)?;
    let delete_acls_now = clock.now().map_err(EngineHostError::Clock)?;
    let acl_deletions = delete_acls::drive(resources, delete_acls_now)?;
    let create_delegation_token_now = clock.now().map_err(EngineHostError::Clock)?;
    let delegation_token_creations =
        create_delegation_token::drive(resources, create_delegation_token_now)?;
    let describe_delegation_tokens_now = clock.now().map_err(EngineHostError::Clock)?;
    let delegation_token_descriptions =
        describe_delegation_tokens::drive(resources, describe_delegation_tokens_now)?;
    let renew_delegation_token_now = clock.now().map_err(EngineHostError::Clock)?;
    let delegation_token_renewals =
        renew_delegation_token::drive(resources, renew_delegation_token_now)?;
    let expire_delegation_token_now = clock.now().map_err(EngineHostError::Clock)?;
    let delegation_token_expirations =
        expire_delegation_token::drive(resources, expire_delegation_token_now)?;
    let progress = combine(
        &create,
        &delete,
        &describe,
        &partitions,
        &topics,
        &configs,
        &alter_configs,
        &group_offsets,
        &group_offset_delete,
        &group_offset_alter,
    );
    let config_progress = extend_with_legacy_alter_configs(&progress, &legacy_alter_configs);
    let mut partition_base = config_progress;
    list_offsets_schedule::extend(&mut partition_base, &list_offsets_progress);
    list_offsets_schedule::extend_partition_reassignments(&mut partition_base, &list_reassignments);
    list_offsets_schedule::extend_partition_reassignment_alterations(
        &mut partition_base,
        &alter_reassignments,
    );
    let partition_progress = extend_with_partition_operations(
        &partition_base,
        &producer_descriptions,
        &topic_partition_descriptions,
        &elections,
        &record_deletions,
        &partition_transaction_aborts,
    );
    let group_progress = extend_with_group_operations(
        &partition_progress,
        &group_descriptions,
        &transaction_descriptions,
        &producer_fencings,
        &transaction_listings,
        &member_removals,
        &group_deletions,
        &group_listings,
        &share_group_offset_deletions,
        &share_group_offset_listings,
        &share_group_offset_alterations,
        &share_group_descriptions,
        &streams_group_descriptions,
    );
    Ok(extend_with_broker_operations(
        &group_progress,
        &log_directories,
        &replica_log_directories,
        &log_directory_alterations,
        &acl_descriptions,
        &quota_descriptions,
        &quota_alterations,
        &scram_alterations,
        &scram_descriptions,
        &metadata_quorum,
        &feature_updates,
        &feature_descriptions,
        &broker_unregistrations,
        &voter_additions,
        &voter_removals,
        &client_metrics_resources,
        &config_resources,
        &acl_creations,
        &acl_deletions,
        &delegation_token_creations,
        &delegation_token_descriptions,
        &delegation_token_renewals,
        &delegation_token_expirations,
    ))
}

const fn extend_with_partition_operations(
    progress: &AdminProgress,
    producer_descriptions: &describe_producers::AdminDescribeProducersProgress,
    topic_partition_descriptions: &describe_topic_partitions::AdminDescribeTopicPartitionsProgress,
    elections: &elect_leaders::ElectLeadersProgress,
    record_deletions: &delete_records::DeleteRecordsProgress,
    partition_transaction_aborts: &abort_partition_transaction::AbortPartitionTransactionProgress,
) -> AdminProgress {
    AdminProgress {
        unsettled: progress
            .unsettled
            .saturating_add(producer_descriptions.unsettled)
            .saturating_add(topic_partition_descriptions.unsettled)
            .saturating_add(elections.unsettled)
            .saturating_add(record_deletions.unsettled)
            .saturating_add(partition_transaction_aborts.unsettled),
        driver_progress: progress.driver_progress
            || producer_descriptions.driver_progress
            || topic_partition_descriptions.driver_progress
            || elections.driver_progress
            || record_deletions.driver_progress
            || partition_transaction_aborts.driver_progress,
        next_deadline: earliest(
            progress.next_deadline,
            earliest(
                producer_descriptions.next_deadline,
                earliest(
                    topic_partition_descriptions.next_deadline,
                    earliest(
                        elections.next_deadline,
                        earliest(
                            record_deletions.next_deadline,
                            partition_transaction_aborts.next_deadline,
                        ),
                    ),
                ),
            ),
        ),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the sole group progress aggregation boundary keeps each concrete owner explicit"
)]
const fn extend_with_group_operations(
    progress: &AdminProgress,
    group_descriptions: &describe_consumer_groups::DescribeConsumerGroupsProgress,
    transaction_descriptions: &describe_transactions::AdminDescribeTransactionsProgress,
    producer_fencings: &fence_producers::AdminFenceProducersProgress,
    transaction_listings: &list_transactions::AdminListTransactionsProgress,
    member_removals: &remove_consumer_group_members::RemoveConsumerGroupMembersProgress,
    group_deletions: &delete_consumer_groups::DeleteConsumerGroupsProgress,
    group_listings: &list_consumer_groups::ListConsumerGroupsProgress,
    share_group_offset_deletions: &delete_share_group_offsets::DeleteShareGroupOffsetsProgress,
    share_group_offset_listings: &list_share_group_offsets::ListShareGroupOffsetsProgress,
    share_group_offset_alterations: &alter_share_group_offsets::AlterShareGroupOffsetsProgress,
    share_group_descriptions: &describe_share_group::DescribeShareGroupProgress,
    streams_group_descriptions: &describe_streams_group::DescribeStreamsGroupProgress,
) -> AdminProgress {
    AdminProgress {
        unsettled: progress
            .unsettled
            .saturating_add(group_descriptions.unsettled)
            .saturating_add(transaction_descriptions.unsettled)
            .saturating_add(producer_fencings.unsettled)
            .saturating_add(transaction_listings.unsettled)
            .saturating_add(member_removals.unsettled)
            .saturating_add(group_deletions.unsettled)
            .saturating_add(group_listings.unsettled)
            .saturating_add(share_group_offset_deletions.unsettled)
            .saturating_add(share_group_offset_listings.unsettled)
            .saturating_add(share_group_offset_alterations.unsettled)
            .saturating_add(share_group_descriptions.unsettled)
            .saturating_add(streams_group_descriptions.unsettled),
        driver_progress: progress.driver_progress
            || group_descriptions.driver_progress
            || transaction_descriptions.driver_progress
            || producer_fencings.driver_progress
            || transaction_listings.driver_progress
            || member_removals.driver_progress
            || group_deletions.driver_progress
            || group_listings.driver_progress
            || share_group_offset_deletions.driver_progress
            || share_group_offset_listings.driver_progress
            || share_group_offset_alterations.driver_progress
            || share_group_descriptions.driver_progress
            || streams_group_descriptions.driver_progress,
        next_deadline: earliest(
            progress.next_deadline,
            earliest(
                group_descriptions.next_deadline,
                earliest(
                    transaction_descriptions.next_deadline,
                    earliest(
                        producer_fencings.next_deadline,
                        earliest(
                            transaction_listings.next_deadline,
                            earliest(
                                member_removals.next_deadline,
                                earliest(
                                    group_deletions.next_deadline,
                                    earliest(
                                        group_listings.next_deadline,
                                        earliest(
                                            share_group_offset_deletions.next_deadline,
                                            earliest(
                                                share_group_offset_listings.next_deadline,
                                                earliest(
                                                    share_group_offset_alterations.next_deadline,
                                                    earliest(
                                                        share_group_descriptions.next_deadline,
                                                        streams_group_descriptions.next_deadline,
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    }
}

pub(super) fn drive_group_offsets_then_capture_delete(
    group_offsets_now: Moment,
    drive_group_offsets: impl FnOnce(
        Moment,
    ) -> Result<
        list_consumer_group_offsets::ListConsumerGroupOffsetsProgress,
        EngineHostError,
    >,
    capture_delete_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<
    (
        list_consumer_group_offsets::ListConsumerGroupOffsetsProgress,
        Moment,
    ),
    EngineHostError,
> {
    let group_offsets = drive_group_offsets(group_offsets_now)?;
    let delete_now = capture_delete_now()?;
    Ok((group_offsets, delete_now))
}

pub(super) fn drive_create_then_capture_delete(
    create_now: Moment,
    drive_create: impl FnOnce(Moment) -> Result<create_topics::CreateTopicsProgress, EngineHostError>,
    capture_delete_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<(create_topics::CreateTopicsProgress, Moment), EngineHostError> {
    let create = drive_create(create_now)?;
    let delete_now = capture_delete_now()?;
    Ok((create, delete_now))
}

pub(super) fn drive_delete_then_capture_describe(
    delete_now: Moment,
    drive_delete: impl FnOnce(Moment) -> Result<delete_topics::DeleteTopicsProgress, EngineHostError>,
    capture_describe_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<(delete_topics::DeleteTopicsProgress, Moment), EngineHostError> {
    let delete = drive_delete(delete_now)?;
    let describe_now = capture_describe_now()?;
    Ok((delete, describe_now))
}

pub(super) fn drive_describe_then_capture_topics(
    describe_now: Moment,
    drive_describe: impl FnOnce(
        Moment,
    )
        -> Result<describe_cluster::DescribeClusterProgress, EngineHostError>,
    capture_topics_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<(describe_cluster::DescribeClusterProgress, Moment), EngineHostError> {
    let describe = drive_describe(describe_now)?;
    let topics_now = capture_topics_now()?;
    Ok((describe, topics_now))
}

pub(in crate::engine_host) fn apply_completions(
    resources: &mut EngineHostResources,
) -> Result<bool, EngineHostError> {
    let create = create_topics::apply_completions(resources)?;
    let delete = delete_topics::apply_completions(resources)?;
    let describe = describe_cluster::apply_completions(resources)?;
    let partitions = create_partitions::apply_completions(resources)?;
    let topics = describe_topics::apply_completions(resources)?;
    let configs = describe_configs::apply_completions(resources)?;
    let alter_configs = incremental_alter_configs::apply_completions(resources)?;
    Ok(create || delete || describe || partitions || topics || configs || alter_configs)
}
