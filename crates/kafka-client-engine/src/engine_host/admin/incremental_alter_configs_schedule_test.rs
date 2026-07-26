//! Fair scheduling and shutdown accounting for `IncrementalAlterConfigs`.

use kafka_client_core::Deadline;

use super::{
    alter_consumer_group_offsets::AlterConsumerGroupOffsetsProgress,
    create_partitions::CreatePartitionsProgress, create_topics::CreateTopicsProgress,
    delete_consumer_group_offsets::DeleteConsumerGroupOffsetsProgress,
    delete_topics::DeleteTopicsProgress, describe_cluster::DescribeClusterProgress,
    describe_configs::DescribeConfigsProgress, describe_topics::DescribeTopicsProgress,
    incremental_alter_configs::IncrementalAlterConfigsProgress,
    list_consumer_group_offsets::ListConsumerGroupOffsetsProgress, schedule::combine,
};

#[test]
fn incremental_alter_configs_owner_is_independent_and_prevents_false_quiescence() {
    let idle = 0;
    let combined = combine(
        &CreateTopicsProgress {
            unsettled: idle,
            driver_progress: false,
            next_deadline: None,
        },
        &DeleteTopicsProgress {
            unsettled: idle,
            driver_progress: false,
            next_deadline: None,
        },
        &DescribeClusterProgress {
            unsettled: idle,
            driver_progress: false,
            next_deadline: None,
        },
        &CreatePartitionsProgress {
            unsettled: idle,
            driver_progress: false,
            next_deadline: None,
        },
        &DescribeTopicsProgress {
            unsettled: idle,
            driver_progress: false,
            next_deadline: None,
        },
        &DescribeConfigsProgress {
            unsettled: idle,
            driver_progress: false,
            next_deadline: None,
        },
        &IncrementalAlterConfigsProgress {
            unsettled: 1,
            driver_progress: true,
            next_deadline: Some(Deadline::from_tick(1)),
        },
        &ListConsumerGroupOffsetsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DeleteConsumerGroupOffsetsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &AlterConsumerGroupOffsetsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
    );
    assert_eq!(combined.unsettled, 1);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(1)));
}
