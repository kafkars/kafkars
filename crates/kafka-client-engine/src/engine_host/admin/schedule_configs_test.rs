//! Independent shutdown accounting for the concrete `DescribeConfigs` owner.

use kafka_client_core::{Deadline, Moment};

use super::{
    super::EngineHostError, alter_consumer_group_offsets::AlterConsumerGroupOffsetsProgress,
    create_partitions::CreatePartitionsProgress, create_topics::CreateTopicsProgress,
    delete_consumer_group_offsets::DeleteConsumerGroupOffsetsProgress,
    delete_topics::DeleteTopicsProgress, describe_cluster::DescribeClusterProgress,
    describe_configs::DescribeConfigsProgress, describe_topics::DescribeTopicsProgress,
    incremental_alter_configs::IncrementalAlterConfigsProgress,
    list_consumer_group_offsets::ListConsumerGroupOffsetsProgress, schedule::combine,
};

pub(super) fn drive_alter_configs_then_capture_group_offsets(
    alter_configs_now: Moment,
    drive_alter_configs: impl FnOnce(Moment) -> Result<IncrementalAlterConfigsProgress, EngineHostError>,
    capture_group_offsets_now: impl FnOnce() -> Result<Moment, EngineHostError>,
) -> Result<(IncrementalAlterConfigsProgress, Moment), EngineHostError> {
    let alter_configs = drive_alter_configs(alter_configs_now)?;
    let group_offsets_now = capture_group_offsets_now()?;
    Ok((alter_configs, group_offsets_now))
}

#[test]
fn describe_configs_owner_is_independent_and_prevents_false_quiescence() {
    let combined = combine(
        &create(),
        &delete(),
        &describe(),
        &partitions(),
        &topics(),
        &DescribeConfigsProgress {
            unsettled: 1,
            driver_progress: true,
            next_deadline: Some(Deadline::from_tick(2)),
        },
        &alter_configs(),
        &group_offsets(),
        &group_offset_delete(),
        &group_offset_alter(),
    );
    assert_eq!(combined.unsettled, 1);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(2)));
}

const fn create() -> CreateTopicsProgress {
    CreateTopicsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn delete() -> DeleteTopicsProgress {
    DeleteTopicsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn describe() -> DescribeClusterProgress {
    DescribeClusterProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn partitions() -> CreatePartitionsProgress {
    CreatePartitionsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn topics() -> DescribeTopicsProgress {
    DescribeTopicsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn alter_configs() -> IncrementalAlterConfigsProgress {
    IncrementalAlterConfigsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn group_offsets() -> ListConsumerGroupOffsetsProgress {
    ListConsumerGroupOffsetsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn group_offset_delete() -> DeleteConsumerGroupOffsetsProgress {
    DeleteConsumerGroupOffsetsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn group_offset_alter() -> AlterConsumerGroupOffsetsProgress {
    AlterConsumerGroupOffsetsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}
