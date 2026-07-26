//! Scheduling scenarios for concrete consumer-group offset listing.

use std::cell::Cell;

use kafka_client_core::{Deadline, Moment};

use super::{
    alter_consumer_group_offsets::AlterConsumerGroupOffsetsProgress,
    create_partitions::CreatePartitionsProgress,
    create_topics::CreateTopicsProgress,
    delete_consumer_group_offsets::DeleteConsumerGroupOffsetsProgress,
    delete_topics::DeleteTopicsProgress,
    describe_cluster::DescribeClusterProgress,
    describe_configs::DescribeConfigsProgress,
    describe_topics::DescribeTopicsProgress,
    incremental_alter_configs::IncrementalAlterConfigsProgress,
    list_consumer_group_offsets::ListConsumerGroupOffsetsProgress,
    schedule::{combine, drive_alter_configs_then_capture_group_offsets},
};

#[test]
fn group_offsets_owner_is_independent_and_prevents_false_quiescence() {
    let combined = combine(
        &idle_create(),
        &idle_delete(),
        &idle_describe(),
        &idle_partitions(),
        &idle_topics(),
        &idle_configs(),
        &idle_alter_configs(),
        &ListConsumerGroupOffsetsProgress {
            unsettled: 1,
            driver_progress: true,
            next_deadline: Some(Deadline::from_tick(3)),
        },
        &idle_group_offset_delete(),
        &idle_group_offset_alter(),
    );
    assert_eq!(combined.unsettled, 1);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(3)));
}

#[test]
fn group_offsets_uses_time_recaptured_after_prior_admin_work() {
    let observed = Cell::new(Moment::from_tick(5));
    let progress = idle_alter_configs();
    let result = drive_alter_configs_then_capture_group_offsets(
        observed.get(),
        |now| {
            assert_eq!(now, Moment::from_tick(5));
            observed.set(Moment::from_tick(13));
            Ok(progress)
        },
        || Ok(observed.get()),
    );
    let Ok((_alter_configs, group_offsets_now)) = result else {
        panic!("deterministic turn moments should remain representable");
    };
    assert_eq!(group_offsets_now, Moment::from_tick(13));
}

const fn idle_create() -> CreateTopicsProgress {
    CreateTopicsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_delete() -> DeleteTopicsProgress {
    DeleteTopicsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_describe() -> DescribeClusterProgress {
    DescribeClusterProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_partitions() -> CreatePartitionsProgress {
    CreatePartitionsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_topics() -> DescribeTopicsProgress {
    DescribeTopicsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_configs() -> DescribeConfigsProgress {
    DescribeConfigsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_alter_configs() -> IncrementalAlterConfigsProgress {
    IncrementalAlterConfigsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_group_offset_delete() -> DeleteConsumerGroupOffsetsProgress {
    DeleteConsumerGroupOffsetsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_group_offset_alter() -> AlterConsumerGroupOffsetsProgress {
    AlterConsumerGroupOffsetsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}
