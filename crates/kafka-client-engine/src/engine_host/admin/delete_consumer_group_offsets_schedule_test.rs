//! Independent scheduling and fresh-time scenarios for offset deletion.

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
    schedule::{combine, drive_group_offsets_then_capture_delete},
};

#[test]
fn offset_deletion_is_independent_and_prevents_false_quiescence() {
    let deletion = DeleteConsumerGroupOffsetsProgress {
        unsettled: 1,
        driver_progress: true,
        next_deadline: Some(Deadline::from_tick(3)),
    };
    let combined = combine(
        &idle_create(),
        &idle_delete_topics(),
        &idle_describe(),
        &idle_partitions(),
        &idle_topics(),
        &idle_configs(),
        &idle_alter(),
        &idle_group_offsets(),
        &deletion,
        &idle_group_offset_alter(),
    );
    assert_eq!(combined.unsettled, 1);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(3)));
}

#[test]
fn offset_deletion_uses_time_recaptured_after_offset_listing() {
    let observed = Cell::new(Moment::from_tick(4_000_000));
    let result = drive_group_offsets_then_capture_delete(
        observed.get(),
        |now| {
            assert_eq!(now, Moment::from_tick(4_000_000));
            observed.set(Moment::from_tick(8_000_000));
            Ok(idle_group_offsets())
        },
        || Ok(observed.get()),
    );
    let Ok((_listing, deletion_now)) = result else {
        panic!("deterministic turn moments should remain representable");
    };
    assert_eq!(deletion_now, Moment::from_tick(8_000_000));
}

const fn idle_create() -> CreateTopicsProgress {
    CreateTopicsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_delete_topics() -> DeleteTopicsProgress {
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

const fn idle_alter() -> IncrementalAlterConfigsProgress {
    IncrementalAlterConfigsProgress {
        unsettled: 0,
        driver_progress: false,
        next_deadline: None,
    }
}

const fn idle_group_offsets() -> ListConsumerGroupOffsetsProgress {
    ListConsumerGroupOffsetsProgress {
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
