//! Cross-admin fairness and shutdown-quiescence scenarios.

use kafka_client_core::Deadline;

use super::{
    create_partitions::CreatePartitionsProgress, create_topics::CreateTopicsProgress,
    delete_topics::DeleteTopicsProgress, describe_cluster::DescribeClusterProgress,
    describe_configs::DescribeConfigsProgress, describe_topics::DescribeTopicsProgress,
    incremental_alter_configs::IncrementalAlterConfigsProgress, schedule::combine,
};

#[test]
fn saturated_create_lane_cannot_hide_runnable_delete_work() {
    let combined = combine(
        &CreateTopicsProgress {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        },
        &DeleteTopicsProgress {
            unsettled: 1,
            driver_progress: true,
            next_deadline: Some(Deadline::from_tick(7)),
        },
        &DescribeClusterProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &CreatePartitionsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &idle_topics(),
        &idle_configs(),
        &idle_alter_configs(),
    );
    assert_eq!(combined.unsettled, usize::MAX);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(7)));
}

#[test]
fn either_concrete_owner_prevents_false_shutdown_quiescence() {
    for (create, delete) in [(1, 0), (0, 1)] {
        let combined = combine(
            &CreateTopicsProgress {
                unsettled: create,
                driver_progress: false,
                next_deadline: Some(Deadline::from_tick(9)),
            },
            &DeleteTopicsProgress {
                unsettled: delete,
                driver_progress: false,
                next_deadline: Some(Deadline::from_tick(5)),
            },
            &DescribeClusterProgress {
                unsettled: 0,
                driver_progress: false,
                next_deadline: Some(Deadline::from_tick(7)),
            },
            &CreatePartitionsProgress {
                unsettled: 0,
                driver_progress: false,
                next_deadline: None,
            },
            &idle_topics(),
            &idle_configs(),
            &idle_alter_configs(),
        );
        assert_ne!(combined.unsettled, 0);
        assert_eq!(combined.next_deadline, Some(Deadline::from_tick(5)));
    }
}

#[test]
fn describe_cluster_owner_prevents_false_shutdown_quiescence() {
    let combined = combine(
        &CreateTopicsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DeleteTopicsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DescribeClusterProgress {
            unsettled: 1,
            driver_progress: false,
            next_deadline: Some(Deadline::from_tick(7)),
        },
        &CreatePartitionsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &idle_topics(),
        &idle_configs(),
        &idle_alter_configs(),
    );
    assert_eq!(combined.unsettled, 1);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(7)));
}

#[test]
fn saturated_delete_lane_cannot_hide_runnable_describe_cluster_work() {
    let combined = combine(
        &CreateTopicsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DeleteTopicsProgress {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        },
        &DescribeClusterProgress {
            unsettled: 1,
            driver_progress: true,
            next_deadline: Some(Deadline::from_tick(4)),
        },
        &CreatePartitionsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &idle_topics(),
        &idle_configs(),
        &idle_alter_configs(),
    );
    assert_eq!(combined.unsettled, usize::MAX);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(4)));
}

#[test]
fn create_partitions_owner_is_independent_and_prevents_false_quiescence() {
    let combined = combine(
        &CreateTopicsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DeleteTopicsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DescribeClusterProgress {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        },
        &CreatePartitionsProgress {
            unsettled: 1,
            driver_progress: true,
            next_deadline: Some(Deadline::from_tick(3)),
        },
        &idle_topics(),
        &idle_configs(),
        &idle_alter_configs(),
    );
    assert_eq!(combined.unsettled, usize::MAX);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(3)));
}

#[test]
fn describe_topics_owner_is_independent_and_prevents_false_quiescence() {
    let combined = combine(
        &CreateTopicsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: Some(Deadline::from_tick(9)),
        },
        &DeleteTopicsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DescribeClusterProgress {
            unsettled: usize::MAX,
            driver_progress: false,
            next_deadline: None,
        },
        &CreatePartitionsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DescribeTopicsProgress {
            unsettled: 1,
            driver_progress: true,
            next_deadline: Some(Deadline::from_tick(2)),
        },
        &idle_configs(),
        &idle_alter_configs(),
    );
    assert_eq!(combined.unsettled, usize::MAX);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(2)));
}

#[test]
fn describe_configs_owner_is_independent_and_prevents_false_quiescence() {
    let combined = combine(
        &CreateTopicsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DeleteTopicsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &DescribeClusterProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &CreatePartitionsProgress {
            unsettled: 0,
            driver_progress: false,
            next_deadline: None,
        },
        &idle_topics(),
        &DescribeConfigsProgress {
            unsettled: 1,
            driver_progress: true,
            next_deadline: Some(Deadline::from_tick(2)),
        },
        &idle_alter_configs(),
    );
    assert_eq!(combined.unsettled, 1);
    assert!(combined.driver_progress);
    assert_eq!(combined.next_deadline, Some(Deadline::from_tick(2)));
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
