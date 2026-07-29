//! Stable StreamsGroup assignment tests.

use super::{StreamsGroupAssignment, StreamsGroupTaskIds};

#[test]
fn assignment_preserves_each_task_role() {
    let assignment = StreamsGroupAssignment::new(
        vec![StreamsGroupTaskIds::new("active".to_owned(), vec![0, 2])],
        vec![StreamsGroupTaskIds::new("standby".to_owned(), vec![1])],
        vec![StreamsGroupTaskIds::new("warmup".to_owned(), vec![3])],
    );

    assert_eq!(assignment.active_tasks()[0].partitions(), [0, 2]);
    assert_eq!(assignment.standby_tasks()[0].subtopology_id(), "standby");
    assert_eq!(assignment.warmup_tasks()[0].partitions(), [3]);
}
