//! Provisional group-checkpoint scalar access contract.

use super::Checkpoint;

#[test]
fn checkpoint_reports_its_assignment_generation() {
    let checkpoint = Checkpoint {
        group_id: "workers".to_owned(),
        assignment_epoch: 7,
    };

    assert_eq!(checkpoint.assignment_epoch(), 7);
}
