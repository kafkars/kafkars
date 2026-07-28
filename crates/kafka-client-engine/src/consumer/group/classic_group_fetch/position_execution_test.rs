//! Compile-time bounded position-stage contract.

use super::position_execution::ClassicGroupPositionStage;

#[test]
fn position_stage_separates_pressure_from_terminal_fault() {
    assert_ne!(
        ClassicGroupPositionStage::Backpressured,
        ClassicGroupPositionStage::Faulted
    );
    assert_ne!(
        ClassicGroupPositionStage::Idle,
        ClassicGroupPositionStage::Progressed
    );
}
