//! Scalar progress-report invariants for one bounded classic-group Fetch turn.

use super::turn_model::ClassicGroupFetchTurn;

#[test]
fn default_turn_reports_neither_progress_pressure_nor_fault() {
    let turn = ClassicGroupFetchTurn::default();

    assert!(!turn.progressed());
    assert!(!turn.blocked());
    assert!(!turn.fault_retained());
}

#[test]
fn every_execution_stage_independently_reports_progress() {
    let stages = [
        ClassicGroupFetchTurn {
            effect_interpreted: true,
            ..ClassicGroupFetchTurn::default()
        },
        ClassicGroupFetchTurn {
            timer_input_applied: true,
            ..ClassicGroupFetchTurn::default()
        },
        ClassicGroupFetchTurn {
            position_polled: true,
            ..ClassicGroupFetchTurn::default()
        },
        ClassicGroupFetchTurn {
            fetch_polled: true,
            ..ClassicGroupFetchTurn::default()
        },
        ClassicGroupFetchTurn {
            position_submitted: true,
            ..ClassicGroupFetchTurn::default()
        },
        ClassicGroupFetchTurn {
            fetch_submitted: true,
            ..ClassicGroupFetchTurn::default()
        },
    ];

    for stage in stages {
        assert!(stage.progressed());
        assert!(!stage.blocked());
        assert!(!stage.fault_retained());
    }
}

#[test]
fn pressure_and_retained_fault_remain_orthogonal_to_progress() {
    let turn = ClassicGroupFetchTurn {
        blocked: true,
        fault_retained: true,
        ..ClassicGroupFetchTurn::default()
    };

    assert!(!turn.progressed());
    assert!(turn.blocked());
    assert!(turn.fault_retained());
}
