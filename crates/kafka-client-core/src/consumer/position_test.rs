//! Per-partition next-fetch transition scenarios.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, StartPosition,
    assignment_test::{assign, assigned, offset},
};
use crate::Moment;

#[test]
fn resolved_start_and_fetch_progress_issue_exact_ordered_positions() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(&mut machine, vec![assigned(1, 0, StartPosition::Beginning)]);
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = initial.effects()[0] else {
        panic!("beginning should require resolution");
    };
    let resolved = machine
        .apply(AssignedConsumerInput::PositionResolved {
            fence,
            next_offset: offset(10),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("resolve beginning: {error}"));
    let AssignedConsumerEffect::FetchReady {
        fence: first_fetch,
        next_offset,
    } = resolved.effects()[0]
    else {
        panic!("resolved position should fetch");
    };
    assert_eq!(next_offset, offset(10));

    let advanced = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: first_fetch,
            next_offset: offset(14),
        })
        .unwrap_or_else(|error| panic!("advance completed fetch: {error}"));
    assert!(matches!(
        advanced.effects(),
        [AssignedConsumerEffect::FetchReady {
            fence,
            next_offset,
        }] if fence.position() == first_fetch.position()
            && fence.revision() > first_fetch.revision()
            && *next_offset == offset(14)
    ));
}

#[test]
fn offset_regression_rejects_without_consuming_fetch_revision() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let AssignedConsumerEffect::FetchReady {
        fence: first_fetch, ..
    } = initial.effects()[0]
    else {
        panic!("explicit offset should fetch");
    };
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchAdvanced {
            fence: first_fetch,
            next_offset: offset(9),
        }),
        Err(AssignedConsumerMachineError::OffsetRegression {
            requested: offset(10),
            observed: offset(9),
        })
    );

    let accepted = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: first_fetch,
            next_offset: offset(11),
        })
        .unwrap_or_else(|error| panic!("valid progress after rejection: {error}"));
    assert!(matches!(
        accepted.effects(),
        [AssignedConsumerEffect::FetchReady { fence, next_offset }]
            if fence.revision().get() == 2 && *next_offset == offset(11)
    ));
}

#[test]
fn older_fetch_revision_cannot_advance_the_active_execution() {
    let mut machine = AssignedConsumerMachine::new();
    let initial = assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(10)))],
    );
    let AssignedConsumerEffect::FetchReady {
        fence: first_fetch, ..
    } = initial.effects()[0]
    else {
        panic!("explicit offset should fetch");
    };
    let second = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: first_fetch,
            next_offset: offset(12),
        })
        .unwrap_or_else(|error| panic!("advance first fetch: {error}"));
    let AssignedConsumerEffect::FetchReady {
        fence: second_fetch,
        ..
    } = second.effects()[0]
    else {
        panic!("advancement should issue the next fetch");
    };

    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchAdvanced {
            fence: first_fetch,
            next_offset: offset(13),
        }),
        Err(AssignedConsumerMachineError::StaleFetch {
            supplied: first_fetch,
        })
    );
    let accepted = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: second_fetch,
            next_offset: offset(14),
        })
        .unwrap_or_else(|error| panic!("active fetch survives stale result: {error}"));
    assert!(matches!(
        accepted.effects(),
        [AssignedConsumerEffect::FetchReady { fence, next_offset }]
            if fence.revision().get() == 3 && *next_offset == offset(14)
    ));
}
