//! Direct-consumer close fencing, ordering, and atomic preflight scenarios.

use super::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerMachineError, FetchOwnership, FetchRecords, PositionEpoch, StartPosition,
    assignment_test::{assign, assigned, offset, partition},
};
use crate::{Deadline, Moment};

#[test]
fn close_accepts_then_suspends_and_revokes_in_assignment_order() {
    let mut machine = AssignedConsumerMachine::new();
    let assignment = assign(
        &mut machine,
        vec![
            assigned(2, 3, StartPosition::Offset(offset(10))),
            assigned(1, 4, StartPosition::Offset(offset(20))),
        ],
    );
    let [
        AssignedConsumerEffect::FetchReady {
            fence: first_fetch, ..
        },
        AssignedConsumerEffect::FetchReady {
            fence: second_fetch,
            ..
        },
    ] = assignment.effects()
    else {
        panic!("explicit positions must start Fetch");
    };
    let epoch = assignment
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assigned transition epoch"));
    let paused = machine
        .apply(AssignedConsumerInput::Pause {
            assignment_epoch: epoch,
            partition: partition(2, 3),
        })
        .unwrap_or_else(|error| panic!("pause before close: {error}"));
    let AssignedConsumerEffect::Suspend {
        fence: paused_fence,
    } = paused.effects()[0]
    else {
        panic!("pause must publish its fence");
    };

    let closed = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("begin close: {error}"));

    assert_eq!(closed.assignment_epoch(), Some(epoch));
    let [
        AssignedConsumerEffect::AcceptClose { close_id },
        AssignedConsumerEffect::Suspend { fence: first },
        AssignedConsumerEffect::Revoke {
            assignment_epoch: first_epoch,
            partition: first_partition,
        },
        AssignedConsumerEffect::Suspend { fence: second },
        AssignedConsumerEffect::Revoke {
            assignment_epoch: second_epoch,
            partition: second_partition,
        },
    ] = closed.effects()
    else {
        panic!("close must accept then suspend and revoke in assignment order");
    };
    assert_eq!(close_id.get(), 1);
    assert_eq!(first.partition(), partition(2, 3));
    assert_eq!(*first_partition, partition(2, 3));
    assert_eq!(second.partition(), partition(1, 4));
    assert_eq!(*second_partition, partition(1, 4));
    assert_eq!(*first_epoch, epoch);
    assert_eq!(*second_epoch, epoch);
    assert!(first.position_epoch() > paused_fence.position_epoch());
    assert!(second.position_epoch() > second_fetch.position().position_epoch());
    assert_eq!(
        machine.fetch_ownership(*first_fetch),
        Ok(FetchOwnership::Superseded)
    );
    assert_eq!(
        machine.fetch_ownership(*second_fetch),
        Ok(FetchOwnership::Superseded)
    );
    assert!(machine.is_closed());
}

#[test]
fn close_cannot_regenerate_fetch_work_or_accept_controls() {
    let mut machine = AssignedConsumerMachine::new();
    let assignment = assign(
        &mut machine,
        vec![assigned(1, 0, StartPosition::Offset(offset(7)))],
    );
    let AssignedConsumerEffect::FetchReady { fence, .. } = assignment.effects()[0] else {
        panic!("explicit position must start Fetch");
    };
    let epoch = assignment
        .assignment_epoch()
        .unwrap_or_else(|| panic!("assigned transition epoch"));
    let close = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("begin close: {error}"));
    let AssignedConsumerEffect::Suspend {
        fence: closed_fence,
    } = close.effects()[1]
    else {
        panic!("close must publish its final position fence");
    };
    assert!(
        close
            .effects()
            .iter()
            .all(|effect| !matches!(effect, AssignedConsumerEffect::FetchReady { .. }))
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::Resume {
            assignment_epoch: epoch,
            partition: partition(1, 0),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(100),
        }),
        Err(AssignedConsumerMachineError::ConsumerClosed)
    );
    assert_eq!(
        machine.apply(AssignedConsumerInput::FetchAdvanced {
            fence,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(8),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        }),
        Err(AssignedConsumerMachineError::StalePosition {
            active: closed_fence,
            supplied: fence.position(),
        })
    );
}

#[test]
fn close_fences_pending_resolution_without_emitting_new_work() {
    let mut machine = AssignedConsumerMachine::new();
    let assigned = assign(&mut machine, vec![assigned(3, 2, StartPosition::Beginning)]);
    let AssignedConsumerEffect::ResolvePosition { fence, .. } = assigned.effects()[0] else {
        panic!("beginning position must resolve");
    };
    let closed = machine
        .apply(AssignedConsumerInput::BeginClose)
        .unwrap_or_else(|error| panic!("close resolving consumer: {error}"));
    let AssignedConsumerEffect::Suspend {
        fence: closed_fence,
    } = closed.effects()[1]
    else {
        panic!("close must fence after acceptance");
    };
    assert_eq!(
        machine.apply(AssignedConsumerInput::PositionResolved {
            fence,
            next_offset: offset(4),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        }),
        Err(AssignedConsumerMachineError::StalePosition {
            active: closed_fence,
            supplied: fence,
        })
    );
}

#[test]
fn close_preflight_failure_preserves_every_partition_and_admission() {
    let mut machine = AssignedConsumerMachine::new();
    let assignment = assign(
        &mut machine,
        vec![
            assigned(1, 0, StartPosition::Offset(offset(1))),
            assigned(1, 1, StartPosition::Offset(offset(2))),
        ],
    );
    let AssignedConsumerEffect::FetchReady {
        fence: first_fetch, ..
    } = assignment.effects()[0]
    else {
        panic!("first explicit position must start Fetch");
    };
    let exhausted = PositionEpoch::try_from_raw_for_test(u64::MAX)
        .unwrap_or_else(|| panic!("maximum position epoch is nonzero"));
    machine
        .assignment
        .as_mut()
        .unwrap_or_else(|| panic!("test assignment"))
        .find_mut(partition(1, 1))
        .unwrap_or_else(|error| panic!("second partition: {error}"))
        .replace_position_epoch_for_test(exhausted);

    assert_eq!(
        machine.apply(AssignedConsumerInput::BeginClose),
        Err(AssignedConsumerMachineError::PositionEpochExhausted {
            partition: partition(1, 1),
        })
    );
    assert!(!machine.is_closed());
    assert_eq!(
        machine.fetch_ownership(first_fetch),
        Ok(FetchOwnership::Active)
    );
}
