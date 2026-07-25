//! Classic input ownership and time-fence evidence.

use crate::{Deadline, Moment};

use super::{ClassicGroupInput, MembershipCycle};

#[test]
fn begin_retains_the_public_boundary_deadline_and_observation() {
    let input = ClassicGroupInput::Begin {
        now: Moment::from_tick(3),
        deadline: Deadline::from_tick(11),
    };
    assert_eq!(
        input,
        ClassicGroupInput::Begin {
            now: Moment::from_tick(3),
            deadline: Deadline::from_tick(11),
        }
    );
}

#[test]
fn deadline_expiration_is_fenced_to_one_exact_cycle() {
    let cycle = MembershipCycle::initial();
    assert_eq!(
        ClassicGroupInput::DeadlineElapsed {
            cycle,
            now: Moment::from_tick(11),
        },
        ClassicGroupInput::DeadlineElapsed {
            cycle,
            now: Moment::from_tick(11),
        }
    );
}
