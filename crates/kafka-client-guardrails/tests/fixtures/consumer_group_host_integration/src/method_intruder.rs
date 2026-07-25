//! Privileged classic-cycle and Join-transfer methods forbidden by this fixture.

fn intrude(owner: &mut Owner) {
    owner.try_begin_classic_cycle();
    owner.into_driver_acceptance();
    owner.confirm_join_driver_owned();
    owner.borrow_execution_state();
    owner.replace_execution_state();
    owner.set_execution_state();
}

struct Owner;

impl Owner {
    fn try_begin_classic_cycle(&mut self) {}
    fn into_driver_acceptance(&mut self) {}
    fn confirm_join_driver_owned(&mut self) {}
    fn borrow_execution_state(&mut self) {}
    fn replace_execution_state(&mut self) {}
    fn set_execution_state(&mut self) {}
}
