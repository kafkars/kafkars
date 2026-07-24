//! Deliberately cloneable close state mutated outside its configured owner.

#[derive(Clone, Copy)]
enum AssignedConsumerCloseState {
    Open,
}

#[derive(Clone, Copy)]
struct AssignedConsumerMachine {
    close_state: AssignedConsumerCloseState,
}

impl AssignedConsumerMachine {
    fn close(&mut self) {
        self.close_state = AssignedConsumerCloseState::Open;
    }
}
