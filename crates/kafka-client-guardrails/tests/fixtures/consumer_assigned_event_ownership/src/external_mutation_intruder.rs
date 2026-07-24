//! Deliberately mutates owner events from a typed free-function argument.

struct AssignedConsumerOwner {
    events: Vec<u8>,
}

fn violate_from_outside_owner(owner: &mut AssignedConsumerOwner) {
    owner.events.clear();
}
