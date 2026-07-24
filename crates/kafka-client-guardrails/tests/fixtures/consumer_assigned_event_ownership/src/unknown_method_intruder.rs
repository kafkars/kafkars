//! Deliberately invokes an unclassified event mutator outside its owner.

struct AssignedConsumerEventStore;

impl AssignedConsumerEventStore {
    fn recover_after_driver_shutdown(&mut self) {}
}

struct AssignedConsumerOwner {
    events: AssignedConsumerEventStore,
}

fn violate_from_outside_owner(owner: &mut AssignedConsumerOwner) {
    owner.events.recover_after_driver_shutdown();
}
