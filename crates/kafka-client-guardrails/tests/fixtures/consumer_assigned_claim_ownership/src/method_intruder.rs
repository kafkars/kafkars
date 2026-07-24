//! Deliberately constructs the one-shot claim slot elsewhere.

struct AssignedConsumerClaimSlot;
struct Intruder;

impl Intruder {
    fn violate(&self, port: Port) {
        let (_slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    }
}
