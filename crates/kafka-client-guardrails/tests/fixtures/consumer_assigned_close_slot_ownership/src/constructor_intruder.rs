//! Deliberately constructs the slot before its lifecycle owner exists.

struct AssignedCloseSlot;

impl AssignedCloseSlot {
    fn create_for_assigned_owner() -> Self {
        Self
    }
}

fn violate() {
    let _slot = AssignedCloseSlot::create_for_assigned_owner();
}
