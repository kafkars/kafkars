//! Deliberately cloneable assigned-close owners.

#[derive(Clone, Copy)]
enum AssignedCloseState {
    Vacant,
}

#[derive(Clone, Copy)]
struct AssignedCloseSlot {
    state: AssignedCloseState,
}
