//! Forbidden sibling consumer of prepared-entry reservation ownership.

fn steal<T>(reservation: T) {
    reservation.into_entries();
    reservation.recover_group_offset_commit_entries();
}
