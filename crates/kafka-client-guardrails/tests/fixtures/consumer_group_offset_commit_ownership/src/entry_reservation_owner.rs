//! Exact fixture owner of prepared-entry reservation transfer and recovery.

fn transfer<T>(reservation: T) {
    reservation.into_entries();
}

fn recover<T>(reservation: T) {
    reservation.recover_group_offset_commit_entries();
}
