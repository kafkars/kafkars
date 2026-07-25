//! Forbidden sibling consumer of prepared-entry reservation ownership.

fn steal<T>(reservation: T) {
    reservation.into_entries();
    reservation.recover_entries();
}
