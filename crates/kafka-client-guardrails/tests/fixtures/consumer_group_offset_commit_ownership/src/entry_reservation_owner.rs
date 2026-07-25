//! Exact fixture owner of prepared-entry reservation transfer and recovery.

fn transfer<T>(reservation: T) {
    reservation.into_entries();
}

fn recover_entries<T>(reservation: T) {
    reservation.recover_entries();
}
