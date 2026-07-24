//! Deliberately unbacked Fetch output reservation construction.

struct FetchOutputReservation;

impl FetchOutputReservation {
    fn from_acquired_capacity(_bytes: usize) -> Self {
        Self
    }
}

fn mint_without_capacity_owner() {
    let _reservation = FetchOutputReservation::from_acquired_capacity(4_096);
}
