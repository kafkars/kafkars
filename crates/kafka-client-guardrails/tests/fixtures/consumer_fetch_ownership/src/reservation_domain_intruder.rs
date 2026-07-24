//! Deliberately minted Fetch reservations outside the store owner.

struct FetchReservationDomain;

impl FetchReservationDomain {
    fn create_store_domain() -> Self {
        Self
    }

    fn issue_pair(&self, _sequence: u64, _bytes: usize) {}
}

fn mint_without_the_delivery_store() {
    let domain = FetchReservationDomain::create_store_domain();
    domain.issue_pair(1, 4_096);
}
