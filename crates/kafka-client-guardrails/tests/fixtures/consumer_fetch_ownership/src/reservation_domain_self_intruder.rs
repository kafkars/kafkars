//! Deliberately hides reservation-domain construction behind `Self`.

struct FetchReservationDomain;

impl FetchReservationDomain {
    fn create_store_domain() -> Self {
        Self
    }

    fn mint_via_self() {
        let _domain = Self::create_store_domain();
    }
}
