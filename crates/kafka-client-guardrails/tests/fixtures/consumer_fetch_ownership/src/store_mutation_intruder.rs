//! Deliberately mutated Fetch delivery-store fields outside their owners.

struct FetchDeliveryStore {
    next_sequence: Option<u64>,
    next_authorization: Option<u64>,
    used_bytes: usize,
    slots: Vec<FetchSlot>,
}

impl FetchDeliveryStore {
    fn mutate(&mut self) {
        self.next_sequence = None;
        self.next_authorization = None;
        self.used_bytes = 1;
        self.slots.push(FetchSlot {
            charged_bytes: 0,
            provenance: false,
            outcome: false,
            state: 0,
        });
    }
}

struct FetchSlot {
    charged_bytes: usize,
    provenance: bool,
    outcome: bool,
    state: u8,
}

impl FetchSlot {
    fn mutate(&mut self) {
        self.charged_bytes = 1;
        self.provenance = true;
        self.outcome = true;
        self.state = 1;
    }
}
