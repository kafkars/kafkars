//! Deliberately steals prepared event-claim transfer capabilities.

struct Intruder;

impl Intruder {
    fn violate(&self) {
        self.install_replacement_claims();
        self.install_partition_claim();
        self.commit_event_claims();
        self.rollback_event_claims();
        self.take_event();
        self.retain_terminal();
        self.observe_effect();
    }
}
