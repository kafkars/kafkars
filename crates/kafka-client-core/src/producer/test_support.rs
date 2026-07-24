//! Test-only producer identity installation shared by established scenarios.

use super::super::ProducerMachine;

impl ProducerMachine {
    pub(crate) fn install_identity_for_test(&mut self) {
        let generation = self.idempotence.begin_acquisition();
        let acquired = self
            .idempotence
            .plan_acquired(generation, 7, 2)
            .unwrap_or_else(|error| panic!("test identity must be valid: {error}"));
        if let Some(identity) = acquired {
            self.idempotence.commit_acquired(identity);
        }
        assert!(acquired.is_some());
    }
}
