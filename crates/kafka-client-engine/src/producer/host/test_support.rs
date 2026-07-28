//! Focused producer host fault controls available only to unit evidence.

use super::{ProducerHost, ProducerHostInvariantError};

impl ProducerHost {
    pub(in crate::producer) fn inject_post_acceptance_fault(
        &mut self,
        error: ProducerHostInvariantError,
    ) {
        self.post_acceptance_fault = Some(error);
    }

    pub(in crate::producer) fn take_post_acceptance_fault(
        &mut self,
    ) -> Option<ProducerHostInvariantError> {
        self.post_acceptance_fault.take()
    }
}
