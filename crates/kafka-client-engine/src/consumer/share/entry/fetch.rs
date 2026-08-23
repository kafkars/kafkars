//! Narrow borrowing seam for one member's hosted share-fetch state.

use super::ShareConsumerEntry;
use crate::consumer::share::{ShareMembershipInterpreter, fetch_state::ShareFetchEntryState};

impl ShareConsumerEntry {
    pub(in crate::consumer::share) const fn fetch(&self) -> &ShareFetchEntryState {
        &self.fetch
    }

    pub(in crate::consumer::share) fn fetch_mut(&mut self) -> &mut ShareFetchEntryState {
        &mut self.fetch
    }

    pub(in crate::consumer::share) fn fetch_and_membership(
        &mut self,
    ) -> (
        &mut ShareFetchEntryState,
        Option<&ShareMembershipInterpreter>,
    ) {
        (&mut self.fetch, self.membership.as_ref())
    }
}
