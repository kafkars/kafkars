//! Single-slot completion ownership paired with one broker-session acknowledgement.

use crate::consumer::share_acknowledge::ShareAcknowledgementCompletionOwner;

use super::fetch_session::ShareFetchSessionOwner;

impl ShareFetchSessionOwner {
    pub(in crate::consumer::share) fn acknowledgement_completion_is_present(&self) -> bool {
        self.acknowledgement_completion.is_some()
    }

    pub(in crate::consumer::share) fn acknowledgement_completion_is_publishable(&self) -> bool {
        self.acknowledgement_completion
            .as_ref()
            .is_some_and(|owner| {
                matches!(
                    owner,
                    ShareAcknowledgementCompletionOwner::Publishable { .. }
                )
            })
    }

    pub(in crate::consumer::share) fn install_acknowledgement_completion(
        &mut self,
        completion: ShareAcknowledgementCompletionOwner,
    ) -> Result<(), ShareAcknowledgementCompletionOwner> {
        if self.acknowledgement_completion.is_some() {
            return Err(completion);
        }
        self.acknowledgement_completion = Some(completion);
        Ok(())
    }

    pub(in crate::consumer::share) fn take_acknowledgement_completion(
        &mut self,
    ) -> Option<ShareAcknowledgementCompletionOwner> {
        self.acknowledgement_completion.take()
    }
}
