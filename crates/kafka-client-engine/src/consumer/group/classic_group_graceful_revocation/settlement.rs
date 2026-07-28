//! Exact retirement only after the core owner retains completed or lost terminal.

use kafka_client_core::ClassicProcessingLease;

use super::{
    super::{
        classic_group_assignment::retire_and_revoke_classic_group_assignment,
        classic_group_fetch::ClassicGroupFetchOwner, classic_group_owner::ClassicGroupOwner,
        session_catalog::GroupSessionCatalog,
    },
    ClassicGroupRevocationHostError, ClassicGroupRevocationOwner,
};

impl ClassicGroupRevocationOwner {
    pub(in crate::consumer::group) fn settle_terminal(
        &mut self,
        classic: &ClassicGroupOwner,
        catalog: &mut GroupSessionCatalog,
        processing_lease: &mut ClassicProcessingLease,
        fetch: &mut ClassicGroupFetchOwner,
    ) -> Result<bool, ClassicGroupRevocationHostError> {
        let Some(terminal) = self.terminal() else {
            return Ok(false);
        };
        let pending = self
            .take_pending()
            .ok_or(ClassicGroupRevocationHostError::MissingPending)?;
        let assignment_epoch = terminal.lease().assignment_epoch();
        match retire_and_revoke_classic_group_assignment(
            classic,
            catalog,
            processing_lease,
            fetch,
            pending.assignment,
            pending.generation,
        ) {
            Ok(_retirement) => self.release_terminal(assignment_epoch)?,
            Err(failure) => {
                let kind = failure.kind;
                self.restore_pending(super::model::PendingClassicGroupRevocation::new(
                    failure.assignment,
                    failure.classic_generation,
                ));
                return Err(ClassicGroupRevocationHostError::Revocation(kind));
            }
        }
        Ok(true)
    }
}
