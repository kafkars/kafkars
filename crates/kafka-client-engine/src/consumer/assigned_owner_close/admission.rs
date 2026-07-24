//! Completion-first admission for one assigned-consumer close.

use kafka_client_core::AssignedConsumerInput;

use crate::consumer::{
    assigned_host::AssignedConsumerCloseObserver, assigned_owner::AssignedConsumerOwner,
    assigned_owner_fault::AssignedConsumerOwnerFault,
    assigned_owner_model::AssignedConsumerOwnerError,
};

impl AssignedConsumerOwner {
    /// Reserves terminal capacity before core can accept close.
    pub(crate) fn begin_close(
        &mut self,
    ) -> Result<AssignedConsumerCloseObserver, AssignedConsumerOwnerError> {
        self.ensure_admission_ready()?;
        let (completion_id, observer) = self
            .close_completions
            .reserve()
            .map_err(AssignedConsumerOwnerError::Completion)?;
        if let Err(error) = self.close.reserve(completion_id) {
            if let Err(rollback) = self.close_completions.rollback_reservation(completion_id) {
                self.fault = Some(AssignedConsumerOwnerFault::CloseCompletion(rollback));
                return Err(AssignedConsumerOwnerError::Faulted);
            }
            return Err(AssignedConsumerOwnerError::Close(error));
        }
        let transition = match self.machine.apply(AssignedConsumerInput::BeginClose) {
            Ok(transition) => transition,
            Err(error) => {
                let completion_id = match self.close.release_rejected() {
                    Ok(completion_id) => completion_id,
                    Err(release) => {
                        self.fault = Some(AssignedConsumerOwnerFault::Close(release));
                        return Err(AssignedConsumerOwnerError::Faulted);
                    }
                };
                if let Err(rollback) = self.close_completions.rollback_reservation(completion_id) {
                    self.fault = Some(AssignedConsumerOwnerFault::CloseCompletion(rollback));
                    return Err(AssignedConsumerOwnerError::Faulted);
                }
                return Err(AssignedConsumerOwnerError::Core(error));
            }
        };
        self.enqueue_transition(transition, None);
        Ok(AssignedConsumerCloseObserver::from_completion(observer))
    }
}
