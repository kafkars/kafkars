//! Selection-specific `DescribeTopics` response correlation.

use crate::admin::{
    DescribeTopicOutcome, DescribeTopicsMachine, DescribeTopicsMachineError,
    DescribeTopicsSelection,
};

impl DescribeTopicsMachine {
    pub(super) fn validate_outcomes(
        &self,
        outcomes: &[DescribeTopicOutcome],
    ) -> Result<(), DescribeTopicsMachineError> {
        if !self.plan.include_authorized_operations()
            && outcomes
                .iter()
                .any(DescribeTopicOutcome::has_authorized_operations)
        {
            return Err(DescribeTopicsMachineError::UnexpectedAuthorizedOperations);
        }
        match self.plan.selection() {
            DescribeTopicsSelection::Named(topics) => {
                if topics.len() != outcomes.len() {
                    return Err(DescribeTopicsMachineError::OutcomeCountMismatch);
                }
                if topics
                    .iter()
                    .zip(outcomes)
                    .any(|(topic, outcome)| topic != outcome.topic())
                {
                    return Err(DescribeTopicsMachineError::OutcomeTopicMismatch);
                }
            }
            DescribeTopicsSelection::Ids(_) => {
                return Err(DescribeTopicsMachineError::OutcomeSelectionMismatch);
            }
            DescribeTopicsSelection::All { .. } => validate_all_outcomes(outcomes)?,
        }
        Ok(())
    }
}

fn validate_all_outcomes(
    outcomes: &[DescribeTopicOutcome],
) -> Result<(), DescribeTopicsMachineError> {
    if outcomes.iter().any(|outcome| outcome.topic().is_empty()) {
        return Err(DescribeTopicsMachineError::EmptyOutcomeTopic);
    }
    for pair in outcomes.windows(2) {
        match pair[0].topic().as_bytes().cmp(pair[1].topic().as_bytes()) {
            core::cmp::Ordering::Less => {}
            core::cmp::Ordering::Equal => {
                return Err(DescribeTopicsMachineError::DuplicateOutcomeTopic);
            }
            core::cmp::Ordering::Greater => {
                return Err(DescribeTopicsMachineError::OutcomeTopicOrder);
            }
        }
    }
    Ok(())
}
