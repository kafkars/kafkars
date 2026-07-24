//! Preparation failures at the direct position-execution boundary.

use crate::driver::PositionRequestPreparationError;

/// Preparation rejected a non-resolution effect without changing core state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparePositionError {
    UnexpectedEffect,
    DeadlineMismatch {
        effect: kafka_client_core::Deadline,
        operation: kafka_client_core::Deadline,
    },
}

impl From<PositionRequestPreparationError> for PreparePositionError {
    fn from(error: PositionRequestPreparationError) -> Self {
        match error {
            PositionRequestPreparationError::UnexpectedEffect => Self::UnexpectedEffect,
            PositionRequestPreparationError::DeadlineMismatch { effect, operation } => {
                Self::DeadlineMismatch { effect, operation }
            }
        }
    }
}
