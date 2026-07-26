//! Ordered mechanism requests emitted by transaction initialization.

use crate::{Deadline, OperationId};

use super::{
    TransactionInitializationPlan, TransactionInitializationTerminal, TransactionalOwnerId,
};

/// One concrete mechanism request emitted by initialization policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionInitializationEffect {
    /// Submit one `InitProducerId` request with the original deadline.
    Submit {
        /// Opaque owner whose transactional-id string remains in the engine.
        owner_id: TransactionalOwnerId,
        /// Stable operation identity reserved before machine construction.
        operation_id: OperationId,
        /// Original public absolute deadline.
        deadline: Deadline,
        /// Exact scalar request intent.
        plan: TransactionInitializationPlan,
    },
    /// Publish the sole terminal decision.
    Complete {
        /// Opaque transactional owner fence.
        owner_id: TransactionalOwnerId,
        /// Stable operation identity.
        operation_id: OperationId,
        /// Core-owned terminal decision.
        terminal: TransactionInitializationTerminal,
    },
}

/// Ordered result of one deterministic transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionInitializationTransition {
    effect: Option<TransactionInitializationEffect>,
}

impl TransactionInitializationTransition {
    pub(super) const fn none() -> Self {
        Self { effect: None }
    }

    pub(super) const fn one(effect: TransactionInitializationEffect) -> Self {
        Self {
            effect: Some(effect),
        }
    }

    /// Consumes this transition into its optional effect.
    pub fn into_effect(self) -> Option<TransactionInitializationEffect> {
        self.effect
    }
}
