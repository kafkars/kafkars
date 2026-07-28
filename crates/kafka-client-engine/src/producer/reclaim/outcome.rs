//! Core owners and exact identities carried through completion reclamation.

use kafka_client_core::ProducerInput;

use crate::completion::CompletionId;

use super::super::{
    binding::OperationBindings, flush::FlushBindings, terminal_backlog::ProducerTerminalOwner,
};

/// Result of one engine-side finish attempt after core accepted the input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompletionReclaimOutcome {
    /// Observer state is briefly locked; retry only the finish phase.
    Retry,
    /// Registry capacity and the exact producer binding were reclaimed.
    Reclaimed {
        /// Core producer owner whose terminal ownership ended.
        owner: ProducerTerminalOwner,
        /// Exact engine completion generation that was recycled.
        completion_id: CompletionId,
    },
    /// The exhausted registry slot was retired and its exact producer binding removed.
    Retired {
        /// Core producer owner whose terminal ownership ended.
        owner: ProducerTerminalOwner,
        /// Exact engine completion generation that exhausted.
        completion_id: CompletionId,
    },
}

pub(super) const fn reclaim_input(owner: ProducerTerminalOwner) -> ProducerInput {
    match owner {
        ProducerTerminalOwner::Record(operation_id) => {
            ProducerInput::CompletionReclaimed { operation_id }
        }
        ProducerTerminalOwner::Flush(flush_id) => {
            ProducerInput::FlushCompletionReclaimed { flush_id }
        }
    }
}

pub(super) fn owner_completion(
    owner: ProducerTerminalOwner,
    bindings: &OperationBindings,
    flush_bindings: &FlushBindings,
) -> Option<CompletionId> {
    match owner {
        ProducerTerminalOwner::Record(operation_id) => bindings.completion(operation_id),
        ProducerTerminalOwner::Flush(flush_id) => flush_bindings.completion(flush_id),
    }
}
