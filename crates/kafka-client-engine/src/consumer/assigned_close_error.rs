//! Exact invariant diagnostics for fixed-capacity assigned-close retention.

use kafka_client_core::{AssignedConsumerCloseId, AssignedConsumerEffect};

use crate::completion::CompletionId;

/// Observable phase of the single assigned-consumer close slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedCloseSlotPhase {
    /// No terminal capacity is currently held.
    Vacant,
    /// Terminal capacity is reserved before core close admission.
    Reserved,
    /// Core accepted and identified the close operation.
    Accepted,
    /// Core authorized the sole retained terminal outcome.
    Ready,
    /// The terminal crossed notifier ownership exactly once.
    Published,
}

/// Close effect kind preserved in invariant diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedCloseEffectKind {
    /// Core accepted the close.
    Accept,
    /// Core authorized terminal completion.
    Complete,
}

/// Exact reason a close-slot transition could not be applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedCloseSlotError {
    /// Capacity was reserved outside the sole vacant phase.
    InvalidReservation {
        /// Phase retained by the failed transition.
        phase: AssignedCloseSlotPhase,
    },
    /// A rejected core admission was released outside the reserved phase.
    InvalidRelease {
        /// Phase retained by the failed transition.
        phase: AssignedCloseSlotPhase,
    },
    /// A close effect arrived before its required predecessor.
    EffectOutOfOrder {
        /// Effect that could not be applied.
        effect: AssignedCloseEffectKind,
        /// Identity supplied by core.
        close_id: AssignedConsumerCloseId,
        /// Phase retained by the failed transition.
        phase: AssignedCloseSlotPhase,
    },
    /// A close effect named a different operation than the retained owner.
    MismatchedCloseId {
        /// Effect that could not be applied.
        effect: AssignedCloseEffectKind,
        /// Identity already bound to the slot.
        active: AssignedConsumerCloseId,
        /// Different identity supplied by the effect.
        supplied: AssignedConsumerCloseId,
    },
    /// Publication named a different completion reservation.
    MismatchedCompletionId {
        /// Completion reservation retained by the slot.
        active: CompletionId,
        /// Different reservation supplied by publication.
        supplied: CompletionId,
    },
    /// Core repeated an effect already applied for the same operation.
    DuplicateEffect {
        /// Repeated effect.
        effect: AssignedCloseEffectKind,
        /// Identity retained by the slot.
        close_id: AssignedConsumerCloseId,
    },
    /// An effect arrived after terminal publication.
    StaleEffect {
        /// Stale effect.
        effect: AssignedCloseEffectKind,
        /// Identity supplied by the stale effect.
        close_id: AssignedConsumerCloseId,
    },
    /// A non-close effect was routed to this narrow mechanism.
    UnexpectedEffect {
        /// Unconsumed effect retained in the diagnostic.
        effect: AssignedConsumerEffect,
    },
    /// The owner queried a normal terminal value before core authorized it.
    TerminalUnavailable {
        /// Phase retained by the failed transition.
        phase: AssignedCloseSlotPhase,
    },
    /// The owner queried the draining identity outside the accepted phase.
    AcceptedIdUnavailable {
        /// Phase retained by the failed query.
        phase: AssignedCloseSlotPhase,
    },
}
