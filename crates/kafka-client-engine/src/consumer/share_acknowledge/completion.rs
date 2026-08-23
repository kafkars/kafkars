//! Linear completion identity, reconstruction, and terminal-publication ownership.

use crate::{
    completion::CompletionId,
    consumer::{
        share_acknowledge::ShareAcknowledgeOutcome, share_batch::ShareAcknowledgementRecovery,
    },
};

/// One accepted acknowledgement before or after terminal translation.
#[must_use = "accepted acknowledgement completion must publish exactly once"]
pub(in crate::consumer) enum ShareAcknowledgementCompletionOwner {
    Pending {
        id: CompletionId,
        recovery: ShareAcknowledgementRecovery,
    },
    Publishable {
        id: CompletionId,
        terminal: Box<ShareAcknowledgeOutcome>,
    },
}

impl ShareAcknowledgementCompletionOwner {
    pub(in crate::consumer) const fn pending(
        id: CompletionId,
        recovery: ShareAcknowledgementRecovery,
    ) -> Self {
        Self::Pending { id, recovery }
    }

    pub(in crate::consumer) fn into_pending(
        self,
    ) -> Option<(CompletionId, ShareAcknowledgementRecovery)> {
        match self {
            Self::Pending { id, recovery } => Some((id, recovery)),
            Self::Publishable { .. } => None,
        }
    }

    pub(in crate::consumer) fn publishable(
        id: CompletionId,
        terminal: ShareAcknowledgeOutcome,
    ) -> Self {
        Self::Publishable {
            id,
            terminal: Box::new(terminal),
        }
    }

    pub(in crate::consumer) fn into_publishable(
        self,
    ) -> Result<(CompletionId, ShareAcknowledgeOutcome), Self> {
        match self {
            Self::Publishable { id, terminal } => Ok((id, *terminal)),
            pending @ Self::Pending { .. } => Err(pending),
        }
    }
}
