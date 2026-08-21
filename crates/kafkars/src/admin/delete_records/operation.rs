//! Named single-observer Admin `DeleteRecords` operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_delete_records::AdminDeleteRecords};

use super::DeleteRecordsResult;

/// Sole terminal observer for one submitted Admin `DeleteRecords` query.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DeleteRecords {
    inner: AdminDeleteRecords,
}

impl DeleteRecords {
    pub(crate) const fn from_bridge(inner: AdminDeleteRecords) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DeleteRecordsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DeleteRecords {
    type Output = Result<DeleteRecordsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
