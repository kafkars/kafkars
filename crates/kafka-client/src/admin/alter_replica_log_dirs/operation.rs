//! Named single-observer replica log-directory alteration operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::admin_alter_replica_log_dirs::AdminAlterReplicaLogDirs};

use super::AlterReplicaLogDirsResult;

/// Sole terminal observer for one submitted replica log-directory alteration.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterReplicaLogDirs {
    inner: AdminAlterReplicaLogDirs,
}

impl AlterReplicaLogDirs {
    pub(crate) const fn from_bridge(inner: AdminAlterReplicaLogDirs) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<AlterReplicaLogDirsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for AlterReplicaLogDirs {
    type Output = Result<AlterReplicaLogDirsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
