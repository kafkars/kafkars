//! Named single-observer SCRAM credential-alteration operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{KafkaError, bridge::alter_user_scram_credentials::AdminAlterUserScramCredentials};

use super::AlterUserScramCredentialsResult;

/// Sole terminal observer for one submitted SCRAM credential alteration.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct AlterUserScramCredentials {
    inner: AdminAlterUserScramCredentials,
}

impl AlterUserScramCredentials {
    pub(crate) const fn from_bridge(inner: AdminAlterUserScramCredentials) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<AlterUserScramCredentialsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for AlterUserScramCredentials {
    type Output = Result<AlterUserScramCredentialsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
