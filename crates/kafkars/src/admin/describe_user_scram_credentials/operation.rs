//! Named single-observer SCRAM credential-description operation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    KafkaError, bridge::describe_user_scram_credentials::AdminDescribeUserScramCredentials,
};

use super::DescribeUserScramCredentialsResult;

/// Sole terminal observer for one submitted SCRAM credential description.
#[derive(Debug)]
#[must_use = "dropping abandons observation without cancelling accepted admin work"]
pub struct DescribeUserScramCredentials {
    inner: AdminDescribeUserScramCredentials,
}

impl DescribeUserScramCredentials {
    pub(crate) const fn from_bridge(inner: AdminDescribeUserScramCredentials) -> Self {
        Self { inner }
    }

    /// Blocks on the same terminal observer used by [`Future::poll`].
    pub fn wait(self) -> Result<DescribeUserScramCredentialsResult, KafkaError> {
        self.inner.wait()
    }
}

impl Future for DescribeUserScramCredentials {
    type Output = Result<DescribeUserScramCredentialsResult, KafkaError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll(context)
    }
}
