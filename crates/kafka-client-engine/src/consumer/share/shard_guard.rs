//! Registry guard projection and unlock notification ownership.

use std::ops::{Deref, DerefMut};

use crate::consumer::share_recv::ShareConsumerRecvWait;

use super::{registry::ShareConsumerRegistry, shard::ShareConsumerRegistryGuard};

impl Deref for ShareConsumerRegistryGuard<'_> {
    type Target = ShareConsumerRegistry;

    fn deref(&self) -> &Self::Target {
        self.registry
            .as_deref()
            .unwrap_or_else(|| unreachable!("share registry guard is present before Drop"))
    }
}

impl DerefMut for ShareConsumerRegistryGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.registry
            .as_deref_mut()
            .unwrap_or_else(|| unreachable!("share registry guard is present before Drop"))
    }
}

impl Drop for ShareConsumerRegistryGuard<'_> {
    fn drop(&mut self) {
        drop(self.registry.take());
        self.shared
            .request_share_recv_notification(ShareConsumerRecvWait::Unlock);
    }
}
