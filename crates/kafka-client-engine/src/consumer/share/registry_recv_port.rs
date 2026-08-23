//! Narrow share-registry capability for generation-fenced receive waiting.

use std::task::Waker;

use kafka_client_core::GroupId;

use crate::consumer::share_recv::{
    ShareConsumerRecvError, ShareConsumerRecvRegistration, ShareConsumerRecvSignalError,
    ShareConsumerRecvWait,
};

use super::port::ShareConsumerPort;

impl ShareConsumerPort {
    pub(in crate::consumer) fn arm_share_recv_task(
        &self,
        group_id: GroupId,
        current: Option<ShareConsumerRecvRegistration>,
        wait: ShareConsumerRecvWait,
        waker: &Waker,
    ) -> Result<ShareConsumerRecvRegistration, ShareConsumerRecvError> {
        self.shared
            .share_recv_signal()
            .arm_task(group_id, current, wait, waker)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn arm_share_recv_blocking(
        &self,
        group_id: GroupId,
        current: Option<ShareConsumerRecvRegistration>,
        wait: ShareConsumerRecvWait,
    ) -> Result<ShareConsumerRecvRegistration, ShareConsumerRecvError> {
        self.shared
            .share_recv_signal()
            .arm_blocking(group_id, current, wait)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn rearm_share_recv_task(
        &self,
        group_id: GroupId,
        current: ShareConsumerRecvRegistration,
        wait: ShareConsumerRecvWait,
        waker: &Waker,
    ) -> Result<ShareConsumerRecvRegistration, ShareConsumerRecvError> {
        self.shared
            .share_recv_signal()
            .rearm_task(group_id, current, wait, waker)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn rearm_share_recv_blocking(
        &self,
        group_id: GroupId,
        current: ShareConsumerRecvRegistration,
        wait: ShareConsumerRecvWait,
    ) -> Result<ShareConsumerRecvRegistration, ShareConsumerRecvError> {
        self.shared
            .share_recv_signal()
            .rearm_blocking(group_id, current, wait)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn wait_share_recv(
        &self,
        registration: ShareConsumerRecvRegistration,
    ) -> Result<(), ShareConsumerRecvError> {
        self.shared
            .share_recv_signal()
            .wait(registration)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn cancel_share_recv(
        &self,
        registration: &mut Option<ShareConsumerRecvRegistration>,
    ) {
        if let Some(registration) = registration.take() {
            self.shared.share_recv_signal().cancel(registration);
        }
    }
}

const fn translate_recv_signal(_error: ShareConsumerRecvSignalError) -> ShareConsumerRecvError {
    ShareConsumerRecvError::internal_invariant()
}
