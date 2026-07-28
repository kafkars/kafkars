//! Narrow group-registry capability for generation-fenced receive waiting.

use std::task::Waker;

use kafka_client_core::GroupId;

use crate::consumer::group_recv::{
    GroupConsumerRecvError, GroupConsumerRecvRegistration, GroupConsumerRecvSignalError,
    GroupConsumerRecvWait,
};

use super::registry_port::GroupConsumerPort;

impl GroupConsumerPort {
    pub(in crate::consumer) fn arm_group_recv_task(
        &self,
        group_id: GroupId,
        current: Option<GroupConsumerRecvRegistration>,
        wait: GroupConsumerRecvWait,
        waker: &Waker,
    ) -> Result<GroupConsumerRecvRegistration, GroupConsumerRecvError> {
        self.shared
            .group_recv_signal()
            .arm_task(group_id, current, wait, waker)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn arm_group_recv_blocking(
        &self,
        group_id: GroupId,
        current: Option<GroupConsumerRecvRegistration>,
        wait: GroupConsumerRecvWait,
    ) -> Result<GroupConsumerRecvRegistration, GroupConsumerRecvError> {
        self.shared
            .group_recv_signal()
            .arm_blocking(group_id, current, wait)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn rearm_group_recv_task(
        &self,
        group_id: GroupId,
        current: GroupConsumerRecvRegistration,
        wait: GroupConsumerRecvWait,
        waker: &Waker,
    ) -> Result<GroupConsumerRecvRegistration, GroupConsumerRecvError> {
        self.shared
            .group_recv_signal()
            .rearm_task(group_id, current, wait, waker)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn rearm_group_recv_blocking(
        &self,
        group_id: GroupId,
        current: GroupConsumerRecvRegistration,
        wait: GroupConsumerRecvWait,
    ) -> Result<GroupConsumerRecvRegistration, GroupConsumerRecvError> {
        self.shared
            .group_recv_signal()
            .rearm_blocking(group_id, current, wait)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn wait_group_recv(
        &self,
        registration: GroupConsumerRecvRegistration,
    ) -> Result<(), GroupConsumerRecvError> {
        self.shared
            .group_recv_signal()
            .wait(registration)
            .map_err(translate_recv_signal)
    }

    pub(in crate::consumer) fn cancel_group_recv(
        &self,
        registration: &mut Option<GroupConsumerRecvRegistration>,
    ) {
        if let Some(registration) = registration.take() {
            self.shared.group_recv_signal().cancel(registration);
        }
    }
}

const fn translate_recv_signal(_error: GroupConsumerRecvSignalError) -> GroupConsumerRecvError {
    GroupConsumerRecvError::internal_invariant()
}
