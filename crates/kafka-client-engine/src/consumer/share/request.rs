//! Strict request materialization from one prepared core share-heartbeat effect.

use kafka_client_core::ShareGroupHeartbeatRequestKind;

use crate::protocol::consumer::share_group::{
    PreparedShareGroupHeartbeatRequest, ShareGroupHeartbeatRequestFailure,
    share_group_join_request, share_group_leave_request, share_group_steady_request,
};

use super::{
    ShareMembershipError, ShareMembershipInterpreter, membership::ShareMembershipRetryGate,
};

impl ShareMembershipInterpreter {
    pub(super) fn prepare_request(
        &self,
    ) -> Result<PreparedShareGroupHeartbeatRequest, ShareMembershipError> {
        if self.retry_gate != ShareMembershipRetryGate::Open {
            return Err(ShareMembershipError::Occupied);
        }
        let prepared = self.prepared.ok_or(ShareMembershipError::EffectShape)?;
        let request = match prepared.kind {
            ShareGroupHeartbeatRequestKind::Join
                if prepared.member_epoch.is_none() && prepared.assignment_generation.is_none() =>
            {
                let topics: Vec<&str> = self.catalog.topic_names().collect();
                share_group_join_request(
                    self.catalog.group(),
                    self.catalog.member(),
                    self.catalog.rack(),
                    &topics,
                )
            }
            ShareGroupHeartbeatRequestKind::Steady
                if prepared.member_epoch.is_some()
                    && prepared.member_epoch == prepared.attempt.member_epoch() =>
            {
                share_group_steady_request(
                    self.catalog.group(),
                    self.catalog.member(),
                    prepared
                        .member_epoch
                        .map(kafka_client_core::ShareGroupMemberEpoch::get)
                        .ok_or(ShareMembershipError::EffectShape)?,
                )
            }
            ShareGroupHeartbeatRequestKind::Leave
                if prepared.member_epoch.is_some()
                    && prepared.member_epoch == prepared.attempt.member_epoch() =>
            {
                share_group_leave_request(self.catalog.group(), self.catalog.member())
            }
            _ => return Err(ShareMembershipError::EffectShape),
        };
        request.map_err(map_request_error)
    }
}

fn map_request_error(_error: ShareGroupHeartbeatRequestFailure) -> ShareMembershipError {
    ShareMembershipError::EffectShape
}
