//! Fair hosted share-membership turn and closed execution error vocabulary.

use kafka_client_core::Moment;

use crate::{clock::MonotonicClock, driver::DriverOwner};

use super::{
    ShareMembershipError, registry::ShareConsumerRegistry, registry_close::ShareConsumerCloseTurn,
    registry_heartbeat_due::ShareHeartbeatDueTurn,
    registry_heartbeat_settlement::ShareHeartbeatSettlementTurn,
    registry_heartbeat_submission::ShareHeartbeatSubmissionTurn,
    registry_invalidation::ShareInvalidationTurn, registry_topic_identity::ShareTopicIdentityTurn,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareMembershipTurn {
    Idle,
    Progress,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShareMembershipHostError {
    Membership(ShareMembershipError),
    EffectShape,
    Invalidation,
}

impl From<ShareMembershipError> for ShareMembershipHostError {
    fn from(error: ShareMembershipError) -> Self {
        Self::Membership(error)
    }
}

impl ShareConsumerRegistry {
    pub(crate) fn turn_membership(
        &mut self,
        now: Moment,
        clock: &MonotonicClock,
        driver: &DriverOwner,
    ) -> Result<ShareMembershipTurn, ShareMembershipHostError> {
        if self
            .reclaim_one_close_completion()
            .map_err(|_error| ShareMembershipHostError::EffectShape)?
        {
            return Ok(ShareMembershipTurn::Progress);
        }
        let heartbeat_blocked = match self.settle_one_heartbeat(now, clock)? {
            ShareHeartbeatSettlementTurn::Progress => return Ok(ShareMembershipTurn::Progress),
            ShareHeartbeatSettlementTurn::Blocked => true,
            ShareHeartbeatSettlementTurn::Idle => false,
        };
        let topic_blocked = match self.turn_one_topic_identity(now, driver) {
            Ok(ShareTopicIdentityTurn::Progress) => return Ok(ShareMembershipTurn::Progress),
            Ok(ShareTopicIdentityTurn::Blocked) => true,
            Ok(ShareTopicIdentityTurn::Idle) => false,
            Err(_error) => return Err(ShareMembershipHostError::EffectShape),
        };
        let invalidation_blocked = match self.drive_one_invalidation(driver)? {
            ShareInvalidationTurn::Progress => return Ok(ShareMembershipTurn::Progress),
            ShareInvalidationTurn::Blocked => true,
            ShareInvalidationTurn::Idle => false,
        };
        let close_blocked = match self.turn_one_close(now)? {
            ShareConsumerCloseTurn::Progress => return Ok(ShareMembershipTurn::Progress),
            ShareConsumerCloseTurn::Blocked => true,
            ShareConsumerCloseTurn::Idle => false,
        };
        if self.prepare_one_heartbeat_due(now, clock)? == ShareHeartbeatDueTurn::Progress {
            return Ok(ShareMembershipTurn::Progress);
        }
        Ok(match self.submit_one_heartbeat(now, clock, driver)? {
            ShareHeartbeatSubmissionTurn::Progress => ShareMembershipTurn::Progress,
            ShareHeartbeatSubmissionTurn::Blocked => ShareMembershipTurn::Blocked,
            ShareHeartbeatSubmissionTurn::Idle
                if heartbeat_blocked || topic_blocked || invalidation_blocked || close_blocked =>
            {
                ShareMembershipTurn::Blocked
            }
            ShareHeartbeatSubmissionTurn::Idle => ShareMembershipTurn::Idle,
        })
    }
}
