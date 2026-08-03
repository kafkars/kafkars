//! Bounded allocation and policy construction for the classic-group Fetch owner.

use std::{collections::VecDeque, time::Duration};

use kafka_client_core::{AssignedConsumerMachine, GroupPositionMissingOffsetPolicy, ReadIsolation};

use crate::{
    consumer::{
        assigned_event::AssignedConsumerEventStore, assigned_owner_model::fetch_isolation,
        assigned_timers::AssignedTimers, fetch_execution::DirectFetchExecutor,
        position_execution::PositionResolutionExecutor,
    },
    protocol::fetch::{FetchDecodeLimits, FetchRequestSettings},
};

use super::{
    model::ClassicGroupFetchBuildError,
    owner::{
        ClassicGroupFetchOwner, FIRST_GROUP_FETCH_CALLS, FIRST_GROUP_FETCH_DELIVERIES,
        FIRST_GROUP_FETCH_DELIVERY_BYTES, FIRST_GROUP_FETCH_EFFECTS,
        FIRST_GROUP_FETCH_OUTPUT_BYTES, FIRST_GROUP_FETCH_PARTITIONS,
    },
};

const FIRST_GROUP_FETCH_REQUEST_BYTES: u32 = 1024 * 1024;
const FIRST_GROUP_FETCH_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

impl ClassicGroupFetchOwner {
    pub(in crate::consumer::group) fn try_new() -> Result<Self, ClassicGroupFetchBuildError> {
        Self::try_new_with_policies(
            ReadIsolation::ReadUncommitted,
            GroupPositionMissingOffsetPolicy::Error,
        )
    }

    pub(in crate::consumer::group) fn try_new_with_read_isolation(
        read_isolation: ReadIsolation,
    ) -> Result<Self, ClassicGroupFetchBuildError> {
        Self::try_new_with_policies(read_isolation, GroupPositionMissingOffsetPolicy::Error)
    }

    pub(in crate::consumer::group) fn try_new_with_policies(
        read_isolation: ReadIsolation,
        missing_offset_policy: GroupPositionMissingOffsetPolicy,
    ) -> Result<Self, ClassicGroupFetchBuildError> {
        let mut effects = VecDeque::new();
        let mut raw_position_deadlines = VecDeque::new();
        let mut pending_positions = VecDeque::new();
        let mut pending_fetches = VecDeque::new();
        let mut reclaim_faults = Vec::new();
        effects
            .try_reserve_exact(FIRST_GROUP_FETCH_EFFECTS)
            .map_err(|_error| ClassicGroupFetchBuildError::Allocation)?;
        raw_position_deadlines
            .try_reserve_exact(FIRST_GROUP_FETCH_PARTITIONS)
            .map_err(|_error| ClassicGroupFetchBuildError::Allocation)?;
        pending_positions
            .try_reserve_exact(FIRST_GROUP_FETCH_PARTITIONS)
            .map_err(|_error| ClassicGroupFetchBuildError::Allocation)?;
        pending_fetches
            .try_reserve_exact(FIRST_GROUP_FETCH_PARTITIONS)
            .map_err(|_error| ClassicGroupFetchBuildError::Allocation)?;
        reclaim_faults
            .try_reserve_exact(FIRST_GROUP_FETCH_DELIVERIES)
            .map_err(|_error| ClassicGroupFetchBuildError::Allocation)?;
        let events = AssignedConsumerEventStore::new(FIRST_GROUP_FETCH_PARTITIONS)
            .map_err(|_error| ClassicGroupFetchBuildError::Allocation)?;
        let mut fetches = DirectFetchExecutor::create_unbound(
            FIRST_GROUP_FETCH_CALLS,
            FIRST_GROUP_FETCH_DELIVERIES,
            FIRST_GROUP_FETCH_DELIVERY_BYTES,
        );
        fetches
            .try_enable_sessions(FIRST_GROUP_FETCH_PARTITIONS)
            .map_err(|()| ClassicGroupFetchBuildError::Allocation)?;
        let fetch_settings = FetchRequestSettings::new(
            500,
            1,
            FIRST_GROUP_FETCH_REQUEST_BYTES,
            FIRST_GROUP_FETCH_REQUEST_BYTES,
            0,
        )
        .with_isolation(fetch_isolation(read_isolation));
        fetches.configure_broker_sessions(fetch_settings, FIRST_GROUP_FETCH_ATTEMPT_TIMEOUT);
        Ok(Self {
            machine: AssignedConsumerMachine::with_read_isolation(read_isolation),
            activation: None,
            timers: AssignedTimers::new(FIRST_GROUP_FETCH_PARTITIONS),
            positions: PositionResolutionExecutor::new(FIRST_GROUP_FETCH_CALLS),
            fetches,
            events,
            effects,
            raw_position_deadlines,
            pending_positions,
            pending_fetches,
            fetch_settings,
            fetch_decode_limits: FetchDecodeLimits::default(),
            fetch_attempt_timeout: FIRST_GROUP_FETCH_ATTEMPT_TIMEOUT,
            missing_offset_policy,
            read_isolation,
            partition_capacity: FIRST_GROUP_FETCH_PARTITIONS,
            effect_capacity: FIRST_GROUP_FETCH_EFFECTS,
            hard_fetch_output_bytes: FIRST_GROUP_FETCH_OUTPUT_BYTES,
            fault: None,
            seek: None,
            reclaim_faults,
            reclaim_overflow: None,
        })
    }
}
