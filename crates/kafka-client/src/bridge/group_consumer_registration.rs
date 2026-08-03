//! Bounded registration and membership-start translation for one hosted group.

use std::{sync::Arc, time::Duration};

use kafka_client_engine::{
    Engine as SharedEngine, GroupConsumerClassicAssignor as EngineGroupConsumerClassicAssignor,
    GroupConsumerDormantReleaseErrorKind, GroupConsumerMissingOffsetPolicy,
    GroupConsumerProtocol as EngineGroupConsumerProtocol,
    GroupConsumerRegistration as EngineGroupConsumerRegistration, GroupConsumerStartCapture,
};

use super::group_consumer_registration_result::{
    accepted_fault, translate_group_registration, translate_group_start,
};
use super::{super::client::engine_read_isolation, group_consumer::GroupConsumerEngine};
use crate::{
    ClassicGroupAssignor, ConsumerGroupProtocol, ErrorKind, KafkaError, OffsetReset, ReadIsolation,
};

impl GroupConsumerEngine {
    pub(crate) fn register(
        engine: &SharedEngine,
        capture: GroupConsumerStartCapture,
        group: &str,
        group_instance_id: Option<&str>,
        topics: &[String],
        group_protocol: ConsumerGroupProtocol,
        classic_group_assignor: Option<ClassicGroupAssignor>,
        offset_reset: OffsetReset,
        read_isolation: ReadIsolation,
        processing_timeout: Duration,
    ) -> Result<Self, KafkaError> {
        let mut registration = EngineGroupConsumerRegistration::new(
            Arc::<str>::from(group),
            topics
                .iter()
                .map(|topic| Arc::<str>::from(topic.as_str()))
                .collect(),
        );
        if let Some(group_instance_id) = group_instance_id {
            registration = registration.with_group_instance_id(Arc::<str>::from(group_instance_id));
        }
        let registration = registration.with_protocol(engine_group_protocol(group_protocol));
        let registration = match classic_group_assignor {
            Some(assignor) => registration.with_classic_assignor(engine_classic_assignor(assignor)),
            None => registration,
        };
        let registration = registration
            .with_missing_offset_policy(engine_missing_offset_policy(offset_reset))
            .with_read_isolation(engine_read_isolation(read_isolation))
            .with_processing_timeout(processing_timeout);
        let mut handle = engine
            .register_group_consumer(registration)
            .map_err(|error| translate_group_registration(&error))?;
        match handle.try_start_captured(capture) {
            Ok(accepted) => Ok(Self {
                handle,
                startup_fault: accepted_fault(accepted),
            }),
            Err(error) => {
                let start_error = translate_group_start(error);
                match handle.release_dormant() {
                    Ok(()) => Err(start_error),
                    Err(release)
                        if matches!(
                            release.kind(),
                            GroupConsumerDormantReleaseErrorKind::Closed
                                | GroupConsumerDormantReleaseErrorKind::GroupUnavailable
                        ) =>
                    {
                        Err(start_error)
                    }
                    Err(release) => Ok(Self {
                        handle: release.into_handle(),
                        startup_fault: Some(KafkaError::new(
                            ErrorKind::Internal,
                            "group membership rejected but dormant rollback could not complete",
                        )),
                    }),
                }
            }
        }
    }
}

const fn engine_classic_assignor(
    assignor: ClassicGroupAssignor,
) -> EngineGroupConsumerClassicAssignor {
    match assignor {
        ClassicGroupAssignor::Range => EngineGroupConsumerClassicAssignor::Range,
        ClassicGroupAssignor::CooperativeSticky => {
            EngineGroupConsumerClassicAssignor::CooperativeSticky
        }
    }
}

const fn engine_group_protocol(protocol: ConsumerGroupProtocol) -> EngineGroupConsumerProtocol {
    match protocol {
        ConsumerGroupProtocol::Classic => EngineGroupConsumerProtocol::Classic,
        ConsumerGroupProtocol::Consumer => EngineGroupConsumerProtocol::Consumer,
    }
}

pub(super) const fn engine_missing_offset_policy(
    policy: OffsetReset,
) -> GroupConsumerMissingOffsetPolicy {
    match policy {
        OffsetReset::Error => GroupConsumerMissingOffsetPolicy::Error,
        OffsetReset::Earliest => GroupConsumerMissingOffsetPolicy::Earliest,
        OffsetReset::Latest => GroupConsumerMissingOffsetPolicy::Latest,
    }
}
