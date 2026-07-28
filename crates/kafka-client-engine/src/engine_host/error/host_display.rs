//! Human-readable diagnostics retain the concrete failed engine-host owner.

use std::fmt;

use super::host::EngineHostError;

impl fmt::Display for EngineHostError {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clock(error) => write!(formatter, "engine clock failed: {error}"),
            Self::Producer(error) => write!(formatter, "producer host failed: {error}"),
            Self::ProducerHandoff(error) => {
                write!(formatter, "prepared Produce handoff failed: {error}")
            }
            Self::ProducerIdentityHandoff(error) => {
                write!(formatter, "producer identity handoff failed: {error}")
            }
            Self::ProduceCompletion(error) => write!(formatter, "{error}"),
            Self::ProducerIdentityCompletion(error) => write!(formatter, "{error}"),
            Self::ProducerStop(error) => write!(formatter, "producer recovery failed: {error}"),
            Self::ProducerCleanup(error) => {
                write!(formatter, "producer terminal cleanup failed: {error}")
            }
            Self::ProducerLockPoisoned => {
                formatter.write_str("producer host ownership lock is poisoned")
            }
            Self::AssignedConsumer(error) => {
                write!(formatter, "assigned-consumer owner failed: {error:?}")
            }
            Self::AssignedConsumerFault(fault) => {
                write!(formatter, "assigned-consumer owner faulted: {fault:?}")
            }
            Self::AssignedConsumerLockPoisoned => {
                formatter.write_str("assigned-consumer ownership lock is poisoned")
            }
            Self::AssignedConsumerOwnerMissing => {
                formatter.write_str("assigned-consumer owner is unavailable")
            }
            Self::AssignedConsumerCloseIncomplete => {
                formatter.write_str("assigned-consumer close terminal is not retained")
            }
            Self::AssignedConsumerUnsettled(count) => {
                write!(
                    formatter,
                    "{count} assigned-consumer work items remain retained"
                )
            }
            Self::AssignedConsumerRecovery(recovery) => {
                write!(
                    formatter,
                    "assigned-consumer recovery observed {recovery:?}"
                )
            }
            Self::AssignedConsumerCompletion(error) => {
                write!(
                    formatter,
                    "assigned-consumer completion notifier failed: {error}"
                )
            }
            Self::GroupConsumer(error) => {
                write!(formatter, "group-consumer registry failed: {error}")
            }
            Self::GroupConsumerLockPoisoned => {
                formatter.write_str("group-consumer registry ownership lock is poisoned")
            }
            Self::GroupConsumerRecvNotifierUnavailable => {
                formatter.write_str("group-consumer receive notifier is unavailable")
            }
            Self::TransactionInitialization(error) => {
                write!(formatter, "transaction initialization failed: {error}")
            }
            Self::TransactionInitializationLockPoisoned => {
                formatter.write_str("transaction initialization ownership lock is poisoned")
            }
            Self::CreateTopics(error) => write!(formatter, "CreateTopics host failed: {error}"),
            Self::CreateTopicsCompletion(error) => write!(formatter, "{error}"),
            Self::CreateTopicsLockPoisoned => {
                formatter.write_str("CreateTopics host ownership lock is poisoned")
            }
            Self::DeleteTopics(error) => write!(formatter, "DeleteTopics host failed: {error}"),
            Self::DeleteTopicsCompletion(error) => write!(formatter, "{error}"),
            Self::DeleteTopicsLockPoisoned => {
                formatter.write_str("DeleteTopics host ownership lock is poisoned")
            }
            Self::DescribeAcls(error) => {
                write!(formatter, "DescribeAcls host failed: {error}")
            }
            Self::DescribeAclsLockPoisoned => {
                formatter.write_str("DescribeAcls host ownership lock is poisoned")
            }
            Self::DescribeClientQuotas(error) => {
                write!(formatter, "DescribeClientQuotas host failed: {error}")
            }
            Self::DescribeClientQuotasLockPoisoned => {
                formatter.write_str("DescribeClientQuotas host ownership lock is poisoned")
            }
            Self::AlterClientQuotas(error) => {
                write!(formatter, "AlterClientQuotas host failed: {error}")
            }
            Self::AlterClientQuotasLockPoisoned => {
                formatter.write_str("AlterClientQuotas host ownership lock is poisoned")
            }
            Self::DescribeUserScramCredentials(error) => {
                write!(
                    formatter,
                    "DescribeUserScramCredentials host failed: {error}"
                )
            }
            Self::DescribeUserScramCredentialsLockPoisoned => {
                formatter.write_str("DescribeUserScramCredentials host ownership lock is poisoned")
            }
            Self::CreateAcls(error) => {
                write!(formatter, "CreateAcls host failed: {error}")
            }
            Self::CreateAclsLockPoisoned => {
                formatter.write_str("CreateAcls host ownership lock is poisoned")
            }
            Self::DeleteAcls(error) => {
                write!(formatter, "DeleteAcls host failed: {error}")
            }
            Self::DeleteAclsLockPoisoned => {
                formatter.write_str("DeleteAcls host ownership lock is poisoned")
            }
            Self::DescribeCluster(error) => {
                write!(formatter, "DescribeCluster host failed: {error}")
            }
            Self::DescribeClusterCompletion(error) => write!(formatter, "{error}"),
            Self::DescribeClusterLockPoisoned => {
                formatter.write_str("DescribeCluster host ownership lock is poisoned")
            }
            Self::DescribeConsumerGroups(error) => {
                write!(formatter, "DescribeConsumerGroups host failed: {error}")
            }
            Self::DescribeConsumerGroupsLockPoisoned => {
                formatter.write_str("DescribeConsumerGroups host ownership lock is poisoned")
            }
            Self::DescribeLogDirs(error) => {
                write!(formatter, "DescribeLogDirs host failed: {error}")
            }
            Self::DescribeLogDirsLockPoisoned => {
                formatter.write_str("DescribeLogDirs host ownership lock is poisoned")
            }
            Self::AlterReplicaLogDirs(error) => {
                write!(formatter, "AlterReplicaLogDirs host failed: {error}")
            }
            Self::AlterReplicaLogDirsLockPoisoned => {
                formatter.write_str("AlterReplicaLogDirs host ownership lock is poisoned")
            }
            Self::ListConsumerGroups(error) => {
                write!(formatter, "ListConsumerGroups host failed: {error}")
            }
            Self::ListConsumerGroupsLockPoisoned => {
                formatter.write_str("ListConsumerGroups host ownership lock is poisoned")
            }
            Self::CreatePartitions(error) => {
                write!(formatter, "CreatePartitions host failed: {error}")
            }
            Self::CreatePartitionsCompletion(error) => write!(formatter, "{error}"),
            Self::CreatePartitionsLockPoisoned => {
                formatter.write_str("CreatePartitions host ownership lock is poisoned")
            }
            Self::DescribeTopics(error) => write!(formatter, "DescribeTopics host failed: {error}"),
            Self::DescribeTopicsCompletion(error) => write!(formatter, "{error}"),
            Self::DescribeTopicsLockPoisoned => {
                formatter.write_str("DescribeTopics host ownership lock is poisoned")
            }
            Self::DescribeConfigs(error) => {
                write!(formatter, "DescribeConfigs host failed: {error}")
            }
            Self::DescribeConfigsCompletion(error) => write!(formatter, "{error}"),
            Self::DescribeConfigsLockPoisoned => {
                formatter.write_str("DescribeConfigs host ownership lock is poisoned")
            }
            Self::IncrementalAlterConfigs(error) => {
                write!(formatter, "IncrementalAlterConfigs host failed: {error}")
            }
            Self::IncrementalAlterConfigsCompletion(error) => write!(formatter, "{error}"),
            Self::IncrementalAlterConfigsLockPoisoned => {
                formatter.write_str("IncrementalAlterConfigs host ownership lock is poisoned")
            }
            Self::ListConsumerGroupOffsets(error) => {
                write!(formatter, "ListConsumerGroupOffsets host failed: {error}")
            }
            Self::ListConsumerGroupOffsetsLockPoisoned => {
                formatter.write_str("ListConsumerGroupOffsets host ownership lock is poisoned")
            }
            Self::DeleteConsumerGroupOffsets(error) => {
                write!(formatter, "DeleteConsumerGroupOffsets host failed: {error}")
            }
            Self::DeleteConsumerGroupOffsetsLockPoisoned => {
                formatter.write_str("DeleteConsumerGroupOffsets host ownership lock is poisoned")
            }
            Self::AlterConsumerGroupOffsets(error) => {
                write!(formatter, "AlterConsumerGroupOffsets host failed: {error}")
            }
            Self::AlterConsumerGroupOffsetsLockPoisoned => {
                formatter.write_str("AlterConsumerGroupOffsets host ownership lock is poisoned")
            }
            Self::AdminListOffsets(error) => {
                write!(formatter, "Admin ListOffsets host failed: {error}")
            }
            Self::AdminListOffsetsLockPoisoned => {
                formatter.write_str("Admin ListOffsets host ownership lock is poisoned")
            }
            Self::ListPartitionReassignments(error) => {
                write!(formatter, "ListPartitionReassignments host failed: {error}")
            }
            Self::ListPartitionReassignmentsLockPoisoned => {
                formatter.write_str("ListPartitionReassignments host ownership lock is poisoned")
            }
            Self::AlterPartitionReassignments(error) => {
                write!(
                    formatter,
                    "AlterPartitionReassignments host failed: {error}"
                )
            }
            Self::AlterPartitionReassignmentsLockPoisoned => {
                formatter.write_str("AlterPartitionReassignments host ownership lock is poisoned")
            }
            Self::ElectLeaders(error) => {
                write!(formatter, "ElectLeaders host failed: {error}")
            }
            Self::ElectLeadersLockPoisoned => {
                formatter.write_str("ElectLeaders host ownership lock is poisoned")
            }
            Self::RemoveConsumerGroupMembers(error) => {
                write!(formatter, "RemoveConsumerGroupMembers host failed: {error}")
            }
            Self::RemoveConsumerGroupMembersLockPoisoned => {
                formatter.write_str("RemoveConsumerGroupMembers host ownership lock is poisoned")
            }
            Self::DeleteRecords(error) => {
                write!(formatter, "DeleteRecords host failed: {error}")
            }
            Self::DeleteRecordsLockPoisoned => {
                formatter.write_str("DeleteRecords host ownership lock is poisoned")
            }
            Self::DeleteConsumerGroups(error) => {
                write!(formatter, "DeleteConsumerGroups host failed: {error}")
            }
            Self::DeleteConsumerGroupsLockPoisoned => {
                formatter.write_str("DeleteConsumerGroups host ownership lock is poisoned")
            }
            Self::AdminCompletion(error) => {
                write!(
                    formatter,
                    "shared admin completion notifier failed: {error}"
                )
            }
            Self::Driver(error) => write!(formatter, "embedded driver failed: {error}"),
            Self::DriverOwnerMissing => formatter.write_str("embedded driver owner is unavailable"),
            Self::DriverStopped => formatter.write_str("embedded driver stopped unexpectedly"),
            Self::TrackedProduceCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked Produce calls remain at terminal cleanup"
                )
            }
            Self::TrackedProducerIdentityCallsRemain(count) => write!(
                formatter,
                "{count} tracked producer identity calls remain at terminal cleanup"
            ),
            Self::TrackedCreateTopicsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked CreateTopics calls remain at terminal cleanup"
                )
            }
            Self::TrackedDeleteTopicsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked DeleteTopics calls remain at terminal cleanup"
                )
            }
            Self::DescribeClusterCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} DescribeCluster calls remain at terminal cleanup"
                )
            }
            Self::TrackedCreatePartitionsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} tracked CreatePartitions calls remain at terminal cleanup"
                )
            }
            Self::DescribeTopicsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} DescribeTopics calls remain at terminal cleanup"
                )
            }
            Self::DescribeConfigsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} DescribeConfigs calls remain at terminal cleanup"
                )
            }
            Self::IncrementalAlterConfigsCallsRemain(count) => {
                write!(
                    formatter,
                    "{count} IncrementalAlterConfigs calls remain at terminal cleanup"
                )
            }
            Self::HostPanicked => formatter.write_str("engine host thread panicked"),
            Self::Notifier(error) => write!(formatter, "completion notifier failed: {error}"),
            Self::Recovery { primary, cleanup } => {
                write!(
                    formatter,
                    "{primary}; terminal cleanup also failed: {cleanup}"
                )
            }
            #[cfg(test)]
            Self::ForcedTestFailure => formatter.write_str("forced engine host test failure"),
        }
    }
}
