//! Explicit joins from concrete admin admission into the embedded reactor wake.

use crate::{
    admin::{
        AdminDescribeProducersShardWake, AdminDescribeProducersShardWakeError,
        AdminDescribeTransactionsShardWake, AdminDescribeTransactionsShardWakeError,
        AdminFenceProducersShardWake, AdminFenceProducersShardWakeError, AdminListOffsetsShardWake,
        AdminListOffsetsShardWakeError, AlterConsumerGroupOffsetsShardWake,
        AlterConsumerGroupOffsetsShardWakeError, AlterPartitionReassignmentsShardWake,
        AlterPartitionReassignmentsShardWakeError, CreatePartitionsShardWake,
        CreatePartitionsShardWakeError, CreateTopicsShardWake, CreateTopicsShardWakeError,
        DeleteConsumerGroupOffsetsShardWake, DeleteConsumerGroupOffsetsShardWakeError,
        DeleteConsumerGroupsShardWake, DeleteConsumerGroupsShardWakeError, DeleteRecordsShardWake,
        DeleteRecordsShardWakeError, DeleteTopicsShardWake, DeleteTopicsShardWakeError,
        DescribeClusterShardWake, DescribeClusterShardWakeError, DescribeConfigsShardWake,
        DescribeConfigsShardWakeError, DescribeConsumerGroupsShardWake,
        DescribeConsumerGroupsShardWakeError, DescribeTopicsShardWake,
        DescribeTopicsShardWakeError, ElectLeadersShardWake, ElectLeadersShardWakeError,
        IncrementalAlterConfigsShardWake, IncrementalAlterConfigsShardWakeError,
        ListConsumerGroupOffsetsShardWake, ListConsumerGroupOffsetsShardWakeError,
        ListPartitionReassignmentsShardWake, ListPartitionReassignmentsShardWakeError,
        RemoveConsumerGroupMembersShardWake, RemoveConsumerGroupMembersShardWakeError,
    },
    driver::ReactorWake,
};

impl CreateTopicsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), CreateTopicsShardWakeError> {
        self.request()
            .map_err(|error| CreateTopicsShardWakeError::from_io(error.into_io()))
    }
}

impl DeleteTopicsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DeleteTopicsShardWakeError> {
        self.request()
            .map_err(|error| DeleteTopicsShardWakeError::from_io(error.into_io()))
    }
}

impl DeleteRecordsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DeleteRecordsShardWakeError> {
        self.request()
            .map_err(|error| DeleteRecordsShardWakeError::from_io(error.into_io()))
    }
}

impl DeleteConsumerGroupsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DeleteConsumerGroupsShardWakeError> {
        self.request()
            .map_err(|error| DeleteConsumerGroupsShardWakeError::from_io(error.into_io()))
    }
}

impl DescribeClusterShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeClusterShardWakeError> {
        self.request()
            .map_err(|error| DescribeClusterShardWakeError::from_io(error.into_io()))
    }
}

impl DescribeConsumerGroupsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeConsumerGroupsShardWakeError> {
        self.request()
            .map_err(|error| DescribeConsumerGroupsShardWakeError::from_io(error.into_io()))
    }
}

impl CreatePartitionsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), CreatePartitionsShardWakeError> {
        self.request()
            .map_err(|error| CreatePartitionsShardWakeError::from_io(error.into_io()))
    }
}

impl DescribeTopicsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeTopicsShardWakeError> {
        self.request()
            .map_err(|error| DescribeTopicsShardWakeError::from_io(error.into_io()))
    }
}

impl DescribeConfigsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeConfigsShardWakeError> {
        self.request()
            .map_err(|error| DescribeConfigsShardWakeError::from_io(error.into_io()))
    }
}

impl IncrementalAlterConfigsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), IncrementalAlterConfigsShardWakeError> {
        self.request()
            .map_err(|error| IncrementalAlterConfigsShardWakeError::from_io(error.into_io()))
    }
}

impl ListConsumerGroupOffsetsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), ListConsumerGroupOffsetsShardWakeError> {
        self.request()
            .map_err(|error| ListConsumerGroupOffsetsShardWakeError::from_io(error.into_io()))
    }
}

impl DeleteConsumerGroupOffsetsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DeleteConsumerGroupOffsetsShardWakeError> {
        self.request()
            .map_err(|error| DeleteConsumerGroupOffsetsShardWakeError::from_io(error.into_io()))
    }
}

impl AlterConsumerGroupOffsetsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AlterConsumerGroupOffsetsShardWakeError> {
        self.request()
            .map_err(|error| AlterConsumerGroupOffsetsShardWakeError::from_io(error.into_io()))
    }
}

impl AdminListOffsetsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AdminListOffsetsShardWakeError> {
        self.request()
            .map_err(|error| AdminListOffsetsShardWakeError::from_io(error.into_io()))
    }
}

impl AdminDescribeProducersShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AdminDescribeProducersShardWakeError> {
        self.request()
            .map_err(|error| AdminDescribeProducersShardWakeError::from_io(error.into_io()))
    }
}

impl AdminDescribeTransactionsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AdminDescribeTransactionsShardWakeError> {
        self.request()
            .map_err(|error| AdminDescribeTransactionsShardWakeError::from_io(error.into_io()))
    }
}

impl AdminFenceProducersShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AdminFenceProducersShardWakeError> {
        self.request()
            .map_err(|error| AdminFenceProducersShardWakeError::from_io(error.into_io()))
    }
}

impl ListPartitionReassignmentsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), ListPartitionReassignmentsShardWakeError> {
        self.request()
            .map_err(|error| ListPartitionReassignmentsShardWakeError::from_io(error.into_io()))
    }
}

impl AlterPartitionReassignmentsShardWake for ReactorWake {
    fn wake(&self) -> Result<(), AlterPartitionReassignmentsShardWakeError> {
        self.request()
            .map_err(|error| AlterPartitionReassignmentsShardWakeError::from_io(error.into_io()))
    }
}

impl ElectLeadersShardWake for ReactorWake {
    fn wake(&self) -> Result<(), ElectLeadersShardWakeError> {
        self.request()
            .map_err(|error| ElectLeadersShardWakeError::from_io(error.into_io()))
    }
}

impl RemoveConsumerGroupMembersShardWake for ReactorWake {
    fn wake(&self) -> Result<(), RemoveConsumerGroupMembersShardWakeError> {
        self.request()
            .map_err(|error| RemoveConsumerGroupMembersShardWakeError::from_io(error.into_io()))
    }
}
