//! Explicit joins from concrete admin admission into the embedded reactor wake.

use crate::{
    admin::{
        AlterConsumerGroupOffsetsShardWake, AlterConsumerGroupOffsetsShardWakeError,
        CreatePartitionsShardWake, CreatePartitionsShardWakeError, CreateTopicsShardWake,
        CreateTopicsShardWakeError, DeleteConsumerGroupOffsetsShardWake,
        DeleteConsumerGroupOffsetsShardWakeError, DeleteTopicsShardWake,
        DeleteTopicsShardWakeError, DescribeClusterShardWake, DescribeClusterShardWakeError,
        DescribeConfigsShardWake, DescribeConfigsShardWakeError, DescribeTopicsShardWake,
        DescribeTopicsShardWakeError, IncrementalAlterConfigsShardWake,
        IncrementalAlterConfigsShardWakeError, ListConsumerGroupOffsetsShardWake,
        ListConsumerGroupOffsetsShardWakeError,
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

impl DescribeClusterShardWake for ReactorWake {
    fn wake(&self) -> Result<(), DescribeClusterShardWakeError> {
        self.request()
            .map_err(|error| DescribeClusterShardWakeError::from_io(error.into_io()))
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
