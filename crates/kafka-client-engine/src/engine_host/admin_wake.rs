//! Explicit join between `CreateTopics` admission and the embedded reactor wake.

use crate::{
    admin::{
        CreatePartitionsShardWake, CreatePartitionsShardWakeError, CreateTopicsShardWake,
        CreateTopicsShardWakeError, DeleteTopicsShardWake, DeleteTopicsShardWakeError,
        DescribeClusterShardWake, DescribeClusterShardWakeError, DescribeTopicsShardWake,
        DescribeTopicsShardWakeError,
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
