//! Linear ownership of one accepted tracked AnyBroker topic-partition query.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::{DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse};

use super::{
    super::DriverOwner,
    describe_topic_partitions_terminal::{
        DescribeTopicPartitionsRawTerminal, RecoveredDescribeTopicPartitionsCall,
        retain_describe_topic_partitions_terminal,
    },
};

/// One accepted tracked driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeTopicPartitions call must be terminally settled"]
pub(crate) struct DescribeTopicPartitionsCall {
    call: Option<RoutedCall<DescribeTopicPartitionsResponse>>,
}

impl DescribeTopicPartitionsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        request: DescribeTopicPartitionsRequest,
        deadline: Instant,
    ) -> Result<Self, DescribeTopicPartitionsCallAdmissionFailure> {
        let call = driver
            .submit_tracked_describe_topic_partitions(request, deadline)
            .map_err(|_source| DescribeTopicPartitionsCallAdmissionFailure)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without releasing its route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeTopicPartitionsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_topic_partitions_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals unresolved ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredDescribeTopicPartitionsCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDescribeTopicPartitionsCall::new()
        })
    }
}

/// Definitely-unsent bounded-driver rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected DescribeTopicPartitions call must become operation input"]
pub(crate) struct DescribeTopicPartitionsCallAdmissionFailure;
