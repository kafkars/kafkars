//! Linear ownership of one accepted coordinator-routed API-key 69 call.

use std::{error::Error, fmt};

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::ConsumerGroupDescribeResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::describe_consumer_groups::{
        ConsumerGroupDescribeRequestFailure, consumer_group_describe_request,
    },
};

use super::{
    super::DriverOwner,
    consumer_group_describe_submission::ConsumerGroupDescribeSubmitError,
    consumer_group_describe_terminal::{
        ConsumerGroupDescribeRawTerminal, RecoveredConsumerGroupDescribeCall,
        retain_consumer_group_describe_terminal,
    },
};

/// One accepted modern-group description call retained by its concrete host owner.
#[must_use = "an accepted ConsumerGroupDescribe call must be terminally settled"]
pub(crate) struct ConsumerGroupDescribeCall {
    call: Option<RoutedCall<ConsumerGroupDescribeResponse>>,
}

impl ConsumerGroupDescribeCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        group_id: &str,
        include_authorized_operations: bool,
        request_scratch_limit: usize,
        deadline: OperationDeadline,
    ) -> Result<Self, ConsumerGroupDescribeCallAdmissionFailure> {
        let request = consumer_group_describe_request(
            group_id,
            include_authorized_operations,
            request_scratch_limit,
        )
        .map_err(ConsumerGroupDescribeCallAdmissionFailure::Request)?;
        let call = driver
            .submit_tracked_consumer_group_describe(group_id, request, deadline)
            .map_err(ConsumerGroupDescribeCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ConsumerGroupDescribeRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_consumer_group_describe_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredConsumerGroupDescribeCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredConsumerGroupDescribeCall::new()
        })
    }
}

/// Definitely-unsent request-construction or tracked-driver rejection.
#[must_use = "a rejected ConsumerGroupDescribe call must become operation input"]
#[derive(Debug)]
pub(crate) enum ConsumerGroupDescribeCallAdmissionFailure {
    Request(ConsumerGroupDescribeRequestFailure),
    Driver(ConsumerGroupDescribeSubmitError),
}

impl fmt::Display for ConsumerGroupDescribeCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(source) => write!(
                formatter,
                "ConsumerGroupDescribe request rejected: {source:?}"
            ),
            Self::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for ConsumerGroupDescribeCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(_) => None,
            Self::Driver(source) => Some(source),
        }
    }
}
