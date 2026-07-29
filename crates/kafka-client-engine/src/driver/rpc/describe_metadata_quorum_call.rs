//! Linear ownership of one accepted tracked AnyBroker metadata-quorum query.

use std::time::Instant;

use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeQuorumResponse;

use crate::protocol::admin::describe_metadata_quorum::describe_metadata_quorum_request;

use super::{
    super::DriverOwner,
    describe_metadata_quorum_terminal::{
        DescribeMetadataQuorumRawTerminal, RecoveredDescribeMetadataQuorumCall,
        retain_describe_metadata_quorum_terminal,
    },
};

/// One accepted tracked driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeMetadataQuorum call must be terminally settled"]
pub(crate) struct DescribeMetadataQuorumCall {
    call: Option<RoutedCall<DescribeQuorumResponse>>,
}

impl DescribeMetadataQuorumCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        deadline: Instant,
    ) -> Result<Self, DescribeMetadataQuorumCallAdmissionFailure> {
        let request = describe_metadata_quorum_request();
        let call = driver
            .submit_describe_metadata_quorum(request, deadline)
            .map_err(|_source| DescribeMetadataQuorumCallAdmissionFailure)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts one ready raw terminal without blocking.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeMetadataQuorumRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_metadata_quorum_terminal(
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
        mut self,
    ) -> Option<RecoveredDescribeMetadataQuorumCall> {
        self.call.take().map(|call| {
            drop(call);
            RecoveredDescribeMetadataQuorumCall
        })
    }
}

/// Definitely-unsent bounded-driver rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "a rejected DescribeMetadataQuorum call must become operation input"]
pub(crate) struct DescribeMetadataQuorumCallAdmissionFailure;
