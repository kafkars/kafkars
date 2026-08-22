//! Declarative boundary for one-partition Fetch RPC ownership.

mod admission;
#[cfg(test)]
mod admission_test;
mod broker_admission;
#[cfg(test)]
mod broker_admission_test;
mod broker_calls;
mod broker_calls_helpers;
#[cfg(test)]
mod broker_calls_loopback_test;
mod broker_calls_response;
mod broker_calls_settlement;
#[cfg(test)]
mod broker_calls_test;
#[cfg(test)]
mod broker_calls_test_support;
mod broker_close;
#[cfg(test)]
mod broker_close_test;
mod calls;
#[cfg(test)]
mod calls_test;
mod failure;
#[cfg(test)]
mod failure_admission_test;
#[cfg(test)]
mod failure_driver_test;
mod failure_wire;
#[cfg(test)]
mod failure_wire_decode_test;
#[cfg(test)]
mod failure_wire_encode_test;
mod fence;
#[cfg(test)]
mod fence_test;
mod forgotten;
#[cfg(test)]
mod forgotten_test;
mod legacy_request;
mod partition_submission;
mod route;
mod route_correlation;
mod route_refresh;
#[cfg(test)]
mod route_test;
#[cfg(test)]
pub(in crate::driver::rpc) mod routed_response_broker_test;
#[cfg(test)]
mod routed_response_frame_test;
#[cfg(test)]
mod routed_response_test;
mod settlement;
mod settlement_owner;
#[cfg(test)]
mod settlement_owner_test;
#[cfg(test)]
mod settlement_test;
mod stale;
#[cfg(test)]
mod stale_test;
mod submission;
#[cfg(test)]
mod submission_test;
mod terminal;
#[cfg(test)]
mod terminal_test;
mod topic_route;

pub(crate) use admission::{FetchCallAdmission, PartitionFetchRequest};
pub(crate) use broker_calls::{BrokerFetchCallAdmission, TrackedBrokerFetchCalls};
pub(crate) use broker_close::BrokerFetchCloseCall;
pub(crate) use calls::TrackedFetchCalls;
pub(crate) use failure::{classify_fetch_admission, classify_fetch_request_error};
pub(crate) use forgotten::{
    ForgottenFetchCompletionFailure, ForgottenFetchConfirmation, ForgottenFetchRequest,
    ForgottenFetchSubmitFailureKind, ForgottenFetchTerminal, TrackedForgottenFetchCall,
};
pub(crate) use route::{BrokerFetchRouteCall, BrokerFetchRouteFailureKind, BrokerId};
pub(crate) use route_refresh::{FetchRouteRefresh, FetchRouteRefreshPoll};
pub(crate) use settlement::{
    FetchBeginSettlementError, FetchConfirmationError, FetchPoll, StaleFetchConfirmationError,
};
pub(crate) use stale::{FetchControlPending, FetchRecovery};
pub(crate) use terminal::{FetchCompletionObservation, FetchTerminal};
