//! Wire-free core terminals produced by one normalized API-89 response.

use kafka_client_core::{DescribeStreamsGroupBrokerError, DescribeStreamsGroupResult};

/// Exact result for the one coordinator-correlated streams group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NormalizedDescribeStreamsGroupResult {
    Described(DescribeStreamsGroupResult),
    Failed(DescribeStreamsGroupBrokerError),
}
