//! Exact single-ID generated `DescribeTransactions` request construction.

use kafka_wire::DescribeTransactionsRequest;

/// Builds the sole generated v0 request for one already-validated ID.
pub(crate) fn describe_transactions_request(transactional_id: &str) -> DescribeTransactionsRequest {
    let mut request = DescribeTransactionsRequest::default();
    request.transactional_ids = vec![transactional_id.into()];
    request
}
