//! Classic-group leave driver failure classification scenarios.

use kafka_driver::{
    AuthenticationFailure, CallFailure, ConnectionCloseReason, Delivery, RequestError,
};

use super::classic_group_leave_failure::{
    ClassicGroupLeaveDriverFailureKind, classify_request_error,
};

#[test]
fn authentication_failure_remains_distinct_from_transport() {
    let error = RequestError::Rejected {
        failure: CallFailure::ConnectionClosed {
            reason: ConnectionCloseReason::AuthenticationFailed(AuthenticationFailure::Rejected),
        },
        delivery: Delivery::NotSent,
    };

    assert_eq!(
        classify_request_error(&error),
        ClassicGroupLeaveDriverFailureKind::Authentication
    );
}
