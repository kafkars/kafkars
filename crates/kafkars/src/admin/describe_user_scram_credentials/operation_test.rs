//! Public SCRAM credential-description operation shape tests.

use super::{
    DescribeUserScramCredentials, DescribeUserScramCredentialsBuilder,
    DescribeUserScramCredentialsResult,
};

fn assert_send<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn facade_values_remain_runtime_neutral() {
    assert_send_sync::<DescribeUserScramCredentialsBuilder>();
    assert_send::<DescribeUserScramCredentials>();
    assert_send_sync::<DescribeUserScramCredentialsResult>();
}
