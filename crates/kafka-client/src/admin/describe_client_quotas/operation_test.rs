//! Public client-quota operation shape tests.

use super::{DescribeClientQuotas, DescribeClientQuotasBuilder, DescribeClientQuotasResult};

fn assert_send<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn facade_values_remain_runtime_neutral() {
    assert_send_sync::<DescribeClientQuotasBuilder>();
    assert_send::<DescribeClientQuotas>();
    assert_send_sync::<DescribeClientQuotasResult>();
}
