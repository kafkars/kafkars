//! Public client-quota alteration operation shape tests.

use super::{AlterClientQuotas, AlterClientQuotasBuilder, AlterClientQuotasResult};

fn assert_send<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn facade_values_remain_runtime_neutral() {
    assert_send_sync::<AlterClientQuotasBuilder>();
    assert_send::<AlterClientQuotas>();
    assert_send_sync::<AlterClientQuotasResult>();
}
