//! Route and version bounds for Admin `AlterClientQuotas`.

use std::time::Instant;

use kafka_driver::{ApiVersion, Route, TrafficClass};

use super::alter_client_quotas_submission::{
    alter_client_quotas_options, alter_client_quotas_route,
};

#[test]
fn client_quota_alterations_use_interactive_any_broker_v0_through_v1() {
    assert_eq!(alter_client_quotas_route(), Route::AnyBroker);

    let options = alter_client_quotas_options(Instant::now());
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.minimum_version(), Some(ApiVersion::new(0)));
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(1)));
}
