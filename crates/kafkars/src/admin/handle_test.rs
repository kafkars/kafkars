//! Public admin thread-safety and builder scenarios.

use super::{Admin, DescribeUserScramCredentialsBuilder, FeatureUpdate, UpdateFeaturesBuilder};

#[test]
fn admin_is_a_shared_thread_safe_handle() {
    fn assert_shared<T: Clone + Send + Sync>() {}
    assert_shared::<Admin>();
}

#[test]
fn scram_description_starts_as_an_all_user_inert_builder() {
    let method: fn(&Admin) -> DescribeUserScramCredentialsBuilder =
        Admin::describe_user_scram_credentials;

    let _ = method;
}

#[test]
fn finalized_feature_updates_start_as_an_inert_builder() {
    let method: fn(&Admin, Vec<FeatureUpdate>) -> UpdateFeaturesBuilder = Admin::update_features;

    let _ = method;
}
