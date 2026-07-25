//! Private topic-only `IncrementalAlterConfigs` request bridge scenarios.

use kafka_client_engine::{
    IncrementalAlterConfigsRequest as EngineRequest,
    IncrementalConfigAlteration as EngineAlteration, IncrementalConfigOperation as EngineOperation,
    TopicConfigAlterations as EngineTopicAlterations,
};

use super::admin_alter_configs_request::IncrementalAlterConfigsAdminRequest;
use crate::{ConfigAlteration, TopicConfigAlterations};

#[test]
fn request_bridge_is_send_and_preserves_delete_vs_explicit_empty_value() {
    fn assert_send<T: Send>() {}
    assert_send::<IncrementalAlterConfigsAdminRequest>();

    let request = IncrementalAlterConfigsAdminRequest::from_topics([TopicConfigAlterations::new(
        "orders",
        [
            ConfigAlteration::set("cleanup.policy", ""),
            ConfigAlteration::delete("retention.ms"),
            ConfigAlteration::append("compression.type", "zstd"),
            ConfigAlteration::subtract("cleanup.policy", "delete"),
        ],
    )])
    .with_validate_only(true);
    assert!(format!("{request:?}").starts_with("IncrementalAlterConfigsAdminRequest"));
    assert_eq!(
        request.into_engine(),
        EngineRequest::new(vec![EngineTopicAlterations::new(
            "orders".to_owned(),
            vec![
                EngineAlteration::new(
                    "cleanup.policy".to_owned(),
                    EngineOperation::Set(String::new()),
                ),
                EngineAlteration::new("retention.ms".to_owned(), EngineOperation::Delete,),
                EngineAlteration::new(
                    "compression.type".to_owned(),
                    EngineOperation::Append("zstd".to_owned()),
                ),
                EngineAlteration::new(
                    "cleanup.policy".to_owned(),
                    EngineOperation::Subtract("delete".to_owned()),
                ),
            ],
        )])
        .with_validate_only(true)
    );
}
