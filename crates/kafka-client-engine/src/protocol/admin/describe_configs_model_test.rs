//! Scenarios for wire-free normalized configuration facts.

use core::num::NonZeroI16;

use super::describe_configs_model::{NormalizedConfigResource, NormalizedConfigResourceError};

#[test]
fn scalar_model_preserves_unknown_signed_broker_code() {
    let Some(code) = NonZeroI16::new(-32_123) else {
        panic!("fixture code is non-zero");
    };
    let resource = NormalizedConfigResource {
        resource_type: 2,
        resource_name: "orders".to_owned(),
        outcome: Err(NormalizedConfigResourceError {
            code,
            message: Some("future broker error".to_owned()),
            message_truncated: false,
        }),
    };

    let Err(error) = resource.outcome else {
        panic!("fixture must remain a broker rejection");
    };
    assert_eq!(error.code.get(), -32_123);
    assert_eq!(error.message.as_deref(), Some("future broker error"));
    assert!(!error.message_truncated);
}
