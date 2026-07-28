//! Terminal value scenarios for deterministic Admin `AlterClientQuotas`.

use core::num::NonZeroI16;

use super::{
    AlterClientQuotaBrokerError, AlterClientQuotaEntity, AlterClientQuotaEntityComponent,
    AlterClientQuotaOutcome, AlterClientQuotaResult, AlterClientQuotasBatch,
};

#[test]
fn batch_retains_throttle_nullable_identity_and_exact_signed_error() {
    let code = NonZeroI16::new(-31_234).unwrap_or_else(|| panic!("code is nonzero"));
    let batch = AlterClientQuotasBatch::new(
        91,
        vec![
            AlterClientQuotaOutcome::altered(entity("user", None)),
            AlterClientQuotaOutcome::failed(
                entity("client-id", Some("app")),
                AlterClientQuotaBrokerError::new(
                    code,
                    Some("future broker error".to_owned()),
                    true,
                ),
            ),
        ],
    );

    assert_eq!(batch.throttle_time_ms(), 91);
    assert_eq!(
        batch.outcomes()[0].entity().components()[0].entity_name(),
        None
    );
    let AlterClientQuotaResult::Failed(error) = batch.outcomes()[1].result() else {
        panic!("second entity must retain failure");
    };
    assert_eq!(error.code(), -31_234);
    assert_eq!(error.message(), Some("future broker error"));
    assert!(error.message_truncated());
}

fn entity(entity_type: &str, entity_name: Option<&str>) -> AlterClientQuotaEntity {
    AlterClientQuotaEntity::new(vec![AlterClientQuotaEntityComponent::new(
        entity_type.to_owned(),
        entity_name.map(str::to_owned),
    )])
}
