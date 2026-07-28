//! Public-to-engine client-quota alteration translation tests.

use crate::admin::{
    ClientQuotaAlteration, ClientQuotaAlterationOperation, ClientQuotaEntity,
    ClientQuotaEntityComponent,
};

use super::{AlterClientQuotasAdminRequest, engine::Operation};

#[test]
fn entity_identity_operation_order_and_validate_only_translate_losslessly() {
    let request = AlterClientQuotasAdminRequest::new(vec![ClientQuotaAlteration::new(
        ClientQuotaEntity::new([
            ClientQuotaEntityComponent::new("user".to_owned(), Some("alice".to_owned())),
            ClientQuotaEntityComponent::new("client-id".to_owned(), None),
        ]),
        [
            ClientQuotaAlterationOperation::set("producer_byte_rate", 4096.5),
            ClientQuotaAlterationOperation::remove("consumer_byte_rate"),
        ],
    )])
    .with_validate_only(true)
    .into_engine();

    assert!(request.validate_only());
    assert_eq!(request.entries().len(), 1);
    assert_eq!(
        request.entries()[0].entity().components()[0].entity_type(),
        "client-id"
    );
    assert_eq!(
        request.entries()[0].operations()[0].key(),
        "producer_byte_rate"
    );
    assert_eq!(
        request.entries()[0].operations()[0],
        Operation::set("producer_byte_rate".to_owned(), 4096.5)
    );
    assert_eq!(
        request.entries()[0].operations()[1],
        Operation::remove("consumer_byte_rate".to_owned())
    );
}

#[test]
fn malformed_shapes_remain_inert_until_engine_submission() {
    let request = AlterClientQuotasAdminRequest::new(vec![ClientQuotaAlteration::new(
        ClientQuotaEntity::new([]),
        [ClientQuotaAlterationOperation::set("", f64::INFINITY)],
    )])
    .into_engine();

    assert_eq!(request.entries().len(), 1);
}
