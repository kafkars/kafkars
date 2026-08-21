//! Stable client-quota alteration ownership tests.

use super::{ClientQuotaAlteration, ClientQuotaAlterationOperation, ClientQuotaEntity};

#[test]
fn set_remove_and_caller_order_remain_exact() {
    let alteration = ClientQuotaAlteration::new(
        ClientQuotaEntity::new([]),
        [
            ClientQuotaAlterationOperation::set("producer_byte_rate", 8192.5),
            ClientQuotaAlterationOperation::remove("consumer_byte_rate"),
        ],
    );

    assert_eq!(alteration.operations()[0].key(), "producer_byte_rate");
    assert_eq!(alteration.operations()[0].value(), Some(8192.5));
    assert_eq!(alteration.operations()[1].key(), "consumer_byte_rate");
    assert_eq!(alteration.operations()[1].value(), None);
}

#[test]
fn construction_defers_empty_duplicate_and_non_finite_validation() {
    let empty = ClientQuotaAlteration::new(ClientQuotaEntity::new([]), []);
    assert!(empty.operations().is_empty());

    let non_finite = ClientQuotaAlteration::new(
        ClientQuotaEntity::new([]),
        [
            ClientQuotaAlterationOperation::set("", f64::NAN),
            ClientQuotaAlterationOperation::remove(""),
        ],
    );
    assert_eq!(non_finite.operations().len(), 2);
}
