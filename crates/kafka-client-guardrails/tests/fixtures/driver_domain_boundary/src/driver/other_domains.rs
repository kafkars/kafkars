//! Deliberate grouped shared-driver dependencies on later domains.

use crate::{admin::AdminOwner, consumer::ConsumerOwner, transaction::TransactionOwner};

fn leak(
    admin: &AdminOwner,
    consumer: &ConsumerOwner,
    transaction: &TransactionOwner,
) {
    let _values = (admin, consumer, transaction);
}
