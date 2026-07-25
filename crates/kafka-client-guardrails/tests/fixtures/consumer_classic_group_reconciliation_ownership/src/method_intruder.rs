//! Forbidden consumers of classic-group shutdown reconciliation receipts.

fn steal_join_reconciliation<T, U>(value: T, receipt: U) {
    value.reconcile_join_group_after_driver_shutdown(receipt);
}

fn steal_join_receipt<T>(value: T) {
    value.consume_join_group_shutdown_receipt();
}

fn steal_sync_reconciliation<T, U>(value: T, receipt: U) {
    value.reconcile_sync_group_after_driver_shutdown(receipt);
}

fn steal_sync_receipt<T>(value: T) {
    value.consume_sync_group_shutdown_receipt();
}
