//! Transaction initialization call ownership shape.

use super::TransactionInitCall;

#[test]
fn call_owner_is_linear_at_the_adapter_boundary() {
    fn consume(_call: TransactionInitCall) {}
    let _: fn(TransactionInitCall) = consume;
}
