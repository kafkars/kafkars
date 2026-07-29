//! Public token-creation result ownership and throttle scenarios.

use std::time::Duration;

use super::{
    CreateDelegationTokenResult, DelegationToken, DelegationTokenHmac, DelegationTokenPrincipal,
};

#[test]
fn result_retains_throttle_and_one_unique_token() {
    let result = CreateDelegationTokenResult::new(
        Duration::from_millis(17),
        DelegationToken::new(
            DelegationTokenPrincipal::new("User", "alice"),
            None,
            Vec::new(),
            1,
            2,
            3,
            "token-7".to_owned(),
            DelegationTokenHmac::new(vec![1, 2, 3]),
        ),
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(result.token().token_id(), "token-7");
    assert_eq!(result.into_token().hmac().as_bytes(), [1, 2, 3]);
}
