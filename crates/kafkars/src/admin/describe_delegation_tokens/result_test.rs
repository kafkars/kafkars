//! Complete secret-bearing result ownership evidence.

use std::time::Duration;

use crate::admin::DelegationTokenHmac;

use super::{DelegationToken, DelegationTokenPrincipal, DescribeDelegationTokensResult};

#[test]
fn result_retains_order_and_redacts_every_secret() {
    let result = DescribeDelegationTokensResult::new(
        Duration::from_millis(17),
        vec![DelegationToken::new(
            DelegationTokenPrincipal::new("User", "alice"),
            None,
            Vec::new(),
            1,
            2,
            3,
            String::from("token-a"),
            DelegationTokenHmac::new(b"described-secret-must-not-leak".to_vec()),
        )],
    );

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(result.tokens()[0].token_id(), "token-a");
    assert_eq!(
        result.tokens()[0].hmac().as_bytes(),
        b"described-secret-must-not-leak"
    );
    let debug = format!("{result:?}");
    assert!(debug.contains("redacted"));
    assert!(!debug.contains("described-secret-must-not-leak"));

    let (throttle_time, tokens) = result.into_parts();
    assert_eq!(throttle_time, Duration::from_millis(17));
    assert_eq!(tokens.len(), 1);
}

#[test]
fn tokens_can_be_consumed_without_the_throttle() {
    let result = DescribeDelegationTokensResult::new(Duration::ZERO, Vec::new());

    assert!(result.into_tokens().is_empty());
}
