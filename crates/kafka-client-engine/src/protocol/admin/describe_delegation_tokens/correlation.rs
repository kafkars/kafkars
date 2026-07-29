//! Charged borrowed ordering before owned token-list materialization.

use core::cmp::Ordering;

use kafka_wire::describe_delegation_token_response::{
    DescribedDelegationToken, DescribedDelegationTokenRenewer,
};

use super::DescribeDelegationTokensResponseFailure;

pub(super) fn ordered_tokens(
    tokens: &[DescribedDelegationToken],
    required: usize,
    limit: usize,
) -> Result<Vec<&DescribedDelegationToken>, DescribeDelegationTokensResponseFailure> {
    let mut ordered = Vec::new();
    ordered.try_reserve_exact(tokens.len()).map_err(|_| {
        DescribeDelegationTokensResponseFailure::Allocation {
            field: "token_correlation",
            requested: tokens.len(),
        }
    })?;
    ordered.extend(tokens);
    ordered.sort_unstable_by(|left, right| {
        principal_order(
            left.principal_type.as_str(),
            left.principal_name.as_str(),
            right.principal_type.as_str(),
            right.principal_name.as_str(),
        )
        .then_with(|| left.token_id.as_bytes().cmp(right.token_id.as_bytes()))
    });
    ensure_limit(required, limit)?;
    Ok(ordered)
}

pub(super) fn ordered_renewers(
    renewers: &[DescribedDelegationTokenRenewer],
    required: usize,
    limit: usize,
) -> Result<Vec<&DescribedDelegationTokenRenewer>, DescribeDelegationTokensResponseFailure> {
    let mut ordered = Vec::new();
    ordered.try_reserve_exact(renewers.len()).map_err(|_| {
        DescribeDelegationTokensResponseFailure::Allocation {
            field: "renewer_correlation",
            requested: renewers.len(),
        }
    })?;
    ordered.extend(renewers);
    ordered.sort_unstable_by(|left, right| {
        principal_order(
            left.principal_type.as_str(),
            left.principal_name.as_str(),
            right.principal_type.as_str(),
            right.principal_name.as_str(),
        )
    });
    ensure_limit(required, limit)?;
    Ok(ordered)
}

fn principal_order(
    left_type: &str,
    left_name: &str,
    right_type: &str,
    right_name: &str,
) -> Ordering {
    left_type
        .as_bytes()
        .cmp(right_type.as_bytes())
        .then_with(|| left_name.as_bytes().cmp(right_name.as_bytes()))
}

fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeDelegationTokensResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeDelegationTokensResponseFailure::RetainedBytes { required, limit })
}
