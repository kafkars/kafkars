//! Bounded validation of the core-retained non-secret response identity.

use super::{
    AlterUserScramCredentialsCorrelationRef, AlterUserScramCredentialsResponseFailure,
    retention::{MAX_USER_BYTES, MAX_USERS},
};

pub(super) fn validate_correlation(
    correlation: AlterUserScramCredentialsCorrelationRef<'_>,
    required: usize,
    limit: usize,
) -> Result<(), AlterUserScramCredentialsResponseFailure> {
    let users = correlation.affected_users();
    if users.is_empty() {
        return Err(AlterUserScramCredentialsResponseFailure::EmptyAffectedUsers);
    }
    if users.len() > MAX_USERS {
        return Err(
            AlterUserScramCredentialsResponseFailure::TooManyAffectedUsers {
                actual: users.len(),
                max: MAX_USERS,
            },
        );
    }
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(users.len())
        .map_err(|_| retained(required, limit))?;
    for user in users {
        if user.is_empty() {
            return Err(AlterUserScramCredentialsResponseFailure::EmptyAffectedUser);
        }
        if user.len() > MAX_USER_BYTES {
            return Err(
                AlterUserScramCredentialsResponseFailure::AffectedUserTooLong {
                    actual: user.len(),
                    max: MAX_USER_BYTES,
                },
            );
        }
        canonical.push(user.as_str());
    }
    canonical.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if canonical.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(AlterUserScramCredentialsResponseFailure::DuplicateAffectedUser);
    }
    Ok(())
}

const fn retained(required: usize, limit: usize) -> AlterUserScramCredentialsResponseFailure {
    AlterUserScramCredentialsResponseFailure::RetainedBytes { required, limit }
}
