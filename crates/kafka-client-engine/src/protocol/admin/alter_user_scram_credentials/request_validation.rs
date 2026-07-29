//! Validate-first request identities without touching credential bytes.

use super::{
    AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsRequestFailure,
    AlterUserScramCredentialsRequestRef,
    crypto::output_len,
    retention::{
        MAX_ALTERATIONS, MAX_ITERATIONS, MAX_PASSWORD_BYTES, MAX_SALT_BYTES, MAX_USER_BYTES,
        MAX_USERS, MIN_ITERATIONS, MIN_SALT_BYTES,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CanonicalAlterationKey<'a> {
    pub(super) user: &'a str,
    pub(super) mechanism: i8,
    pub(super) index: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FirstUserRef<'a> {
    pub(super) user: &'a str,
    pub(super) first_index: usize,
}

pub(super) fn validated_first_users(
    source: AlterUserScramCredentialsRequestRef<'_>,
    required: usize,
    limit: usize,
) -> Result<Vec<FirstUserRef<'_>>, AlterUserScramCredentialsRequestFailure> {
    let mut keys = validate_and_sort(source, required, limit)?;
    let mut users = Vec::new();
    users
        .try_reserve_exact(keys.len())
        .map_err(|_| retained(required, limit))?;
    let mut start = 0;
    while start < keys.len() {
        let user = keys[start].user;
        let mut first_index = keys[start].index;
        let mut end = start + 1;
        while end < keys.len() && keys[end].user == user {
            first_index = first_index.min(keys[end].index);
            end += 1;
        }
        users.push(FirstUserRef { user, first_index });
        start = end;
    }
    if users.len() > MAX_USERS {
        return Err(AlterUserScramCredentialsRequestFailure::TooManyUsers {
            actual: users.len(),
            max: MAX_USERS,
        });
    }
    users.sort_unstable_by_key(|user| user.first_index);
    keys.clear();
    Ok(users)
}

pub(super) fn validate_request(
    source: AlterUserScramCredentialsRequestRef<'_>,
    required: usize,
    limit: usize,
) -> Result<(), AlterUserScramCredentialsRequestFailure> {
    validated_first_users(source, required, limit).map(drop)
}

fn validate_and_sort(
    source: AlterUserScramCredentialsRequestRef<'_>,
    required: usize,
    limit: usize,
) -> Result<Vec<CanonicalAlterationKey<'_>>, AlterUserScramCredentialsRequestFailure> {
    let alterations = source.alterations();
    if alterations.is_empty() {
        return Err(AlterUserScramCredentialsRequestFailure::EmptyAlterations);
    }
    if alterations.len() > MAX_ALTERATIONS {
        return Err(
            AlterUserScramCredentialsRequestFailure::TooManyAlterations {
                actual: alterations.len(),
                max: MAX_ALTERATIONS,
            },
        );
    }
    let mut keys = Vec::new();
    keys.try_reserve_exact(alterations.len())
        .map_err(|_| retained(required, limit))?;
    for (index, alteration) in alterations.iter().copied().enumerate() {
        validate_alteration(alteration)?;
        keys.push(CanonicalAlterationKey {
            user: alteration.user(),
            mechanism: alteration.mechanism(),
            index,
        });
    }
    keys.sort_unstable_by(|left, right| {
        left.user
            .as_bytes()
            .cmp(right.user.as_bytes())
            .then_with(|| left.mechanism.cmp(&right.mechanism))
    });
    if keys
        .windows(2)
        .any(|pair| pair[0].user == pair[1].user && pair[0].mechanism == pair[1].mechanism)
    {
        return Err(AlterUserScramCredentialsRequestFailure::DuplicateCredential);
    }
    Ok(keys)
}

fn validate_alteration(
    alteration: AlterUserScramCredentialAlterationRef<'_>,
) -> Result<(), AlterUserScramCredentialsRequestFailure> {
    let user = alteration.user();
    if user.is_empty() {
        return Err(AlterUserScramCredentialsRequestFailure::EmptyUser);
    }
    if user.len() > MAX_USER_BYTES {
        return Err(AlterUserScramCredentialsRequestFailure::UserTooLong {
            actual: user.len(),
            max: MAX_USER_BYTES,
        });
    }
    if output_len(alteration.mechanism()).is_none() {
        return Err(
            AlterUserScramCredentialsRequestFailure::UnsupportedMechanism {
                actual: alteration.mechanism(),
            },
        );
    }
    if let AlterUserScramCredentialAlterationRef::Upsert {
        iterations,
        password,
        salt,
        ..
    } = alteration
    {
        validate_upsertion(iterations, password, salt)?;
    }
    Ok(())
}

fn validate_upsertion(
    iterations: u32,
    password: &[u8],
    salt: Option<&[u8]>,
) -> Result<(), AlterUserScramCredentialsRequestFailure> {
    if !(MIN_ITERATIONS..=MAX_ITERATIONS).contains(&iterations) {
        return Err(
            AlterUserScramCredentialsRequestFailure::IterationsOutOfRange {
                actual: iterations,
                min: MIN_ITERATIONS,
                max: MAX_ITERATIONS,
            },
        );
    }
    if password.is_empty() {
        return Err(AlterUserScramCredentialsRequestFailure::EmptyPassword);
    }
    if password.len() > MAX_PASSWORD_BYTES {
        return Err(AlterUserScramCredentialsRequestFailure::PasswordTooLong {
            actual: password.len(),
            max: MAX_PASSWORD_BYTES,
        });
    }
    if let Some(salt) = salt {
        if salt.len() < MIN_SALT_BYTES {
            return Err(AlterUserScramCredentialsRequestFailure::SaltTooShort {
                actual: salt.len(),
                min: MIN_SALT_BYTES,
            });
        }
        if salt.len() > MAX_SALT_BYTES {
            return Err(AlterUserScramCredentialsRequestFailure::SaltTooLong {
                actual: salt.len(),
                max: MAX_SALT_BYTES,
            });
        }
    }
    Ok(())
}

const fn retained(required: usize, limit: usize) -> AlterUserScramCredentialsRequestFailure {
    AlterUserScramCredentialsRequestFailure::RetainedBytes { required, limit }
}
