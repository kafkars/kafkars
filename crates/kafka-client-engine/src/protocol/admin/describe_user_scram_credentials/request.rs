//! Fallible bounded construction of one generated SCRAM description request.

use kafka_wire::{
    DescribeUserScramCredentialsRequest, RetainedSize,
    describe_user_scram_credentials_request::UserName,
};

use super::{
    DescribeUserScramCredentialsRequestRef,
    retention::{MAX_USER_BYTES, MAX_USERS, request_peak_charge},
};

/// Invalid user filter or insufficient request-materialization capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeUserScramCredentialsRequestFailure {
    EmptyUserFilter,
    TooManyUsers { actual: usize, max: usize },
    EmptyUser,
    UserTooLong { actual: usize, max: usize },
    DuplicateUser,
    RetainedBytes { required: usize, limit: usize },
}

/// Builds API-key 50 without acquiring routing, deadline, or retry authority.
pub(crate) fn describe_user_scram_credentials_request(
    source: DescribeUserScramCredentialsRequestRef<'_>,
    retained_limit: usize,
) -> Result<DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsRequestFailure> {
    validate_users(source.users())?;
    let required = request_peak_charge(source).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    validate_unique_users(source.users(), required, retained_limit)?;

    let users = source
        .users()
        .map(|users| materialize_users(users, required, retained_limit))
        .transpose()?;
    let mut request = DescribeUserScramCredentialsRequest::default();
    request.users = users;
    ensure_limit(request.retained_size().heap_bytes(), retained_limit)?;
    Ok(request)
}

pub(super) fn validate_users(
    users: Option<&[String]>,
) -> Result<(), DescribeUserScramCredentialsRequestFailure> {
    let Some(users) = users else {
        return Ok(());
    };
    if users.is_empty() {
        return Err(DescribeUserScramCredentialsRequestFailure::EmptyUserFilter);
    }
    if users.len() > MAX_USERS {
        return Err(DescribeUserScramCredentialsRequestFailure::TooManyUsers {
            actual: users.len(),
            max: MAX_USERS,
        });
    }
    for user in users {
        if user.is_empty() {
            return Err(DescribeUserScramCredentialsRequestFailure::EmptyUser);
        }
        if user.len() > MAX_USER_BYTES {
            return Err(DescribeUserScramCredentialsRequestFailure::UserTooLong {
                actual: user.len(),
                max: MAX_USER_BYTES,
            });
        }
    }
    Ok(())
}

fn validate_unique_users(
    users: Option<&[String]>,
    required: usize,
    limit: usize,
) -> Result<(), DescribeUserScramCredentialsRequestFailure> {
    let Some(users) = users else {
        return Ok(());
    };
    let mut names = Vec::new();
    names
        .try_reserve_exact(users.len())
        .map_err(|_| retained_failure(required, limit))?;
    names.extend(users.iter().map(String::as_str));
    names.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeUserScramCredentialsRequestFailure::DuplicateUser);
    }
    Ok(())
}

fn materialize_users(
    users: &[String],
    required: usize,
    limit: usize,
) -> Result<Vec<UserName>, DescribeUserScramCredentialsRequestFailure> {
    let mut generated = Vec::new();
    generated
        .try_reserve_exact(users.len())
        .map_err(|_| retained_failure(required, limit))?;
    for user in users {
        let mut name = UserName::default();
        name.name = copy_string(user, required, limit)?.into();
        generated.push(name);
    }
    Ok(generated)
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, DescribeUserScramCredentialsRequestFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeUserScramCredentialsRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or_else(|| retained_failure(required, limit))
}

const fn retained_failure(
    required: usize,
    limit: usize,
) -> DescribeUserScramCredentialsRequestFailure {
    DescribeUserScramCredentialsRequestFailure::RetainedBytes { required, limit }
}
