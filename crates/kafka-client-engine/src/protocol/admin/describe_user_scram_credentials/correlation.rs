//! Exact filtered correlation and deterministic unfiltered result ordering.

use kafka_wire::describe_user_scram_credentials_response::DescribeUserScramCredentialsResult;

use super::{
    DescribeUserScramCredentialsRequestRef, DescribeUserScramCredentialsResponseFailure,
    retention::{MAX_USER_BYTES, MAX_USERS},
};

pub(super) fn validate_request_selection(
    request: DescribeUserScramCredentialsRequestRef<'_>,
    required: usize,
    limit: usize,
) -> Result<(), DescribeUserScramCredentialsResponseFailure> {
    canonical_requested(request.users(), required, limit).map(drop)
}

pub(super) fn ordered_results<'a>(
    request: DescribeUserScramCredentialsRequestRef<'_>,
    results: &'a [DescribeUserScramCredentialsResult],
    required: usize,
    limit: usize,
) -> Result<Vec<&'a DescribeUserScramCredentialsResult>, DescribeUserScramCredentialsResponseFailure>
{
    let requested = canonical_requested(request.users(), required, limit)?;
    let mut returned = Vec::new();
    returned
        .try_reserve_exact(results.len())
        .map_err(|_| retained(required, limit))?;
    returned.extend(results);
    returned.sort_unstable_by(|left, right| left.user.as_bytes().cmp(right.user.as_bytes()));
    if returned.windows(2).any(|pair| pair[0].user == pair[1].user) {
        return Err(DescribeUserScramCredentialsResponseFailure::DuplicateUser);
    }

    let Some(requested) = requested else {
        return Ok(returned);
    };
    for user in &requested {
        if returned
            .binary_search_by(|result| result.user.as_bytes().cmp(user.as_bytes()))
            .is_err()
        {
            return Err(DescribeUserScramCredentialsResponseFailure::MissingUser);
        }
    }
    for result in &returned {
        if requested
            .binary_search_by(|user| user.as_bytes().cmp(result.user.as_bytes()))
            .is_err()
        {
            return Err(DescribeUserScramCredentialsResponseFailure::UnexpectedUser);
        }
    }

    let users = request.users().unwrap_or_default();
    let mut ordered = Vec::new();
    ordered
        .try_reserve_exact(users.len())
        .map_err(|_| retained(required, limit))?;
    for user in users {
        let index = returned
            .binary_search_by(|result| result.user.as_bytes().cmp(user.as_bytes()))
            .map_err(|_| DescribeUserScramCredentialsResponseFailure::MissingUser)?;
        ordered.push(returned[index]);
    }
    Ok(ordered)
}

fn canonical_requested<'a>(
    users: Option<&'a [String]>,
    required: usize,
    limit: usize,
) -> Result<Option<Vec<&'a str>>, DescribeUserScramCredentialsResponseFailure> {
    let Some(users) = users else {
        return Ok(None);
    };
    if users.is_empty() {
        return Err(DescribeUserScramCredentialsResponseFailure::EmptyUserFilter);
    }
    if users.len() > MAX_USERS {
        return Err(
            DescribeUserScramCredentialsResponseFailure::TooManyRequestedUsers {
                actual: users.len(),
                max: MAX_USERS,
            },
        );
    }
    let mut requested = Vec::new();
    requested
        .try_reserve_exact(users.len())
        .map_err(|_| retained(required, limit))?;
    for user in users {
        if user.is_empty() {
            return Err(DescribeUserScramCredentialsResponseFailure::EmptyRequestedUser);
        }
        if user.len() > MAX_USER_BYTES {
            return Err(
                DescribeUserScramCredentialsResponseFailure::RequestedUserTooLong {
                    actual: user.len(),
                    max: MAX_USER_BYTES,
                },
            );
        }
        requested.push(user.as_str());
    }
    requested.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if requested.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeUserScramCredentialsResponseFailure::DuplicateRequestedUser);
    }
    Ok(Some(requested))
}

const fn retained(required: usize, limit: usize) -> DescribeUserScramCredentialsResponseFailure {
    DescribeUserScramCredentialsResponseFailure::RetainedBytes { required, limit }
}
