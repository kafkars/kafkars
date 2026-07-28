//! Validated wire-free user selection for one SCRAM credential description query.

use core::fmt;
use std::collections::BTreeSet;

/// Maximum explicitly selected users retained by one operation.
pub const DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_USERS: usize = 16 * 1024;
const MAX_USER_NAME_BYTES: usize = i16::MAX as usize;

/// Validated intent for one bounded SCRAM credential description query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsPlan {
    users: Option<Vec<String>>,
}

impl DescribeUserScramCredentialsPlan {
    /// Validates an optional caller-ordered selection of unique users.
    ///
    /// `None` explicitly selects every user. A present selection must be
    /// nonempty so an empty array cannot silently acquire all-user semantics.
    pub fn new(users: Option<Vec<String>>) -> Result<Self, DescribeUserScramCredentialsPlanError> {
        let Some(selected_users) = users.as_ref() else {
            return Ok(Self { users });
        };
        if selected_users.is_empty() {
            return Err(DescribeUserScramCredentialsPlanError::EmptyUserSelection);
        }
        if selected_users.len() > DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_USERS {
            return Err(DescribeUserScramCredentialsPlanError::TooManyUsers);
        }
        let mut identities = BTreeSet::new();
        for user in selected_users {
            if user.is_empty() {
                return Err(DescribeUserScramCredentialsPlanError::EmptyUserName);
            }
            if user.len() > MAX_USER_NAME_BYTES {
                return Err(DescribeUserScramCredentialsPlanError::UserNameTooLong);
            }
            if !identities.insert(user.as_str()) {
                return Err(DescribeUserScramCredentialsPlanError::DuplicateUserName);
            }
        }
        Ok(Self { users })
    }

    /// Returns selected users in caller order, or `None` for all users.
    pub fn users(&self) -> Option<&[String]> {
        self.users.as_deref()
    }

    /// Reports whether the query explicitly selects every user.
    pub const fn describes_all_users(&self) -> bool {
        self.users.is_none()
    }
}

/// Invalid deterministic SCRAM credential description intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescribeUserScramCredentialsPlanError {
    /// A present user selection must contain at least one user.
    EmptyUserSelection,
    /// One operation cannot retain more than 16,384 selected users.
    TooManyUsers,
    /// User names must not be empty.
    EmptyUserName,
    /// User names must fit Kafka's string domain.
    UserNameTooLong,
    /// One operation cannot repeat a user name.
    DuplicateUserName,
}

impl fmt::Display for DescribeUserScramCredentialsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid DescribeUserScramCredentials selection: {self:?}"
        )
    }
}

impl std::error::Error for DescribeUserScramCredentialsPlanError {}
