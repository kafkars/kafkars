//! Optional public user filter translated only at the engine boundary.

use super::engine::Request as EngineRequest;

/// Optional user filter retained by the inert public builder.
pub(crate) struct DescribeUserScramCredentialsAdminRequest {
    users: Option<Vec<String>>,
}

impl DescribeUserScramCredentialsAdminRequest {
    pub(crate) const fn new() -> Self {
        Self { users: None }
    }

    pub(crate) fn with_users(mut self, users: Vec<String>) -> Self {
        self.users = Some(users);
        self
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.users)
    }
}

impl std::fmt::Debug for DescribeUserScramCredentialsAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DescribeUserScramCredentialsAdminRequest")
            .field("users", &self.users)
            .finish()
    }
}
