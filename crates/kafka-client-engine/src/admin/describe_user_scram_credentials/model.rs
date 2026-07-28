//! Engine-owned, wire-free user selection for one SCRAM credential description.

use kafka_client_core::{
    DescribeUserScramCredentialsPlan as CorePlan,
    DescribeUserScramCredentialsPlanError as CorePlanError,
};

/// One bounded, wire-free SCRAM credential-description request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeUserScramCredentialsRequest {
    users: Option<Vec<String>>,
}

impl DescribeUserScramCredentialsRequest {
    /// Creates inert request intent; `None` explicitly selects every user.
    pub const fn new(users: Option<Vec<String>>) -> Self {
        Self { users }
    }

    /// Returns selected users in caller order, or `None` for every user.
    pub fn users(&self) -> Option<&[String]> {
        self.users.as_deref()
    }

    /// Consumes this request into its optional caller-ordered user selection.
    pub fn into_users(self) -> Option<Vec<String>> {
        self.users
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        if let Some(users) = self.users.as_mut() {
            users.shrink_to_fit();
        }
        self
    }

    pub(crate) fn into_plan(self) -> Result<CorePlan, CorePlanError> {
        CorePlan::new(self.into_users())
    }
}
