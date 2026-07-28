//! Borrowed user selection and generated-free SCRAM description facts.

/// One borrowed API-key 50 selection; `None` describes every visible user.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DescribeUserScramCredentialsRequestRef<'a> {
    users: Option<&'a [String]>,
}

impl<'a> DescribeUserScramCredentialsRequestRef<'a> {
    /// Selects every user visible to the authenticated principal.
    pub(crate) const fn all() -> Self {
        Self { users: None }
    }

    /// Selects one nonempty caller-ordered set of user names.
    pub(crate) const fn selected(users: &'a [String]) -> Self {
        Self { users: Some(users) }
    }

    pub(crate) const fn users(self) -> Option<&'a [String]> {
        self.users
    }
}

/// One public-safe SCRAM mechanism fact; it contains no credential material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedScramCredentialInfo {
    mechanism: i8,
    iterations: u32,
}

impl NormalizedScramCredentialInfo {
    pub(super) const fn new(mechanism: i8, iterations: u32) -> Self {
        Self {
            mechanism,
            iterations,
        }
    }

    pub(crate) const fn into_parts(self) -> (i8, u32) {
        (self.mechanism, self.iterations)
    }
}

/// One caller-correlated user result with exact signed broker facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedUserScramCredentials {
    pub(super) user: String,
    pub(super) error_code: i16,
    pub(super) error_message: Option<String>,
    pub(super) error_message_truncated: bool,
    pub(super) credential_infos: Vec<NormalizedScramCredentialInfo>,
}

impl NormalizedUserScramCredentials {
    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        i16,
        Option<String>,
        bool,
        Vec<NormalizedScramCredentialInfo>,
    ) {
        (
            self.user,
            self.error_code,
            self.error_message,
            self.error_message_truncated,
            self.credential_infos,
        )
    }
}

/// One validated API-key 50 response with no salts, keys, or passwords.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedDescribeUserScramCredentialsResponse {
    pub(super) throttle_time_ms: u32,
    pub(super) error_code: i16,
    pub(super) error_message: Option<String>,
    pub(super) error_message_truncated: bool,
    pub(super) results: Vec<NormalizedUserScramCredentials>,
    pub(super) retained_bytes: usize,
}

impl NormalizedDescribeUserScramCredentialsResponse {
    pub(crate) fn into_parts(
        self,
    ) -> (
        u32,
        i16,
        Option<String>,
        bool,
        Vec<NormalizedUserScramCredentials>,
        usize,
    ) {
        (
            self.throttle_time_ms,
            self.error_code,
            self.error_message,
            self.error_message_truncated,
            self.results,
            self.retained_bytes,
        )
    }
}
