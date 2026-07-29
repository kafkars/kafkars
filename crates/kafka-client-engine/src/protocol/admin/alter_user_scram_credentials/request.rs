//! Secret-safe construction and wire-trait delegation for API key 51.

use core::fmt;

use kafka_wire::{
    ALTER_USER_SCRAM_CREDENTIALS_API_DESCRIPTOR, AlterUserScramCredentialsRequest,
    AlterUserScramCredentialsResponse, ApiDescriptor, KafkaMessage, KafkaRequest,
    RequestResponsePair, RetainedFootprint, RetainedSize,
    alter_user_scram_credentials_request::{ScramCredentialDeletion, ScramCredentialUpsertion},
};
use kafka_wire_core::{
    ApiKey, ApiVersion, Bytes, BytesMut, DecodeError, Decoder, EncodeError, EncodeTarget, Encoder,
    KafkaDecode, KafkaEncode, VersionRange,
};

use super::{
    AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsRequestRef,
    allocation::{copy_bytes, copy_string},
    crypto::{SecretBytes, derive, random_salt},
    request_validation::validate_request,
    retention::request_peak_charge,
};

/// Invalid request facts or unavailable bounded/cryptographic resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterUserScramCredentialsRequestFailure {
    EmptyAlterations,
    TooManyAlterations { actual: usize, max: usize },
    TooManyUsers { actual: usize, max: usize },
    EmptyUser,
    UserTooLong { actual: usize, max: usize },
    UnsupportedMechanism { actual: i8 },
    IterationsOutOfRange { actual: u32, min: u32, max: u32 },
    EmptyPassword,
    PasswordTooLong { actual: usize, max: usize },
    SaltTooShort { actual: usize, min: usize },
    SaltTooLong { actual: usize, max: usize },
    DuplicateCredential,
    SecureRandom,
    RetainedBytes { required: usize, limit: usize },
}

/// Linear, redacted request accepted directly by `kafka-driver`.
#[must_use = "a prepared SCRAM alteration must be submitted or deliberately released"]
pub(crate) struct PreparedAlterUserScramCredentialsRequest {
    request: AlterUserScramCredentialsRequest,
}

impl fmt::Debug for PreparedAlterUserScramCredentialsRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedAlterUserScramCredentialsRequest")
            .field("deletions", &self.request.deletions.len())
            .field("upsertions", &self.request.upsertions.len())
            .field("credential_material", &"[REDACTED]")
            .finish()
    }
}

impl PreparedAlterUserScramCredentialsRequest {
    /// Reports the generated request's owned heap bytes without exporting wire ownership.
    pub(crate) fn retained_heap_bytes(&self) -> usize {
        self.retained_size().heap_bytes()
    }
}

/// Validates before PBKDF2 and creates one exact-v0 generated owner.
pub(crate) fn alter_user_scram_credentials_request(
    source: AlterUserScramCredentialsRequestRef<'_>,
    retained_limit: usize,
) -> Result<PreparedAlterUserScramCredentialsRequest, AlterUserScramCredentialsRequestFailure> {
    let required = request_peak_charge(source).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    validate_request(source, required, retained_limit)?;
    let deletion_count = source
        .alterations()
        .iter()
        .filter(|item| matches!(item, AlterUserScramCredentialAlterationRef::Delete { .. }))
        .count();
    let upsertion_count = source.alterations().len() - deletion_count;
    let mut deletions = Vec::new();
    deletions
        .try_reserve_exact(deletion_count)
        .map_err(|_| retained(required, retained_limit))?;
    let mut upsertions = Vec::new();
    upsertions
        .try_reserve_exact(upsertion_count)
        .map_err(|_| retained(required, retained_limit))?;
    for alteration in source.alterations().iter().copied() {
        match alteration {
            AlterUserScramCredentialAlterationRef::Delete { user, mechanism } => deletions.push(
                generated_deletion(user, mechanism, required, retained_limit)?,
            ),
            AlterUserScramCredentialAlterationRef::Upsert {
                user,
                mechanism,
                iterations,
                password,
                salt,
            } => upsertions.push(generated_upsertion(
                user,
                mechanism,
                iterations,
                password,
                salt,
                required,
                retained_limit,
            )?),
        }
    }
    let mut request = AlterUserScramCredentialsRequest::default();
    request.deletions = deletions;
    request.upsertions = upsertions;
    ensure_limit(request.retained_size().heap_bytes(), retained_limit)?;
    Ok(PreparedAlterUserScramCredentialsRequest { request })
}

fn generated_deletion(
    user: &str,
    mechanism: i8,
    required: usize,
    limit: usize,
) -> Result<ScramCredentialDeletion, AlterUserScramCredentialsRequestFailure> {
    let mut deletion = ScramCredentialDeletion::default();
    deletion.name = copy_string(user, required, limit)?.into();
    deletion.mechanism = mechanism;
    Ok(deletion)
}

#[allow(clippy::too_many_arguments)]
fn generated_upsertion(
    user: &str,
    mechanism: i8,
    iterations: u32,
    password: &[u8],
    salt: Option<&[u8]>,
    required: usize,
    limit: usize,
) -> Result<ScramCredentialUpsertion, AlterUserScramCredentialsRequestFailure> {
    let salt = match salt {
        Some(salt) => copy_bytes(salt, required, limit)?,
        None => random_salt(required, limit)?,
    };
    let salted_password = derive(mechanism, iterations, password, &salt, required, limit)?;
    let wire_iterations = i32::try_from(iterations).map_err(|_| {
        AlterUserScramCredentialsRequestFailure::IterationsOutOfRange {
            actual: iterations,
            min: super::retention::MIN_ITERATIONS,
            max: super::retention::MAX_ITERATIONS,
        }
    })?;
    let mut upsertion = ScramCredentialUpsertion::default();
    upsertion.name = copy_string(user, required, limit)?.into();
    upsertion.mechanism = mechanism;
    upsertion.iterations = wire_iterations;
    upsertion.salt = Bytes::from(salt);
    upsertion.salted_password = Bytes::from_owner(salted_password);
    Ok(upsertion)
}

fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), AlterUserScramCredentialsRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or_else(|| retained(required, limit))
}

const fn retained(required: usize, limit: usize) -> AlterUserScramCredentialsRequestFailure {
    AlterUserScramCredentialsRequestFailure::RetainedBytes { required, limit }
}

impl KafkaDecode for PreparedAlterUserScramCredentialsRequest {
    fn decode(decoder: &mut Decoder, version: ApiVersion) -> Result<Self, DecodeError> {
        let mut request = AlterUserScramCredentialsRequest::decode(decoder, version)?;
        for upsertion in &mut request.upsertions {
            let protected = SecretBytes(upsertion.salted_password.to_vec());
            upsertion.salted_password = Bytes::from_owner(protected);
        }
        Ok(Self { request })
    }
}

impl KafkaEncode for PreparedAlterUserScramCredentialsRequest {
    fn encode<T: EncodeTarget>(
        &self,
        encoder: &mut Encoder<T>,
        version: ApiVersion,
    ) -> Result<(), EncodeError> {
        self.request.encode(encoder, version)
    }

    fn encoded_len(&self, version: ApiVersion) -> Result<usize, EncodeError> {
        self.request.encoded_len(version)
    }

    fn encode_into(
        &self,
        buffer: &mut BytesMut,
        version: ApiVersion,
    ) -> Result<usize, EncodeError> {
        self.request.encode_into(buffer, version)
    }
}

impl RetainedSize for PreparedAlterUserScramCredentialsRequest {
    fn retained_size(&self) -> RetainedFootprint {
        self.request.retained_size()
    }
}

impl KafkaMessage for PreparedAlterUserScramCredentialsRequest {
    const NAME: &'static str = <AlterUserScramCredentialsRequest as KafkaMessage>::NAME;
    const SUPPORTED_VERSIONS: VersionRange =
        <AlterUserScramCredentialsRequest as KafkaMessage>::SUPPORTED_VERSIONS;
    const FLEXIBLE_VERSIONS: Option<VersionRange> =
        <AlterUserScramCredentialsRequest as KafkaMessage>::FLEXIBLE_VERSIONS;
}

impl KafkaRequest for PreparedAlterUserScramCredentialsRequest {
    const API_KEY: ApiKey = <AlterUserScramCredentialsRequest as KafkaRequest>::API_KEY;
    const API_DESCRIPTOR: &'static ApiDescriptor = &ALTER_USER_SCRAM_CREDENTIALS_API_DESCRIPTOR;
}

impl RequestResponsePair for PreparedAlterUserScramCredentialsRequest {
    type Response = AlterUserScramCredentialsResponse;
}

#[cfg(test)]
impl PreparedAlterUserScramCredentialsRequest {
    pub(super) const fn request_for_test(&self) -> &AlterUserScramCredentialsRequest {
        &self.request
    }
}
