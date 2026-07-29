//! Bounded PBKDF2 derivation and operating-system salt generation.

use core::num::NonZeroU32;

use kafka_client_core::{
    ALTER_USER_SCRAM_CREDENTIALS_SHA_256, ALTER_USER_SCRAM_CREDENTIALS_SHA_512,
};
use ring::{
    pbkdf2,
    rand::{SecureRandom, SystemRandom},
};
use zeroize::Zeroize;

use super::AlterUserScramCredentialsRequestFailure;

pub(super) const SCRAM_SHA_256: i8 = ALTER_USER_SCRAM_CREDENTIALS_SHA_256;
pub(super) const SCRAM_SHA_512: i8 = ALTER_USER_SCRAM_CREDENTIALS_SHA_512;
pub(super) const SHA_256_OUTPUT_BYTES: usize = 32;
pub(super) const SHA_512_OUTPUT_BYTES: usize = 64;
pub(super) const GENERATED_SALT_BYTES: usize = 32;

pub(super) struct SecretBytes(pub(super) Vec<u8>);

impl SecretBytes {
    fn try_zeroed(
        len: usize,
        required: usize,
        limit: usize,
    ) -> Result<Self, AlterUserScramCredentialsRequestFailure> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| retained(required, limit))?;
        bytes.resize(len, 0);
        Ok(Self(bytes))
    }

    #[cfg(test)]
    pub(super) fn wipe_for_test(&mut self) {
        self.0.zeroize();
    }
}

impl AsRef<[u8]> for SecretBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

pub(super) const fn output_len(mechanism: i8) -> Option<usize> {
    match mechanism {
        SCRAM_SHA_256 => Some(SHA_256_OUTPUT_BYTES),
        SCRAM_SHA_512 => Some(SHA_512_OUTPUT_BYTES),
        _ => None,
    }
}

pub(super) fn derive(
    mechanism: i8,
    iterations: u32,
    password: &[u8],
    salt: &[u8],
    required: usize,
    limit: usize,
) -> Result<SecretBytes, AlterUserScramCredentialsRequestFailure> {
    let algorithm = match mechanism {
        SCRAM_SHA_256 => pbkdf2::PBKDF2_HMAC_SHA256,
        SCRAM_SHA_512 => pbkdf2::PBKDF2_HMAC_SHA512,
        _ => {
            return Err(
                AlterUserScramCredentialsRequestFailure::UnsupportedMechanism { actual: mechanism },
            );
        }
    };
    let Some(iterations) = NonZeroU32::new(iterations) else {
        return Err(
            AlterUserScramCredentialsRequestFailure::IterationsOutOfRange {
                actual: 0,
                min: super::retention::MIN_ITERATIONS,
                max: super::retention::MAX_ITERATIONS,
            },
        );
    };
    let mut derived =
        SecretBytes::try_zeroed(output_len(mechanism).unwrap_or_default(), required, limit)?;
    pbkdf2::derive(algorithm, iterations, salt, password, &mut derived.0);
    Ok(derived)
}

pub(super) fn random_salt(
    required: usize,
    limit: usize,
) -> Result<Vec<u8>, AlterUserScramCredentialsRequestFailure> {
    let mut salt = Vec::new();
    salt.try_reserve_exact(GENERATED_SALT_BYTES)
        .map_err(|_| retained(required, limit))?;
    salt.resize(GENERATED_SALT_BYTES, 0);
    SystemRandom::new()
        .fill(&mut salt)
        .map_err(|_| AlterUserScramCredentialsRequestFailure::SecureRandom)?;
    Ok(salt)
}

const fn retained(required: usize, limit: usize) -> AlterUserScramCredentialsRequestFailure {
    AlterUserScramCredentialsRequestFailure::RetainedBytes { required, limit }
}
