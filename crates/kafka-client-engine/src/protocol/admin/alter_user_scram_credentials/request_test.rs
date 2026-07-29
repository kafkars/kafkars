//! Focused request derivation, validation, and secret-redaction tests.

use super::{
    AlterUserScramCredentialAlterationRef as Alteration, AlterUserScramCredentialsRequestFailure,
    AlterUserScramCredentialsRequestRef, alter_user_scram_credentials_request,
    crypto::{GENERATED_SALT_BYTES, SecretBytes},
};

const LIMIT: usize = 4 * 1024 * 1024;
const SALT: &[u8; 16] = b"0123456789abcdef";
const SHA_256_VECTOR: [u8; 32] = [
    0x61, 0x5e, 0xee, 0xe6, 0x52, 0xa9, 0xfe, 0x1c, 0xf9, 0xf4, 0x27, 0xd6, 0x21, 0x85, 0x26, 0xa9,
    0x37, 0xad, 0xd0, 0x18, 0x07, 0x98, 0x54, 0xb2, 0xaa, 0x11, 0x87, 0x5d, 0x75, 0x98, 0x37, 0xac,
];
const SHA_512_VECTOR: [u8; 64] = [
    0x7d, 0x7f, 0x1c, 0x3f, 0x59, 0xff, 0x3c, 0x1b, 0xc1, 0xbe, 0x23, 0x42, 0x78, 0x6e, 0xa3, 0x82,
    0xba, 0xfd, 0x62, 0xff, 0xdb, 0x7a, 0x9c, 0x7e, 0xa9, 0x5d, 0x1a, 0xa1, 0x35, 0xd7, 0x76, 0x87,
    0xcc, 0x74, 0xbb, 0xe5, 0x1d, 0x71, 0xac, 0x12, 0x30, 0x95, 0x74, 0x79, 0xe5, 0xb7, 0x43, 0x80,
    0x95, 0x40, 0x21, 0x51, 0xca, 0xa5, 0xac, 0x7b, 0xac, 0xe9, 0x20, 0xcc, 0xce, 0x8b, 0x4e, 0xd3,
];

#[test]
fn explicit_salt_derives_known_sha_256_and_sha_512_vectors() {
    let alterations = [
        Alteration::upsert("sha-256", 1, 4096, b"password", Some(SALT)),
        Alteration::upsert("sha-512", 2, 4096, b"password", Some(SALT)),
    ];
    let prepared = prepared(&alterations);
    let request = prepared.request_for_test();
    assert!(request.deletions.is_empty());
    assert_eq!(request.upsertions.len(), 2);
    assert_eq!(request.upsertions[0].salt.as_ref(), SALT);
    assert_eq!(
        request.upsertions[0].salted_password.as_ref(),
        SHA_256_VECTOR
    );
    assert_eq!(
        request.upsertions[1].salted_password.as_ref(),
        SHA_512_VECTOR
    );
}

#[test]
fn generated_salt_is_securely_sized_and_request_preserves_non_secret_facts() {
    let alterations = [
        Alteration::delete("alice", 1),
        Alteration::upsert("bob", 2, 8192, b"secret", None),
    ];
    let prepared = prepared(&alterations);
    let request = prepared.request_for_test();
    assert_eq!(request.deletions[0].name.as_str(), "alice");
    assert_eq!(request.deletions[0].mechanism, 1);
    assert_eq!(request.upsertions[0].name.as_str(), "bob");
    assert_eq!(request.upsertions[0].mechanism, 2);
    assert_eq!(request.upsertions[0].iterations, 8192);
    assert_eq!(request.upsertions[0].salt.len(), GENERATED_SALT_BYTES);
    assert_eq!(request.upsertions[0].salted_password.len(), 64);
}

#[test]
fn diagnostics_redact_plaintext_salt_and_derived_credential_material() {
    let alterations = [Alteration::upsert(
        "alice",
        1,
        4096,
        b"password-must-not-leak",
        Some(b"salt-must-not-leak"),
    )];
    let source = AlterUserScramCredentialsRequestRef::new(&alterations);
    let prepared = prepared(&alterations);
    let diagnostic = format!("{source:?} {prepared:?}");
    assert!(diagnostic.contains("[REDACTED]"));
    assert!(!diagnostic.contains("password-must-not-leak"));
    assert!(!diagnostic.contains("salt-must-not-leak"));
    assert!(!diagnostic.contains("615eeee6"));
}

#[test]
fn secret_owner_wipe_path_clears_every_byte() {
    let mut secret = SecretBytes(vec![1, 2, 3, 4, 5]);
    secret.wipe_for_test();
    assert!(secret.0.iter().all(|byte| *byte == 0));
}

#[test]
fn malformed_secret_and_identity_inputs_fail_before_request_ownership() {
    assert_failure(
        &[],
        AlterUserScramCredentialsRequestFailure::EmptyAlterations,
    );
    assert_failure(
        &[Alteration::delete("", 1)],
        AlterUserScramCredentialsRequestFailure::EmptyUser,
    );
    assert_failure(
        &[Alteration::delete("alice", 7)],
        AlterUserScramCredentialsRequestFailure::UnsupportedMechanism { actual: 7 },
    );
    assert_failure(
        &[Alteration::upsert("alice", 1, 4095, b"password", None)],
        AlterUserScramCredentialsRequestFailure::IterationsOutOfRange {
            actual: 4095,
            min: 4096,
            max: 16_384,
        },
    );
    assert_failure(
        &[Alteration::upsert("alice", 1, 4096, b"", None)],
        AlterUserScramCredentialsRequestFailure::EmptyPassword,
    );
    assert_failure(
        &[Alteration::upsert(
            "alice",
            1,
            4096,
            b"password",
            Some(b"short"),
        )],
        AlterUserScramCredentialsRequestFailure::SaltTooShort { actual: 5, min: 16 },
    );
}

#[test]
fn duplicate_user_mechanism_is_rejected_across_delete_and_upsert() {
    let alterations = [
        Alteration::delete("alice", 1),
        Alteration::upsert("alice", 1, 4096, b"password", Some(SALT)),
    ];
    assert_failure(
        &alterations,
        AlterUserScramCredentialsRequestFailure::DuplicateCredential,
    );
}

#[test]
fn user_password_salt_iteration_and_count_limits_are_exact() {
    let long_user = "u".repeat(i16::MAX as usize + 1);
    assert_failure(
        &[Alteration::delete(&long_user, 1)],
        AlterUserScramCredentialsRequestFailure::UserTooLong {
            actual: long_user.len(),
            max: i16::MAX as usize,
        },
    );
    let long_password = vec![7; 16 * 1024 + 1];
    assert_failure(
        &[Alteration::upsert("alice", 1, 4096, &long_password, None)],
        AlterUserScramCredentialsRequestFailure::PasswordTooLong {
            actual: long_password.len(),
            max: 16 * 1024,
        },
    );
    let long_salt = [3; 65];
    assert_failure(
        &[Alteration::upsert(
            "alice",
            1,
            4096,
            b"password",
            Some(&long_salt),
        )],
        AlterUserScramCredentialsRequestFailure::SaltTooLong {
            actual: 65,
            max: 64,
        },
    );
    assert_failure(
        &[Alteration::upsert("alice", 1, 16_385, b"password", None)],
        AlterUserScramCredentialsRequestFailure::IterationsOutOfRange {
            actual: 16_385,
            min: 4096,
            max: 16_384,
        },
    );
    let too_many = vec![Alteration::delete("alice", 1); 1025];
    assert_failure(
        &too_many,
        AlterUserScramCredentialsRequestFailure::TooManyAlterations {
            actual: 1025,
            max: 1024,
        },
    );
}

fn prepared(alterations: &[Alteration<'_>]) -> super::PreparedAlterUserScramCredentialsRequest {
    let result = alter_user_scram_credentials_request(
        AlterUserScramCredentialsRequestRef::new(alterations),
        LIMIT,
    );
    let Ok(prepared) = result else {
        panic!("valid test request must prepare");
    };
    prepared
}

fn assert_failure(
    alterations: &[Alteration<'_>],
    expected: AlterUserScramCredentialsRequestFailure,
) {
    let actual = alter_user_scram_credentials_request(
        AlterUserScramCredentialsRequestRef::new(alterations),
        LIMIT,
    );
    assert_eq!(actual.err(), Some(expected));
}
