//! Public SCRAM alteration ownership and redaction scenarios.

use crate::admin::ScramMechanism;

use super::UserScramCredentialAlteration;

#[test]
fn constructors_preserve_non_secret_intent() {
    let delete = UserScramCredentialAlteration::delete("alice", ScramMechanism::SHA_256);
    assert_eq!(delete.user(), "alice");
    assert_eq!(delete.mechanism(), ScramMechanism::SHA_256);
    assert_eq!(delete.iterations(), None);
    assert_eq!(delete.password_len(), None);
    assert_eq!(delete.salt_len(), None);

    let generated_salt = UserScramCredentialAlteration::upsert(
        "bob",
        ScramMechanism::SHA_512,
        8_192,
        b"password".to_vec(),
    );
    assert_eq!(generated_salt.user(), "bob");
    assert_eq!(generated_salt.mechanism(), ScramMechanism::SHA_512);
    assert_eq!(generated_salt.iterations(), Some(8_192));
    assert_eq!(generated_salt.password_len(), Some(8));
    assert_eq!(generated_salt.salt_len(), None);

    let explicit_salt = UserScramCredentialAlteration::upsert_with_salt(
        "carol",
        ScramMechanism::SHA_256,
        4_096,
        b"secret".to_vec(),
        b"salt".to_vec(),
    );
    assert_eq!(explicit_salt.password_len(), Some(6));
    assert_eq!(explicit_salt.salt_len(), Some(4));
}

#[test]
fn diagnostics_never_contain_password_or_salt_bytes() {
    let alteration = UserScramCredentialAlteration::upsert_with_salt(
        "alice",
        ScramMechanism::SHA_512,
        16_384,
        b"password-must-not-leak".to_vec(),
        b"salt-must-not-leak".to_vec(),
    );

    let diagnostic = format!("{alteration:?}");
    assert!(diagnostic.contains("alice"));
    assert!(diagnostic.contains("password_len"));
    assert!(diagnostic.contains("salt_len"));
    assert!(!diagnostic.contains("password-must-not-leak"));
    assert!(!diagnostic.contains("salt-must-not-leak"));
}

#[test]
fn drop_path_zeroizes_both_secret_buffers() {
    let mut alteration = UserScramCredentialAlteration::upsert_with_salt(
        "alice",
        ScramMechanism::SHA_256,
        4_096,
        b"password".to_vec(),
        b"salt".to_vec(),
    );

    alteration.zeroize_secrets_for_test();
    match &alteration {
        UserScramCredentialAlteration::Upsert { password, salt, .. } => {
            assert!(password.is_empty());
            assert!(salt.as_ref().is_some_and(Vec::is_empty));
        }
        UserScramCredentialAlteration::Delete { .. } => panic!("expected upsert"),
    }
}

#[test]
fn alteration_is_send_and_owns_a_drop_path() {
    fn assert_send<T: Send>() {}
    assert_send::<UserScramCredentialAlteration>();
    assert!(std::mem::needs_drop::<UserScramCredentialAlteration>());
}
