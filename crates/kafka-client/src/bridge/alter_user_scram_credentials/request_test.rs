//! Public-to-engine SCRAM alteration translation and redaction tests.

use crate::admin::{ScramMechanism, UserScramCredentialAlteration};

use super::AlterUserScramCredentialsAdminRequest;

#[test]
fn delete_upsert_and_explicit_salt_translate_losslessly() {
    let request = AlterUserScramCredentialsAdminRequest::new(vec![
        UserScramCredentialAlteration::delete("alice", ScramMechanism::SHA_256),
        UserScramCredentialAlteration::upsert(
            "bob",
            ScramMechanism::SHA_512,
            8_192,
            b"password-a".to_vec(),
        ),
        UserScramCredentialAlteration::upsert_with_salt(
            "carol",
            ScramMechanism::SHA_256,
            4_096,
            b"password-b".to_vec(),
            b"explicit-salt".to_vec(),
        ),
    ])
    .into_engine();

    assert_eq!(request.alterations().len(), 3);
    assert_eq!(request.alterations()[0].user(), "alice");
    assert_eq!(request.alterations()[1].user(), "bob");
    assert_eq!(request.alterations()[2].user(), "carol");
}

#[test]
fn facade_request_diagnostics_redact_all_secret_bytes() {
    let request = AlterUserScramCredentialsAdminRequest::new(vec![
        UserScramCredentialAlteration::upsert_with_salt(
            "alice",
            ScramMechanism::SHA_512,
            8_192,
            b"password-must-not-leak".to_vec(),
            b"salt-must-not-leak".to_vec(),
        ),
    ]);

    let diagnostic = format!("{request:?}");
    assert!(!diagnostic.contains("password-must-not-leak"));
    assert!(!diagnostic.contains("salt-must-not-leak"));
}

#[test]
fn malformed_shapes_remain_inert_until_engine_submission() {
    let request =
        AlterUserScramCredentialsAdminRequest::new(vec![UserScramCredentialAlteration::upsert(
            "",
            ScramMechanism::UNKNOWN,
            0,
            Vec::new(),
        )])
        .into_engine();

    assert_eq!(request.alterations().len(), 1);
}
