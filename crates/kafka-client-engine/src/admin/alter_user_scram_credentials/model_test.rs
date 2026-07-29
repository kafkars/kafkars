//! Secret ownership, redaction, and non-secret core-plan scenarios.

use super::{AlterUserScramCredential, AlterUserScramCredentialsRequest};

#[test]
fn constructors_preserve_non_secret_shape_and_core_plan_order() {
    let request = AlterUserScramCredentialsRequest::new(vec![
        AlterUserScramCredential::delete("alice".to_owned(), 1),
        AlterUserScramCredential::upsert("bob".to_owned(), 2, 8_192, b"password".to_vec()),
    ]);
    assert_eq!(request.alterations()[0].user(), "alice");
    assert_eq!(request.alterations()[0].iterations(), None);
    assert_eq!(request.alterations()[1].user(), "bob");
    assert_eq!(request.alterations()[1].iterations(), Some(8_192));
    assert_eq!(request.alterations()[1].password_len(), Some(8));
    assert_eq!(request.alterations()[1].salt_len(), None);

    let plan = request
        .plan()
        .unwrap_or_else(|_| panic!("non-secret core plan should be valid"));
    assert_eq!(plan.changes()[0].user(), "alice");
    assert_eq!(plan.changes()[1].user(), "bob");
    assert_eq!(
        plan.affected_users(),
        ["alice".to_owned(), "bob".to_owned()]
    );
}

#[test]
fn diagnostics_never_expose_password_or_explicit_salt() {
    let request =
        AlterUserScramCredentialsRequest::new(vec![AlterUserScramCredential::upsert_with_salt(
            "alice".to_owned(),
            1,
            4_096,
            b"password-must-not-leak".to_vec(),
            b"salt-must-not-leak".to_vec(),
        )]);
    let diagnostic = format!("{request:?}");
    assert!(diagnostic.contains("alice"));
    assert!(diagnostic.contains("password_len"));
    assert!(diagnostic.contains("salt_len"));
    assert!(!diagnostic.contains("password-must-not-leak"));
    assert!(!diagnostic.contains("salt-must-not-leak"));
}

#[test]
fn drop_path_zeroizes_plaintext_and_explicit_salt_buffers() {
    let mut alteration = AlterUserScramCredential::upsert_with_salt(
        "alice".to_owned(),
        2,
        16_384,
        b"password".to_vec(),
        b"0123456789abcdef".to_vec(),
    );
    alteration.zeroize_secrets_for_test();
    match &alteration {
        AlterUserScramCredential::Upsert { password, salt, .. } => {
            assert!(password.is_empty());
            assert!(salt.as_ref().is_some_and(Vec::is_empty));
        }
        AlterUserScramCredential::Delete { .. } => panic!("upsertion expected"),
    }
}

#[test]
fn secret_owners_are_send_and_require_drop() {
    fn assert_send<T: Send>() {}
    assert_send::<AlterUserScramCredential>();
    assert_send::<AlterUserScramCredentialsRequest>();
    assert!(std::mem::needs_drop::<AlterUserScramCredential>());
    assert!(std::mem::needs_drop::<AlterUserScramCredentialsRequest>());
}
