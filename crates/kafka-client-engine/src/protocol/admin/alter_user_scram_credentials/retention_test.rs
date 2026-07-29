//! Request and response retained-byte accounting tests.

use kafka_wire::AlterUserScramCredentialsResponse;

use super::{
    AlterUserScramCredentialAlterationRef as Alteration, AlterUserScramCredentialsCorrelationRef,
    AlterUserScramCredentialsRequestRef,
    request::alter_user_scram_credentials_request,
    retention::{request_peak_charge, response_peak_charge},
};

const SALT: &[u8; 16] = b"0123456789abcdef";

#[test]
fn request_charge_includes_plaintext_generated_salt_and_derived_output() {
    let deletion = [Alteration::delete("alice", 1)];
    let sha_256 = [Alteration::upsert(
        "alice",
        1,
        4096,
        b"password",
        Some(SALT),
    )];
    let sha_512 = [Alteration::upsert(
        "alice",
        2,
        4096,
        b"password",
        Some(SALT),
    )];
    let deletion = charge(&deletion);
    let sha_256 = charge(&sha_256);
    let sha_512 = charge(&sha_512);
    assert!(sha_256 > deletion);
    assert!(sha_512 > sha_256);
}

#[test]
fn request_preparation_rejects_one_byte_below_preflight_charge() {
    let alterations = [Alteration::upsert(
        "alice",
        1,
        4096,
        b"password",
        Some(SALT),
    )];
    let source = AlterUserScramCredentialsRequestRef::new(&alterations);
    let required = charge(&alterations);
    let result = alter_user_scram_credentials_request(source, required - 1);
    assert!(matches!(
        result,
        Err(super::AlterUserScramCredentialsRequestFailure::RetainedBytes {
            required: actual,
            limit,
        }) if actual == required && limit == required - 1
    ));
}

#[test]
fn response_charge_grows_with_correlation_and_bounded_results() {
    let one = vec!["alice".to_owned()];
    let two = vec!["alice".to_owned(), "bob".to_owned()];
    let one_response = AlterUserScramCredentialsResponse::default();
    let mut two_response = AlterUserScramCredentialsResponse::default();
    two_response.results.resize_with(2, Default::default);
    let one = response_peak_charge(
        AlterUserScramCredentialsCorrelationRef::new(&one),
        &one_response,
    );
    let two = response_peak_charge(
        AlterUserScramCredentialsCorrelationRef::new(&two),
        &two_response,
    );
    assert!(matches!((one, two), (Some(one), Some(two)) if two > one));
}

fn charge(alterations: &[Alteration<'_>]) -> usize {
    let Some(charge) = request_peak_charge(AlterUserScramCredentialsRequestRef::new(alterations))
    else {
        panic!("bounded test request charge must fit usize");
    };
    charge
}
