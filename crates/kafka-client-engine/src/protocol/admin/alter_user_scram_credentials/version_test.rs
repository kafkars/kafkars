//! Exact API/version and opaque request wire-trait tests.

use kafka_wire::{KafkaMessage, KafkaRequest};
use kafka_wire_core::{ApiVersion, BytesMut, KafkaEncode};

use super::{
    ALTER_USER_SCRAM_CREDENTIALS_MAX_VERSION, ALTER_USER_SCRAM_CREDENTIALS_MIN_VERSION,
    AlterUserScramCredentialAlterationRef, AlterUserScramCredentialsRequestRef,
    PreparedAlterUserScramCredentialsRequest, alter_user_scram_credentials_request,
};

#[test]
fn prepared_request_is_exact_api_51_v0() {
    assert_eq!(
        <PreparedAlterUserScramCredentialsRequest as KafkaRequest>::API_KEY.value(),
        51
    );
    assert_eq!(ALTER_USER_SCRAM_CREDENTIALS_MIN_VERSION, 0);
    assert_eq!(ALTER_USER_SCRAM_CREDENTIALS_MAX_VERSION, 0);
    assert!(PreparedAlterUserScramCredentialsRequest::supports(
        ApiVersion::new(0)
    ));
    assert!(!PreparedAlterUserScramCredentialsRequest::supports(
        ApiVersion::new(1)
    ));
}

#[test]
fn opaque_request_delegates_exact_flexible_v0_encoding() {
    let alterations = [AlterUserScramCredentialAlterationRef::delete("alice", 1)];
    let result = alter_user_scram_credentials_request(
        AlterUserScramCredentialsRequestRef::new(&alterations),
        4 * 1024 * 1024,
    );
    let Ok(request) = result else {
        panic!("valid request must prepare");
    };
    let length = request.encoded_len(ApiVersion::new(0));
    let Ok(length) = length else {
        panic!("exact v0 length must encode");
    };
    let mut bytes = BytesMut::new();
    let encoded = request.encode_into(&mut bytes, ApiVersion::new(0));
    assert_eq!(encoded, Ok(length));
    assert_eq!(bytes.len(), length);
    assert!(request.encoded_len(ApiVersion::new(1)).is_err());
}
