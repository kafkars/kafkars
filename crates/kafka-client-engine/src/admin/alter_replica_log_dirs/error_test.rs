//! Stable admission error value tests.

use super::{AlterReplicaLogDirsAdmissionError, AlterReplicaLogDirsAdmissionErrorKind};

#[test]
fn admission_error_retains_exact_kind_and_operation_name() {
    let error = AlterReplicaLogDirsAdmissionError::new(
        AlterReplicaLogDirsAdmissionErrorKind::InvalidRequest,
    );

    assert_eq!(
        error.kind(),
        AlterReplicaLogDirsAdmissionErrorKind::InvalidRequest
    );
    assert!(error.to_string().contains("AlterReplicaLogDirs"));
}
