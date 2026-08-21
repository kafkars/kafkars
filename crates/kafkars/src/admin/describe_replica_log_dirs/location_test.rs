//! Public replica log-directory location accessor coverage.

use super::ReplicaLogDirLocation;

#[test]
fn location_exposes_exact_path_and_signed_lag() {
    let location = ReplicaLogDirLocation::new("/kafka-logs-2".to_owned(), -7);

    assert_eq!(location.path(), "/kafka-logs-2");
    assert_eq!(location.offset_lag(), -7);
}
