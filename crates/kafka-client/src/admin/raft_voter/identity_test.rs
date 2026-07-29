//! Stable metadata-quorum voter identity tests.

use super::RaftVoterIdentity;

#[test]
fn identity_preserves_signed_voter_and_exact_directory_uuid() {
    let directory_id = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ];
    let identity = RaftVoterIdentity::new(-17, directory_id);

    assert_eq!(identity.voter_id(), -17);
    assert_eq!(identity.directory_id(), directory_id);
    assert_eq!(identity.into_parts(), (-17, directory_id));
}
