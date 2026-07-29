//! Fallible exact copies charged to the caller's retained-byte envelope.

use super::AlterUserScramCredentialsRequestFailure;

pub(super) fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, AlterUserScramCredentialsRequestFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

pub(super) fn copy_bytes(
    source: &[u8],
    required: usize,
    limit: usize,
) -> Result<Vec<u8>, AlterUserScramCredentialsRequestFailure> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained(required, limit))?;
    owned.extend_from_slice(source);
    Ok(owned)
}

const fn retained(required: usize, limit: usize) -> AlterUserScramCredentialsRequestFailure {
    AlterUserScramCredentialsRequestFailure::RetainedBytes { required, limit }
}
