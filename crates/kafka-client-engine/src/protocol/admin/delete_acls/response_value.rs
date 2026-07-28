//! Bounded fallible copying of broker diagnostics and binding strings.

use core::num::NonZeroI16;

use kafka_client_core::DeleteAclBrokerError;

use super::{response::DeleteAclsResponseFailure, retention::bounded_diagnostic_len};

pub(super) fn broker_error(
    code: NonZeroI16,
    source: Option<&str>,
) -> Result<(DeleteAclBrokerError, usize), DeleteAclsResponseFailure> {
    let retained = bounded_diagnostic_len(source);
    let (message, bytes) = match source {
        Some(source) => {
            let (message, bytes) = copy_string(&source[..retained])?;
            (Some(message), bytes)
        }
        None => (None, 0),
    };
    Ok((
        DeleteAclBrokerError::new(
            code,
            message,
            source.is_some_and(|source| retained < source.len()),
        ),
        bytes,
    ))
}

pub(super) fn copy_string(source: &str) -> Result<(String, usize), DeleteAclsResponseFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| DeleteAclsResponseFailure::OwnedValueStorage)?;
    owned.push_str(source);
    let bytes = owned.capacity();
    Ok((owned, bytes))
}
