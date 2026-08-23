//! Allocation-bounded RFC 4122 member identity generation for share registrations.

use std::sync::Arc;

use ring::rand::{SecureRandom, SystemRandom};

pub(super) fn member_spelling() -> Result<Arc<str>, ()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut source = [0u8; 16];
    SystemRandom::new().fill(&mut source).map_err(|_error| ())?;
    source[6] = (source[6] & 0x0f) | 0x40;
    source[8] = (source[8] & 0x3f) | 0x80;
    let mut spelling = String::new();
    spelling.try_reserve_exact(36).map_err(|_error| ())?;
    for (index, byte) in source.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            spelling.push('-');
        }
        spelling.push(char::from(HEX[usize::from(byte >> 4)]));
        spelling.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(Arc::from(spelling))
}
