//! Allocation-bounded Kafka UUID member spelling for share registrations.

use std::sync::Arc;

use ring::rand::{SecureRandom, SystemRandom};

pub(super) fn member_spelling() -> Result<Arc<str>, ()> {
    let mut source = [0u8; 16];
    SystemRandom::new().fill(&mut source).map_err(|_error| ())?;
    source[6] = (source[6] & 0x0f) | 0x40;
    source[8] = (source[8] & 0x3f) | 0x80;
    encode_member(source)
}

pub(super) fn encode_member(source: [u8; 16]) -> Result<Arc<str>, ()> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut spelling = String::new();
    spelling.try_reserve_exact(22).map_err(|_error| ())?;
    for chunk in source[..15].chunks_exact(3) {
        let first = chunk[0];
        let second = chunk[1];
        let third = chunk[2];
        spelling.push(char::from(ALPHABET[usize::from(first >> 2)]));
        spelling.push(char::from(
            ALPHABET[usize::from(((first & 0x03) << 4) | (second >> 4))],
        ));
        spelling.push(char::from(
            ALPHABET[usize::from(((second & 0x0f) << 2) | (third >> 6))],
        ));
        spelling.push(char::from(ALPHABET[usize::from(third & 0x3f)]));
    }
    let final_byte = source[15];
    spelling.push(char::from(ALPHABET[usize::from(final_byte >> 2)]));
    spelling.push(char::from(ALPHABET[usize::from((final_byte & 0x03) << 4)]));
    Ok(Arc::from(spelling))
}
