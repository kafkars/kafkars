//! Conservative retained-memory charge shared by admission and result normalization.

pub(crate) const RESULT_DIAGNOSTIC_BYTES_PER_TOPIC: usize = 1024;

const BASE_OWNER_BYTES: usize = 8 * 1024;
const ENTRY_OWNER_BYTES: usize = 2 * 1024;
const ENGINE_TEXT_COPIES: usize = 3;

pub(crate) fn request_charge(
    topic_count: usize,
    config_count: usize,
    text_bytes: usize,
) -> Option<usize> {
    let entries = topic_count.checked_add(config_count)?;
    let diagnostics = topic_count.checked_mul(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)?;
    BASE_OWNER_BYTES
        .checked_add(entries.checked_mul(ENTRY_OWNER_BYTES)?)?
        .checked_add(text_bytes.checked_mul(ENGINE_TEXT_COPIES)?)?
        .checked_add(diagnostics)
}

pub(crate) fn result_fixed_charge(topic_count: usize, topic_bytes: usize) -> Option<usize> {
    BASE_OWNER_BYTES
        .checked_add(topic_count.checked_mul(ENTRY_OWNER_BYTES)?)?
        .checked_add(topic_bytes.checked_mul(2)?)
}
