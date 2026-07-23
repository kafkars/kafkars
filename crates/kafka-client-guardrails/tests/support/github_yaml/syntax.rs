//! Scalar syntax accepted by the constrained GitHub YAML parser.

pub(super) fn mapping_pair(value: &str) -> Option<(&str, &str)> {
    let (key, value) = value.split_once(':')?;
    Some((key.trim(), value.trim()))
}

pub(super) fn sequence_value(value: &str) -> Option<&str> {
    value
        .strip_prefix("- ")
        .or_else(|| (value == "-").then_some(""))
}

pub(super) fn is_block_marker(value: &str) -> bool {
    matches!(value, "|" | "|-" | ">" | ">-")
}

pub(super) fn reject_yaml_indirection(value: &str, number: usize) -> Result<(), String> {
    if has_yaml_indirection(value) {
        Err(format!(
            "line {number} uses unsupported YAML anchor, alias, or tag syntax"
        ))
    } else {
        Ok(())
    }
}

pub(super) fn has_yaml_indirection(value: &str) -> bool {
    matches!(value.trim().chars().next(), Some('&' | '*' | '!'))
}

pub(super) fn normalize_scalar(value: &str, number: usize) -> Result<String, String> {
    let value = value.trim();
    if value.len() < 2 {
        return Ok(value.to_owned());
    }
    if value.starts_with('\'') && value.ends_with('\'') {
        return Ok(value[1..value.len() - 1].replace("''", "'"));
    }
    if !value.starts_with('"') || !value.ends_with('"') {
        return Ok(value.to_owned());
    }
    decode_double_quoted(&value[1..value.len() - 1], number)
}

fn decode_double_quoted(value: &str, number: usize) -> Result<String, String> {
    let mut decoded = String::new();
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            decoded.push(character);
            continue;
        }
        let escaped = characters
            .next()
            .ok_or_else(|| format!("line {number} ends a quoted scalar with an escape"))?;
        match escaped {
            '"' | '\\' | '/' => decoded.push(escaped),
            '0' => decoded.push('\0'),
            'a' => decoded.push('\u{7}'),
            'b' => decoded.push('\u{8}'),
            't' => decoded.push('\t'),
            'n' => decoded.push('\n'),
            'v' => decoded.push('\u{b}'),
            'f' => decoded.push('\u{c}'),
            'r' => decoded.push('\r'),
            'e' => decoded.push('\u{1b}'),
            '_' => decoded.push('\u{a0}'),
            'N' => decoded.push('\u{85}'),
            'L' => decoded.push('\u{2028}'),
            'P' => decoded.push('\u{2029}'),
            'x' => decoded.push(decode_hex(&mut characters, 2, number)?),
            'u' => decoded.push(decode_hex(&mut characters, 4, number)?),
            'U' => decoded.push(decode_hex(&mut characters, 8, number)?),
            _ => {
                return Err(format!(
                    "line {number} uses unsupported quoted escape `\\{escaped}`"
                ));
            }
        }
    }
    Ok(decoded)
}

fn decode_hex(
    characters: &mut impl Iterator<Item = char>,
    digits: usize,
    number: usize,
) -> Result<char, String> {
    let mut value = 0_u32;
    for _ in 0..digits {
        let digit = characters
            .next()
            .and_then(|character| character.to_digit(16))
            .ok_or_else(|| format!("line {number} has an invalid hexadecimal escape"))?;
        value = value * 16 + digit;
    }
    char::from_u32(value)
        .ok_or_else(|| format!("line {number} has an invalid Unicode scalar escape"))
}
