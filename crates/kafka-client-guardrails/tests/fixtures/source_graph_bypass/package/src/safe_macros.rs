//! Ordinary expression macros that do not expand the Rust source graph.

macro_rules! increment {
    ($value:expr) => {
        $value + 1
    };
}

pub fn uses_safe_expression_macros(value: u8) -> bool {
    let incremented = increment!(value);
    assert!(incremented > value);
    matches!(incremented, 1..=u8::MAX)
}
