//! Macro definition attempting to conceal generated Rust source expansion.

macro_rules! hidden_generated_source {
    () => {
        include!(concat!(env!("OUT_DIR"), "/hidden.rs"));
    };
}
