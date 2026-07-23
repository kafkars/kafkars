//! An unconditional local definition authorizes later innocuous invocation.

macro_rules! harmless {
    () => {};
}

harmless!();
