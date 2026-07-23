# Repository contract

This repository is designing a native Rust Kafka client, not merely a thin
protocol wrapper.

## Before coding

- Read `ARCHITECTURE.md` and the owning modules' `//!` contracts.
- Identify who owns time, bytes, operation completion, and cancellation.
- Prefer an explicit state transition over a callback or hidden side effect.
- Do not add a public abstraction merely to make an internal test convenient.

## Non-negotiable rules

- `unsafe` remains forbidden throughout the repository.
- No async runtime dependency enters the deterministic core.
- No operation is admitted without terminal-completion capacity.
- No public timeout starts later than the public call boundary.

## Rust source shape

- Every Rust source file begins with a `//!` module contract.
- `lib.rs` and `mod.rs` are declarative facades: module declarations and
  re-exports only.
- Unit tests live in sibling `*_test.rs` files and are explicitly declared with
  `#[cfg(test)] mod ...;` from the nearest facade.
- New dependency edges, capability ownership, and files above the design target
  require a deliberate `guardrails.toml` policy change.
- Run `cargo test -p kafka-client-guardrails` while changing repository shape.
