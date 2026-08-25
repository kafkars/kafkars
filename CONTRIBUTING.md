# Contributing

Thank you for helping with the experimental `kafkars` Rust client. Changes are
welcome, but the project is still establishing its public contract. Discuss a
large API, dependency, ownership, or repository-shape change before investing
in it.

## Set up the workspace

Install Git, Bash, and the Rust toolchain declared in `rust-toolchain.toml`,
then fetch the locked published dependencies:

```sh
cargo fetch --locked
```

The workspace uses exact crates.io release candidates for `kafka-driver` and
the `kafka-wire` crates. `Cargo.lock` records their registry sources and
checksums; local paths, Git dependencies, alternate registries, aliases, and
Cargo source overrides are rejected by the guardrail suite.

## Before changing code

Read `ARCHITECTURE.md`, `AGENTS.md`, and the `//!` contract of the owning Rust
module. Identify who owns:

- the original absolute deadline;
- every retained byte;
- terminal-completion capacity and result publication; and
- cancellation or observation abandonment.

Prefer an explicit state transition over a callback or hidden side effect. Do
not add a public abstraction solely to make an internal test convenient.

## Source and test shape

- `unsafe` is forbidden in the Rust-only workspace.
- The deterministic core must not depend on an async runtime, networking, the
  driver, or generated protocol values.
- Every Rust source file starts with a `//!` module contract.
- `lib.rs` and `mod.rs` remain declarative facades.
- Unit tests live in sibling `*_test.rs` files and are declared by the nearest
  facade.
- Dependency edges, capability ownership, test mirrors, and source-size
  exceptions require deliberate policy changes rather than silent bypasses.

While changing repository shape, run:

```sh
cargo test --locked -p kafka-client-guardrails --all-features
```

Before submitting a change, run the unchanged repository gate:

```sh
./scripts/check
```

Do not mark a failing or skipped required lane as passing. If a real-broker
test cannot run, report that qualification gap separately from deterministic
and unit-test results.

## Changes and review

Keep changes small enough to review at one ownership seam. Include focused
regression coverage for failures, deadline boundaries, recovery, and terminal
capacity when they are relevant. Update `SUPPORT.md` when a change affects a
documented limitation or claimed compatibility.

Use a Conventional Commit one-line subject with no body. Sign commits with
OpenPGP and do not add co-author trailers. The maintainer may curate accepted
changes into signed reference-history checkpoints.

Never include credentials, private endpoints, broker data, generated build
artifacts, or local dependency overrides in a contribution. Security findings
belong in the private process described in `SECURITY.md`.
