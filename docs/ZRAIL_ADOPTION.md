# Zrail adoption

This document is the executable migration contract for moving generic source
architecture policy from `kafka-client-guardrails` into Zrail. The migration is
shadow-first: the existing guardrail crate remains authoritative until each
candidate guard has mutation-parity evidence recorded in
`docs/ZRAIL_GUARD_LEDGER.tsv`.

## Current status

Contract initialization succeeds on Zrail `0.0.3-rc.4` against the public
Kafkars repository after the Kafka dependencies move from sibling paths to
exact published release candidates. The successful command is:

```text
zrail init . --preset zsumz \
  --exclude 'crates/kafka-client-guardrails/tests/fixtures/**'
```

RC4 discovers exactly these five public workspace roots:

```text
crates/kafka-client-core
crates/kafka-client-engine
crates/kafka-client-guardrails
crates/kafka-client-sim
crates/kafkars
```

The generated contract is hardened to require module docs, declarative facades
and entrypoints, deny unsafe code, and require reasons for lint suppressions.
Kafkars has no FFI package and forbids unsafe throughout the repository, so no
unsafe ratchet or package exception is part of this contract.

No `zrail.lock` exists and no grants have been accepted. The first baseline
dry-run fails closed because analysis is incomplete: 1,228
`RUST-INCLUDE-002` ordinary path-binding issues and two `RUST-GRAPH-003`
item-position `impl_partition_view!` macro issues in
`crates/kafka-client-engine/src/protocol/consumer/group_offset_fetch/response_view.rs`.
Coverage refuses to report partial success and records 1,230 unresolved issues.
`zrail doctor` reports five packages, 6,276 Rust files, and
`analysis-incomplete`.

This is an analyzer-completeness blocker, not Cargo discovery. Do not baseline,
exclude live source, weaken macro policy, hand-edit a lock, or add a CI lane
that is known to fail in order to hide it. The first mergeable lock must be
produced by the pinned RC4 line against the real repository shape after
analysis is complete.

The migration inventory classifies all 101 top-level integration guard targets
as follows:

| Replacement scope | Count | Meaning |
| --- | ---: | --- |
| Direct | 18 | RC4 has a corresponding structural rail; mutation parity is still required. |
| Partial | 77 | RC4 can own part of the contract, but exact Kafka semantics remain custom. |
| None | 6 | RC4 has no equivalent for the protected semantic contract. |

The existing policy, all custom tests, the exact published-dependency guard,
the three Rust CI evidence lanes, and the independent Testlab qualification
workflow remain authoritative while shadow adoption is blocked.

## Release authority

Only the published release artifact is CI authority. An ambient Cargo install
or a locally built binary is not acceptable evidence.

| Field | Pinned value |
| --- | --- |
| Repository | `zsumz/zrail` |
| Version | `0.0.3-rc.4` |
| Tag | `v0.0.3-rc.4` |
| Source commit | `c76409d93efdc71be1f50b130fe77f0f8c973599` |
| Linux CI asset | `zrail-0.0.3-rc.4-x86_64-unknown-linux-gnu.tar.gz` |
| Linux CI SHA-256 | `c8913f434d046195e873d24aea36669db81b19cf8fabe422564fe770bdc0b01e` |

The current `zactionsz/setup-zrail` action accepts stable versions only, so it
cannot install this prerelease. Until that action gains reviewed prerelease
support, a future Zrail lane must download the exact asset, verify the digest
before extraction, and assert that `zrail --version` is exactly
`zrail 0.0.3-rc.4`.

## Zsumz onboarding contract

Use the zsumz preset explicitly. The Rust preset is not an accepted fallback.
Initialize with:

```bash
zrail_bin=/path/to/pinned/zrail

"$zrail_bin" init . \
  --preset zsumz \
  --exclude 'crates/kafka-client-guardrails/tests/fixtures/**'
"$zrail_bin" baseline --root . --dry-run --format json
```

The excluded tree contains deliberately malformed negative-fixture source, not
live Cargo targets. No live source, unit test, integration test, example, or
benchmark may be excluded to make onboarding pass.

Contract-only `init` writes `zrail.toml` but not `zrail.lock`. Review and
harden the generated contract before accepting any grant. The zsumz preset is
a starter, not the final Kafka contract:

| Generated setting | Required Kafkars setting | Reason |
| --- | --- | --- |
| `tests = "sibling"` | Keep | Matches the repository's sibling-test contract. |
| macro mode `deny-unreviewed` | Keep | Every expansion boundary must have reviewed authority. |
| four 300-line targets | Keep | Matches the design target; exact existing debt is ratcheted. |
| `module_docs = "allow"` | Change to `required` | Every Rust source begins with a module contract. |
| `facades = "allow"` | Change to `declarative` | `lib.rs` and `mod.rs` are declarations and reexports only. |
| `entrypoints = "allow"` | Change to `declarative` | Entrypoint behavior must remain behind owned modules. |
| `unsafe = "allow"` | Change to `deny` | Unsafe is forbidden throughout public Kafkars. |
| lint suppressions `allow` | Change to `reasoned` | Existing reviewed suppressions remain possible but unreasoned growth is denied. |
| no layer or owner rules | Add reviewed rules | The preset does not encode this repository's architecture. |

`deny-unreviewed` macro findings are not baseline-ratchetable. Add narrow,
reasoned allowances for exactly resolved ordinary macros and explicit item
macro authority for `impl_partition_view!`; never weaken the whole macro mode
to `allow`. A baseline is acceptable only after incomplete parse, include, and
item-macro analysis has been eliminated.

Then review grants and create the real lock:

```bash
"$zrail_bin" baseline --root . --dry-run --format json
# Review every proposed grant and ratchet before the next command.
"$zrail_bin" baseline --root . --accept-grants
"$zrail_bin" check --root . --max-findings all
"$zrail_bin" coverage --root . --format json
```

Commit `zrail.toml` and `zrail.lock` together only after that review. A broad
grant, partial traversal, incomplete analysis, or uncovered live target is a
failed onboarding, even if a command otherwise exits successfully.

## Shadow CI topology

After a real config and lock pass locally, add an independent `zrail` evidence
job. It must not be folded into or replace the existing `architecture` job.
The reviewed order is:

```text
check out Kafkars
install the pinned project Rust toolchain
verify and install the exact RC4 release artifact
run zrail check
write target/zrail/coverage.json
upload coverage as retained CI evidence
```

The lane becomes required when the committed contract and lock are accepted.
At that point the aggregate quality gate must inspect four results:
`architecture`, `zrail`, `rust-lint`, and `rust-test`. Testlab qualification
remains a separate product-qualification workflow and is not replaced by the
source-architecture lane.

Shadow adoption removes nothing. In particular, it retains:

- `guardrails.toml` and `kafka-client-guardrails`;
- exact published Kafka requirements and lockfile checksum checks;
- GitHub Actions topology and repository entrypoint contracts;
- pinned Testlab qualification, verdict, and retained-evidence contracts;
- package and release metadata checks;
- invariant registry and executable-evidence governance; and
- ordering, public API shape, ownership transfer, terminal completion,
  cancellation, and other Kafka state-machine semantics.

## Mutation-parity rule

`docs/ZRAIL_GUARD_LEDGER.tsv` has one row per top-level custom guard. Its
`old_result` and `new_result` fields remain pending until a deliberate mutation
has been run against both systems. A direct candidate can be removed only when:

1. its complete governed surface appears in Zrail coverage;
2. the existing guard rejects the recorded mutation;
3. Zrail rejects the same mutation and names the intended rail;
4. no broad grant or exclusion neutralizes that rail;
5. incomplete analysis fails closed; and
6. the evidence is reproducible from the exact release binary and committed
   lock.

For a partial candidate, remove only the proven structural portion and keep a
focused Kafka-semantic test. Green-to-green comparison is not parity evidence.

Migration proceeds in this order:

1. dependency, layer, namespace, facade, and source-graph rules;
2. call, constructor, type, method, field, and mutable-borrow ownership;
3. file roles, exact test mirrors, generated-source, and source authority;
4. obsolete policy tables and AST helpers made unreachable by proven parity.

## Known RC4 gaps for this repository

Initialization is complete after the RC dependency cut, but baseline and
coverage remain blocked by 1,230 fail-closed incomplete-analysis findings.
This is not the only reason the legacy crate remains:

- RC4 has no linear-type or consume/return-signature rail for single-owner
  authority and completion values.
- RC4 does not express statement ordering or source-sequence state-machine
  constraints.
- RC4 does not govern exact public enum variants or all public type-shape
  contracts.
- RC4 cannot replace workflow semantics, Testlab qualification policy, package
  publication policy, exact registry checksum policy, or the invariant
  registry's evidence model.
- Strict zsumz macro analysis needs reviewed ordinary-macro bindings and item
  macro authority before a lock can pass.

These gaps are represented as partial or retained rows in the ledger. They do
not justify weakening incomplete analysis and they block a big-bang deletion
of the custom guardrail crate.

## Zcheck boundary

Current public `main` has no `zcheck.toml`. An older unpublished branch carried
a Zcheck graph tied to sibling-provenance scripts and a Zrail `0.0.2` discovery
blocker. That graph is stale after the registry cut and is not part of this RC4
contract-only slice. The existing scripts and CI jobs remain canonical until a
separate Zcheck migration updates their exact current inputs and guards.
