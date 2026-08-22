# Qualification evidence contract

`matrix.json` is the checked-in qualification policy. It declares every
permitted cell, its cluster size, security mode, required scenarios, and
whether a result gates its evidence set. A runner may not omit a scenario or
substitute a workflow-only cell and still qualify.

The `pr` evidence set has ten required gating cells:

- Kafka 4.3.1 runs the full three-broker plaintext profile.
- Kafka 4.2.1, 4.1.2, and 4.0.2 run plaintext compatibility smoke.
- Kafka 3.9.2, 3.8.1, and 3.7.2 run the three-broker classic profile, which is
  the full profile without KIP-848.
- Kafka 4.3.1 runs bounded `tls_custom`, `sasl_plaintext_plain`, and
  `sasl_tls_custom_scram_sha_512` security smoke. These cells require their
  applicable hostname or wrong-secret rejection scenarios.

The `nightly` evidence set has fourteen explicit cells. Kafka 4.3.1 runs full
qualification over plaintext, custom-root TLS, three SASL_PLAINTEXT
mechanisms, and three custom-root SASL_TLS mechanisms. Kafka 4.2.1, 4.1.2, and
4.0.2 run full plaintext qualification. The three Kafka 3.x classic cells are
required evidence but scheduled advisory results; every PR cell remains
gating.

The matrix deliberately has no positive system-root TLS cell. The ephemeral
brokers use a self-signed qualification CA, so platform-root TLS remains
explicitly unqualified rather than being represented by a custom-root result.
There is no release profile or release job in this workflow.

## Running a declared cell

Run a cell from a clean client checkout with the reviewed sibling pair. The
five arguments are evidence set, profile, Kafka version, security mode, and an
empty output directory outside the checkout:

```console
scripts/run-qualification pr full 4.3.1 plaintext /tmp/kafkars-pr-full
scripts/run-qualification pr security-smoke 4.3.1 tls_custom /tmp/kafkars-pr-tls
scripts/run-qualification nightly full 4.3.1 sasl_tls_custom_scram_sha_512 /tmp/kafkars-nightly-sasl-tls
scripts/run-qualification nightly classic 3.9.2 plaintext /tmp/kafkars-nightly-classic
```

The runner rejects untracked files, ignored files other than the root Cargo
target directory, worktree changes, and noncanonical tracked-path index flags.
It resolves the Kafka tag to a repository digest before launch and binds all
published broker ports to `127.0.0.1`. TLS certificates and stores are
generated per run; SASL uses public qualification-only credentials.

Each scenario appends one tab-separated event containing its stable ID,
terminal status, and elapsed milliseconds. The runner also records its own
terminal status. `scripts/render-qualification cell` combines those facts with
the exact Kafka image digest and full client, driver, and wire commit SHAs.
Missing scenarios, failed negative checks, or a failed runner remain visible
and make a gating cell fail closed. Each negative test is first resolved from a
libtest listing as exactly one ignored test, so a zero-match Cargo invocation
cannot be recorded as a pass.

Before rendering, the runner must capture the immutable image inspection,
resolved Compose configuration, readiness output, crate graph, scenario
events, client output, broker state, broker logs, and teardown result. Required
evidence that is absent or empty fails the run. Ephemeral TLS secrets are
removed only after log capture.

`scripts/render-qualification merge` revalidates every stored cell against the
checked-in policy, rejects mixed evidence sets or crate graphs and conflicting
image digests for one Kafka version, and can require the exact complete `pr` or
`nightly` set. It produces `compatibility.json`,
`COMPATIBILITY.md`, and the evidence-owned table in `SUPPORT.md`; the raw logs
remain beside those summaries in the archived cell artifacts.

The workflow runs the renderer unit suite with bytecode output disabled as a
prerequisite policy lane. The pull-request gate also downloads all ten cell
artifacts and performs a complete-set, qualified merge before archiving one
aggregate review artifact.
