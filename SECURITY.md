# Security policy

## Supported versions

There is no released or supported version of `kafkars` yet. The current
`0.0.0`, `publish = false` source tree is an experimental preview and receives
best-effort security fixes on its active development branch. No older commit,
artifact, crate, tag, C ABI, or foreign-language binding is supported.

This status is not permission to publish a vulnerability before maintainers
have had a reasonable opportunity to investigate it.

## Report a vulnerability privately

Use this repository's **Security** tab and **Report a vulnerability** to open a
private GitHub security advisory. Do not put exploit details, credentials,
private broker addresses, packet captures, or customer data in a public issue.

If private advisory reporting is unavailable, open a public issue asking for a
private contact channel without describing the vulnerability. A maintainer
will provide a private route. This preview does not promise a response or fix
deadline, but reports will be acknowledged and triaged as capacity permits.

Include, where possible:

- the exact client, driver, and wire commit IDs;
- the affected operation and security mode;
- a minimal reproduction using synthetic data;
- the confidentiality, integrity, or availability impact; and
- whether the issue is already public or under active exploitation.

## Scope

Security reports may cover the Rust workspace, the exact sibling revisions in
`dependencies/sibling-revisions.env`, credential retention and redaction,
protocol validation, memory or completion bounds, deadline and cancellation
behavior, and release or provenance tooling.

The absence of a C ABI or foreign binding is deliberate. Hypothetical foreign
interfaces and unrelated Kafka deployments are outside this repository's
current support scope. Vulnerabilities in an upstream dependency should also
be reported to that project when they are not caused by this integration.

Please keep testing non-destructive. Do not access systems or data you do not
own or have explicit permission to test.
