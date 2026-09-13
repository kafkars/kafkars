<p align="center">
  <img src="crates/kafkars/assets/kafkars-logo.svg" alt="kafkars — native Rust client for Apache Kafka®" width="780">
</p>

# kafkars

`kafkars` is a deterministic, runtime-neutral Rust client for Apache Kafka®.
It treats deadlines, retained bytes, operation completion, cancellation, and
delivery certainty as explicit ownership rather than incidental runtime
behavior.

## Status

Version `0.0.2-rc.2` is a release-candidate source preview for API and registry
integration evaluation. It is not a production-supported release or general
broker-compatibility claim.

The source includes concrete producer, direct, group, and share-group consumer,
admin, transaction, metrics, security, and shutdown APIs. There is no stable
API promise. External [Testlab](https://github.com/kafkars/testlab) is the Kafka
real-broker qualification authority; this repository chooses pull-request or
release qualification, retains the resulting evidence, and applies the required
gate. Record-fidelity coverage carries an explicit producer timestamp through
the public delivery receipt, an independent Kafka observation, and the public
assigned-consumer record. Automatic keyed-routing coverage separately omits an
explicit partition and binds Kafkars's public receipt, independent broker
placement, and direct consumer result to a Java-compatible partition oracle.
Complete receipt coverage resolves a nonzero topic UUID through public Admin,
applies it to each ordinary record before admission, and requires the public
topic, UUID, optional leader epoch, partition, offset, timestamp, and nullable
serialized key/value sizes to agree with scenario and independent Kafka truth.
The paired records distinguish a null field from present empty bytes.
Cluster-identity coverage requires every baseline client creation command in
scenario order, including the exact optional `ClientBuilder::expected_cluster_id`
guard. The release scenario rejects the wrong ID, reuses the same client
identity with the correct guard, and joins public Admin to independent cluster
metadata.
Producer-admission coverage selects both immediate `Producer::try_send` and
bounded FIFO `Producer::send`; the waiting-send scenario retains that exact
public method through the Testlab command and independently observes its record
in Kafka. Cancellation coverage carries the same selection through the command,
invokes the matching public observer twice, and preserves stage uncertainty
through terminal and independent broker truth.
Producer-configuration coverage retains one exact complete policy command and
selects both `ClientBuilder::producer_config` and the equivalent
`producer_delivery_timeout`, `producer_compression`, `producer_retry`, and
`producer_limits` setters across the five compression scenarios. Ordinary
producer evidence still supplies the independent broker-visible result.
Every ordinary producer call preserves its exact ordered `try_send`, waiting
`send`, or `send_batch` command, including producer and operation identities,
partition and topic-UUID choices, and complete record input. Individual calls
cannot substitute for a requested batch.
Configured direct-consumer coverage requires one exact client-creation command
containing immutable read isolation, complete broker Fetch policy, and all
retained-delivery limits. The read-committed scenario keeps its aborted
transaction and visible record independently observable.
Direct beginning-assignment coverage preserves every exact
`AssignedConsumer::try_replace_assignment` request in scenario order, including
the single topic-partition path and caller-ordered batch path with its complete
deadline. Substituted, reordered, or duplicate requests cannot qualify.
Direct-consumer delivery coverage selects both waiting
`AssignedConsumer::recv` and immediate `AssignedConsumer::try_take_batch`;
Testlab requires every exact consumer, receive identity, observer, and timeout
command once in scenario order, then joins the returned batch to an independently
observed Kafka record.
Owned-record coverage converts that batch through both
`RecordBatch::into_owned().into_records()` and the direct
`RecordBatch::into_owned_records()` path. Each transfers one
`OwnedConsumerRecord` into an ordinary producer record and keeps the non-clone
`RetainedSourceRecord` readable through the independently checked destination
terminal without replacing any source header.
Fetch-evidence coverage retains the batch's broker-issued topic UUID, requested
and next offsets, log bounds, high watermark, byte charge, and checkpoint, then
joins them to independent Testlab topic-identity, watermark, and record facts.
Direct-consumer event coverage selects both waiting
`AssignedConsumer::next_event` and immediate
`AssignedConsumer::try_take_event`; an independently applied topic READ deny
must surface the exact public position fence and `Broker(29)` failure before
policy removal restores an independently observed record.
Explicit child-ownership coverage builds two private producers from one client
configuration, closes one without stopping its sibling, replaces a closed
producer, and proves two private directly assigned consumers retain separate
cursors over the same records. Every ordinary producer and assigned consumer
also requires one exact creation command retaining its client, child identity,
and shared or independent public construction path.
Multi-topic group coverage passes two caller-ordered topics through the public
builder, observes both assignments, and commits one independently matched
record from each topic under both classic and KIP-848 membership.
Every group member also requires one exact registration command retaining its
client, member, and group identities, caller-ordered topics, selected protocol,
and complete optional public policy.
Group-transition coverage selects both waiting `Consumer::next_event` and
immediate `Consumer::try_take_event` while stable assignments settle. Testlab
retains the exact observer choice for classic and KIP-848 consumers beside
their public transition, assignment, and independent broker evidence.
Hosted group delivery coverage selects both waiting `Consumer::recv` and
immediate `Consumer::try_take_batch`; Testlab requires every exact consumer,
receive identity, observer, checkpoint conversion, processing plan, and timeout
command once in scenario order while both paths commit and join their records to
independent Kafka evidence.
Multi-member qualification likewise fixes each aggregate receive identity,
caller-ordered live consumer set, structural record count, and timeout exactly
once before the adapter round-robins public receives and commits their fenced
checkpoints. Expected record identities remain harness-only.
Full-batch checkpoint coverage separately retains both canonical
`ConsumerBatch::checkpoint` and compatibility
`ConsumerBatch::into_checkpoint`; the classic round trip selects the alias and
the KIP-848 round trip retains the canonical path.
Processing-liveness coverage retains one batch beyond its original configured
processing window, calls `Consumer::acknowledge` midway with an
assignment-fenced checkpoint, and commits that same exact record under both
classic and KIP-848 membership.
Partial-checkpoint coverage receives two ordered records in one retained batch,
marks only the processed prefix through `CheckpointBuilder::mark_processed`,
independently proves the prefix offset, and requires a replacement classic or
KIP-848 member to receive the exact unprocessed suffix.
Transactional-producer initialization requires one exact command retaining the
client, producer, and transactional identities plus the selected
`TransactionalProducerBuilder::transaction_timeout` and `deadline_after`
values.
The authorization-recovery scenario must reuse the same complete construction
after the denied attempt.
Transaction staging coverage preserves the distinction between per-record
`Transaction::send` and one homogeneous `Transaction::send_batch`. Dedicated
commit and abort scenarios retain the exact method and caller-ordered record set,
derive each staged offset from the public batch acknowledgment, and join the
outcome to independent read-committed Kafka evidence.
Topic-identity coverage obtains nonzero UUIDs through public Admin, applies
`Record::expected_topic_uuid` to ordinary and staged records, checks ordinary
receipts directly, and waits for `Transaction::validate_for_commit`; it requires
the sealed IDs to match independent Kafka evidence before commit.
Multi-topic Share coverage passes two caller-ordered topics through the public
builder, observes assignments for both topics, and accepts one
independently matched exact record from each.
Every Share member also requires one exact registration command retaining its
identities, caller-ordered topics, optional rack, membership and close
deadlines, and complete optional acquisition policy.
Every `ShareConsumer::recv` call likewise requires one exact command retaining
its consumer, linear batch identity, timeout, multiplicity, and scenario order
before the public batch and independent record evidence apply.
Explicit linear-batch drop and `ShareConsumer::try_close` qualification also
retain their exact command kind, consumer, applicable batch identity,
multiplicity, and order before redelivery or close-certainty evidence applies.
Share acknowledgement coverage selects both `ShareConsumerBatch::accept_all`
and `ShareConsumerBatch::into_acknowledgement`; the all-Accept path consumes
three exact acquisition ranges while explicit decisions retain mixed Accept,
Release, and Reject behavior.
Admin group-discovery coverage carries state and group-type filters through
both consumer-only and generic listing builders, plus protocol-type filters
through `Admin::list_groups`. Testlab retains each exact filter set and requires
the public results to match independent broker group identity and state; a
separate unfiltered listing remains the control.
Admin group-description metadata coverage selects
`include_authorized_operations(true)` through the dedicated classic, mixed
classic/KIP-848, singleton Share, and plural Share builders. Testlab retains the
exact option in each command, requires the public authorization bitfield on
every successful description, and keeps live group identity and membership
anchored to immediate Kafka CLI observations. Paired Streams-group lifecycles
select authorization metadata, full topology description, and stable offset
reads both true and false across the singular and plural builders.
Active-producer description likewise covers automatic leader routing and
`DescribeProducersBuilder::broker_id(1)` on every fixed broker-one
single-broker release cell, with the same immediate exact Kafka CLI state.
`DescribeConsumerGroupsBuilder` is exercised both by paired
singleton exclusion/inclusion calls and a caller-ordered mixed-protocol call.
Plural consumer-group offset alteration selects
`AlterConsumerGroupOffsetsBuilder::retention_time(Duration::from_millis(86_400_000))`.
Testlab preserves that exact non-default choice in the adapter command while
the existing ordered public outcomes, distinct baselines, and immediate
independent offsets prove the mutation; eventual expiry is not claimed.
Admin configured topic-creation coverage applies every caller-ordered entry
through `NewTopic::config` for singleton calls and successful items in a
partial-result batch. Testlab preserves each complete ordered list in the wire
command, proves the created topology and per-item outcome, then requires an
exact public configuration description and immediate independent librdkafka
value for each selected non-sensitive entry.
Admin manual topic-placement coverage constructs
`NewTopic::with_replica_assignments` from caller-ordered
`TopicReplicaAssignment` values. Testlab retains every partition index and
broker list in the command, then immediate independent metadata requires the
exact complete partition set, each replica order, a leader within its replicas,
and the full replica set in sync on three-broker release cells.
Admin manual partition-expansion coverage constructs `NewPartitions::new` and
selects `with_replica_assignments` with one caller-ordered broker list per new
partition. Testlab keeps the exact prior count outside the wire command, maps
each list to its new partition index, and requires the complete expanded topic,
exact replica order, a replica leader, and full ISR on three-broker release
cells.
Admin client-quota description coverage carries paired
`DescribeClientQuotasBuilder::strict(true)` and `strict(false)` selections for
the same exact named-user rate. Both public values match immediate independent
Kafka CLI queries. The fixture uses a simple one-component entity, so it proves
option preservation without claiming composite-entity result divergence.
Admin client-quota validation coverage selects
`AlterClientQuotasBuilder::validate_only(true)` with an exact proposed named-user
producer rate. Testlab keeps the required current rate outside the wire command,
requires a distinct public validation completion, and immediately queries
Kafka's CLI to prove the prior rate remained unchanged.
Admin delegation-token expiry coverage selects
`ExpireDelegationTokenBuilder::expire_after(Duration::ZERO)` instead of the
omitted immediate-expiry sentinel. Testlab retains that exact zero delay without
secret bytes and requires an immediate sanitized Kafka CLI query to report no
live token for the owner.
Admin topic-description metadata coverage selects
`include_authorized_operations(true)` through both `DescribeTopicsBuilder` and
`DescribeTopicsByIdBuilder`. Testlab retains the exact option, requires the
public authorization bitfield on every successful description, and keeps topic
identity and topology anchored to immediate metadata or pinned Kafka CLI
observations.
Admin topic-partition pagination coverage selects an exact
`response_partition_limit`, retains every public page boundary and returned
cursor, and separately submits each cursor only when requested. Testlab joins
the final partition aggregate to an immediate independent metadata snapshot.
Admin all-topic listing coverage selects
`ListTopicsBuilder::include_authorized_operations(true)` together with paired
`include_internal(false)` and `include_internal(true)` calls. Testlab retains
both options and every detailed public outcome, requires the authorization
bitfield on each success, and matches expected partitions to immediate metadata.
A classic group commit materializes canonical `__consumer_offsets`; metadata
proves it exists around both public calls, which first omit it, then include it
with the internal marker. This does not claim exhaustive listing, independently
verified topic IDs, replica topology, or internal-marker correctness beyond
that canonical topic.
Admin cluster-description coverage retains both
`DescribeClusterBuilder::include_authorized_operations` and
`include_fenced_brokers`. On a disposable three-broker cell, Testlab describes
the full baseline, gracefully stops broker 3, and pairs fenced-broker exclusion
and inclusion. The disabled result must omit broker 3; the enabled result must
return it with the public fenced marker; both must match an immediate
independent active-broker snapshot. A restart restores the complete membership.
Admin transaction-discovery coverage retains all four
`ListTransactionsBuilder` selectors: caller-ordered state and signed producer
IDs, minimum running duration, and Kafka-owned transactional-ID pattern. Each
filtered result must strictly narrow an earlier nonempty unfiltered public
baseline while an immediate pinned CLI query proves the complete transaction
set is unchanged. State, producer-ID, and duration selectors run across their
compatible broker cells; pattern selection stays on Kafka 4.3 gating cells.
Admin offset-selection coverage carries `OffsetSpec::earliest`,
`OffsetSpec::latest`, `OffsetSpec::max_timestamp`, and
`OffsetSpec::for_timestamp` through exact Testlab commands. Record-timestamp
selection joins the public offset and `ListOffsetsResultInfo::timestamp_ms()`
to exact independent broker records and bounding watermarks. The fixtures put
the greatest timestamp before a later lower timestamp and a caller-selected
timestamp after an earlier lower timestamp, so boundary substitutions cannot
pass. A paired fixture also retains exact
`ListOffsetsBuilder::read_isolation` selections: `ReadUncommitted` for the
earliest query and `ReadCommitted` for the latest query, with both results
matched to immediate independent watermarks. It uses ordinary committed
records. A separate caller-ordered batch retains one exact `ReadUncommitted`
selection for the whole request and matches every result to contiguous
independent watermarks. Neither fixture claims unresolved-transaction
last-stable-offset behavior.
Incremental configuration coverage selects `ConfigAlteration::set`, `delete`,
`append`, and `subtract` through both topic-specific and generic-resource Admin
builders. Delete sends no value, while Append and Subtract send only their list
operand; named baselines and immediate independent reads retain the distinct
final broker state outside those commands.
DescribeConfigs metadata coverage selects `include_synonyms(true)` and
`include_documentation(true)` through both public builders. Testlab retains each
selected `ConfigEntry` value, read-only/source/sensitive facts, ordered
`ConfigSynonym` values and sources, configuration type, and documentation while
independent reads continue to prove the effective value.
Legacy configuration coverage first replaces two snapshots through each public
legacy surface, then selects `LegacyTopicConfigEntry::restore_default` for every
key. The Testlab commands omit the expected broker default, while immediate
independent reads must prove the distinct restored values in caller order.
Share rack-identity coverage also retains the optional public builder value and
requires exact broker-reported rack IDs through singleton and plural public
Admin descriptions.
Complete Share-configuration coverage passes non-default long-poll, byte,
record, acquisition-range, attempt-timeout, membership-start, and close values
through the public builder, then requires exact public batches and independent
broker observations.
Classic membership-timing coverage passes non-default session, rebalance,
heartbeat, and rejoin values through the public builder, then requires live
single-broker operation and recovery while each broker is disrupted in turn.
Shared group-runtime coverage passes non-default processing and membership-start
deadlines plus seek and close durations through both the individual setters and
aggregate `ConsumerBuilder::operation_config` path across classic and KIP-848
groups, then requires exact public seek replay plus orderly close.
Missing-offset coverage passes the public `OffsetReset::Error` policy through
both classic and KIP-848 builders. A new group with no committed offset must
return the correlated public `state` failure and no successful batch rather
than silently selecting an initial position; earliest and latest behavior
remain covered separately.
Consumer Fetch-policy coverage passes every broker-request and retained-delivery
capacity through the public assigned, classic, and KIP-848 builders at
non-default values, then requires exact public delivery joined to independent
broker observations.
The pinned release tier defines single-broker plaintext cells for Apache
Kafka 3.7.2, 3.8.1, 3.9.2, 4.0.2, 4.1.2, 4.2.1, and 4.3.1, plus Apache Kafka
4.3.1 three-broker plaintext, custom-root TLS, SASL/PLAIN, and
SCRAM-SHA-256/512 cells, including every SASL mechanism over TLS and a dedicated
authenticated delegation-token lifecycle cell, plus a dedicated modern
Streams-group Admin lifecycle cell. A configured cell or running workflow is
not a qualification result; only archived passing evidence for the exact
client commit and cell is eligible for a compatibility claim. This preview is
Rust-only; a future foreign interface will require its own versioned contract
and qualification.
See [support and compatibility](SUPPORT.md) for the exact boundary.

## Use

```toml
[dependencies]
kafkars = "=0.0.2-rc.2"
```

```rust
use kafkars::{Client, Result};

fn client() -> Result<Client> {
    Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .client_id("orders-api")
        .build()
}
```

The crate root is deliberately limited to `Client`, `Producer`, `Consumer`,
`Admin`, `Error`, and `Result`. Supporting vocabulary lives under the owning
`admin`, `client`, `consumer`, `error`, `metrics`, `producer`, `security`,
`topic`, and `transaction` modules. Client construction remains
`Client::builder()`; its concrete builder is `client::ClientBuilder`.

Compile-checked examples cover the
[producer](crates/kafkars/examples/producer.rs),
[consumer](crates/kafkars/examples/consumer.rs),
[admin](crates/kafkars/examples/admin.rs), and
[transaction](crates/kafkars/examples/transaction.rs) surfaces.

## Design

```text
kafka-client-core ----------------------+
    deterministic policy                |
                                         v
kafka-wire -----------> kafka-driver -> kafka-client-engine ---> kafkars
    protocol bytes        RPC and I/O      integration owner      Rust facade
```

The core owns semantic time, retained-byte accounting, cancellation, and
terminal decisions without owning networking or an async runtime. Each client
owns one private native reactor thread through the engine. Explicit independent
producer and directly assigned consumer builders start an additional private
engine from the same client configuration, giving each successful build its own
close and cursor owner. Runtime-neutral means that Kafkars embeds no async
executor; it does not mean threadless. The engine also owns bounded execution,
protocol adaptation, shutdown, and recovery.
`kafkars` exposes the curated public Rust API.

`kafka-client-sim` supplies virtual-time execution, and
`kafka-client-guardrails` enforces repository and architecture policy. Most
applications should depend only on `kafkars`.

## Validate

Rust `1.88.0`, Git, and Bash are required. From a clean clone:

```sh
./scripts/check
```

Cargo resolves exact published `kafka-driver 0.1.0-rc.6` and `kafka-wire
0.1.0-rc.3` packages from crates.io. `Cargo.lock` binds their registry sources
and checksums. For the root and engine manifest edges, the guardrails reject
local paths, Git dependencies, alternate registries, aliases, and manifest
`[patch]` or `[replace]` overrides.

## Known limitations

- This RC makes no production-support claim for any Kafka version or security
  combination.
- Mutual TLS, SASL/OAUTHBEARER, and SASL/GSSAPI are not exposed.
- This cut is Rust-only and has no stable foreign ABI or language bindings.

The reviewed driver integration now projects exact broker routes, Kafka topic
UUIDs, and configured client IDs. These contracts have loopback integration
evidence. Any broker-compatibility claim for them must remain limited to exact
archived passing Testlab cells; configured or local evidence is insufficient.

## License

Apache-2.0.

Read [ARCHITECTURE.md](ARCHITECTURE.md) before changing ownership boundaries.
See [CONTRIBUTING.md](CONTRIBUTING.md) for participation and
[SECURITY.md](SECURITY.md) for private vulnerability reporting.

## Trademarks

Apache Kafka and the Kafka logo are trademarks of The Apache Software
Foundation. kafkars has no affiliation with and is not
endorsed by The Apache Software Foundation. See the
[Apache Kafka trademark policy](https://kafka.apache.org/community/trademark/).
