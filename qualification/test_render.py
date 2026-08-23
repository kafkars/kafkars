"""Fail-closed qualification policy and evidence renderer tests."""

import argparse
import importlib.util
import json
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "qualification_render", ROOT / "qualification/render.py"
)
assert SPEC is not None and SPEC.loader is not None
RENDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RENDER)


class RendererTests(unittest.TestCase):
    def setUp(self) -> None:
        self.matrix = RENDER.load_json(ROOT / "qualification/matrix.json")
        RENDER.validate_matrix(self.matrix)

    def arguments(
        self,
        events: Path,
        *,
        evidence_set: str = "pr",
        profile: str = "full",
        kafka_version: str = "4.3.1",
        security: str = "plaintext",
        runner_status: str = "passed",
    ) -> argparse.Namespace:
        return argparse.Namespace(
            matrix=ROOT / "qualification/matrix.json",
            evidence_set=evidence_set,
            profile=profile,
            kafka_version=kafka_version,
            security=security,
            runner_status=runner_status,
            image=f"apache/kafka:{kafka_version}",
            image_digest="sha256:" + "a" * 64,
            client_sha="b" * 40,
            driver_sha="c" * 40,
            wire_sha="d" * 40,
            events=events,
        )

    def write_events(
        self,
        path: Path,
        profile: str,
        security: str,
        status: str = "passed",
    ) -> None:
        scenarios = RENDER.required_scenarios(self.matrix, profile, security)
        path.write_text(
            "".join(f"{name}\t{status}\t1\n" for name in scenarios),
            encoding="utf-8",
        )

    def build_policy_cell(
        self,
        directory: Path,
        evidence_set: str,
        policy: dict[str, object],
        *,
        status: str = "passed",
        runner_status: str = "passed",
    ) -> dict[str, object]:
        profile = str(policy["profile"])
        version = str(policy["kafka_version"])
        security = str(policy["security"])
        events = directory / f"{evidence_set}-{profile}-{version}-{security}.tsv"
        self.write_events(events, profile, security, status)
        return RENDER.build_cell(
            self.arguments(
                events,
                evidence_set=evidence_set,
                profile=profile,
                kafka_version=version,
                security=security,
                runner_status=runner_status,
            )
        )

    def test_complete_exact_evidence_qualifies(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            self.write_events(events, "full", "plaintext")
            cell = RENDER.build_cell(self.arguments(events))
        self.assertTrue(cell["qualified"])
        self.assertEqual(
            cell["duration_ms"],
            len(RENDER.required_scenarios(self.matrix, "full", "plaintext")),
        )

    def test_missing_scenario_cannot_qualify(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            events.write_text("producer_batching_partitioning\tpassed\t1\n")
            cell = RENDER.build_cell(self.arguments(events))
        self.assertFalse(cell["qualified"])
        self.assertIn("missing", {item["status"] for item in cell["scenarios"]})

    def test_failed_runner_cannot_qualify_complete_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            self.write_events(events, "full", "plaintext")
            cell = RENDER.build_cell(
                self.arguments(events, runner_status="failed")
            )
        self.assertFalse(cell["qualified"])
        self.assertEqual(cell["runner_status"], "failed")

    def test_security_negative_scenarios_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            base = self.matrix["profiles"]["security-smoke"]["scenarios"]
            events.write_text(
                "".join(f"{name}\tpassed\t1\n" for name in base),
                encoding="utf-8",
            )
            cell = RENDER.build_cell(
                self.arguments(
                    events,
                    profile="security-smoke",
                    security="sasl_tls_custom_scram_sha_512",
                )
            )
        statuses = {item["id"]: item["status"] for item in cell["scenarios"]}
        self.assertEqual(statuses["tls_hostname_rejection"], "missing")
        self.assertEqual(statuses["sasl_wrong_secret_rejection"], "missing")
        self.assertFalse(cell["qualified"])

    def test_mutable_or_malformed_provenance_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            events.write_text("", encoding="utf-8")
            arguments = self.arguments(events)
            arguments.image_digest = "latest"
            with self.assertRaisesRegex(ValueError, "exact sha256"):
                RENDER.build_cell(arguments)

    def test_libtest_listing_requires_one_exact_test(self) -> None:
        name = "public_tls_rejects_mismatched_server_identity"
        with tempfile.TemporaryDirectory() as directory:
            listing = Path(directory) / "tests.txt"
            listing.write_text(
                f"{name}: test\n1 test, 0 benchmarks\n",
                encoding="utf-8",
            )
            RENDER.validate_libtest_listing(listing, name)
            listing.write_text("0 tests, 0 benchmarks\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "found 0"):
                RENDER.validate_libtest_listing(listing, name)
            listing.write_text(f"{name}: test\n{name}: test\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "found 2"):
                RENDER.validate_libtest_listing(listing, name)
        runner = (ROOT / "scripts/run-qualification").read_text(encoding="utf-8")
        self.assertIn('"$test" -- --ignored --exact --list', runner)
        self.assertIn('scripts/render-qualification" libtest', runner)
        self.assertIn("tls-rejection-list.txt", runner)
        self.assertIn("sasl-rejection-list.txt", runner)

    def test_broker_metadata_readiness_parser_is_portable_and_exact(self) -> None:
        runner = (ROOT / "scripts/run-qualification").read_text(encoding="utf-8")
        function = re.search(
            r"(?ms)^metadata_has_exact_broker_ids\(\) \{\n.*?^\}\n",
            runner,
        )
        self.assertIsNotNone(function)
        command = (
            f"{function.group(0)}\n"
            'metadata_has_exact_broker_ids /dev/stdin "$1"'
        )
        complete = "".join(
            f"broker:{19091 + broker_id} (id: {broker_id} rack: null)\n"
            for broker_id in (1, 2, 3)
        )
        accepted = subprocess.run(
            ["bash", "-c", command, "readiness-test", "1,2,3"],
            input=complete,
            text=True,
            check=False,
        )
        self.assertEqual(accepted.returncode, 0)
        for rejected in (
            complete.replace("broker:19094 (id: 3 rack: null)\n", ""),
            complete + "broker:19095 (id: 4 rack: null)\n",
        ):
            completed = subprocess.run(
                ["bash", "-c", command, "readiness-test", "1,2,3"],
                input=rejected,
                text=True,
                check=False,
            )
            self.assertNotEqual(completed.returncode, 0)

    def test_coordinator_state_parser_accepts_exact_positive_broker_id(self) -> None:
        control = (ROOT / "qualification/control-compose").read_text(
            encoding="utf-8"
        )
        function = re.search(
            r"(?ms)^coordinator_id_from_state\(\) \{\n.*?^\}\n",
            control,
        )
        self.assertIsNotNone(function)
        self.assertIn("for _ in $(seq 1 60); do", control)
        self.assertIn("did not become queryable within 120 seconds", control)
        command = f'{function.group(0)}\ncoordinator_id_from_state "$1"'
        group = "kafkars-coordinator-restart-7"
        for state, expected in (
            (f"{group} kafka-2:19092 (2) range Stable 1\n", "2"),
            (f"{group} 3 range Stable 1\n", "3"),
        ):
            completed = subprocess.run(
                ["bash", "-c", command, "coordinator-test", group],
                input=state,
                text=True,
                check=False,
                capture_output=True,
            )
            self.assertEqual(completed.returncode, 0)
            self.assertEqual(completed.stdout.strip(), expected)
        for state in (
            "GROUP COORDINATOR (ID) STATE #MEMBERS\n",
            f"another-group kafka-2:19092 (2) range Stable 1\n",
            f"{group} kafka-2:19092 (0) range Stable 1\n",
            f"{group} kafka-2:19092 (2 range Stable 1\n",
            f"{group} kafka-2:19092 2) range Stable 1\n",
            f"{group} kafka-2:19092 unavailable Stable 2\n",
        ):
            completed = subprocess.run(
                ["bash", "-c", command, "coordinator-test", group],
                input=state,
                text=True,
                check=False,
                capture_output=True,
            )
            self.assertNotEqual(completed.returncode, 0)

    def test_cell_outside_declared_evidence_set_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            events.write_text("", encoding="utf-8")
            arguments = self.arguments(
                events,
                profile="compatibility-smoke",
                kafka_version="4.3.1",
            )
            with self.assertRaisesRegex(ValueError, "outside evidence set"):
                RENDER.build_cell(arguments)

    def test_merge_revalidates_stored_summary_and_runner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            self.write_events(events, "full", "plaintext")
            cell = RENDER.build_cell(self.arguments(events))
        cell["runner_status"] = "failed"
        with self.assertRaisesRegex(ValueError, "summary conflicts"):
            RENDER.validate_cell(self.matrix, cell)

    def test_merge_rejects_mixed_crate_graphs(self) -> None:
        policies = self.matrix["evidence_sets"]["pr"][:2]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.build_policy_cell(root, "pr", policies[0])
            second = self.build_policy_cell(root, "pr", policies[1])
        second["client_sha"] = "e" * 40
        with self.assertRaisesRegex(ValueError, "one exact crate graph"):
            RENDER.evidence_document([first, second])

    def test_merge_rejects_mixed_image_digests_for_one_version(self) -> None:
        policies = [
            cell
            for cell in self.matrix["evidence_sets"]["pr"]
            if cell["kafka_version"] == "4.3.1"
        ][:2]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.build_policy_cell(root, "pr", policies[0])
            second = self.build_policy_cell(root, "pr", policies[1])
        second["image_digest"] = "sha256:" + "e" * 64
        RENDER.validate_cell(self.matrix, first)
        RENDER.validate_cell(self.matrix, second)
        with self.assertRaisesRegex(ValueError, "one exact image digest"):
            RENDER.evidence_document([first, second])

    def test_merge_revalidates_source_document_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cell = self.build_policy_cell(
                root, "pr", self.matrix["evidence_sets"]["pr"][0]
            )
            document = RENDER.evidence_document([cell])
            document["qualified"] = False
            with self.assertRaisesRegex(ValueError, "summary conflicts"):
                RENDER.validate_evidence_document(
                    self.matrix, document, root / "compatibility.json"
                )

    def test_complete_set_rejects_missing_cells(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cell = self.build_policy_cell(
                Path(directory), "pr", self.matrix["evidence_sets"]["pr"][0]
            )
        evidence = RENDER.evidence_document([cell])
        with self.assertRaisesRegex(ValueError, "evidence is incomplete"):
            RENDER.require_complete_set(self.matrix, evidence, "pr")

    def test_complete_nightly_accepts_failed_advisory_cells(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells = []
            for policy in self.matrix["evidence_sets"]["nightly"]:
                advisory = not policy["gating"]
                cells.append(
                    self.build_policy_cell(
                        root,
                        "nightly",
                        policy,
                        status="failed" if advisory else "passed",
                        runner_status="failed" if advisory else "passed",
                    )
                )
        evidence = RENDER.evidence_document(cells)
        RENDER.require_complete_set(self.matrix, evidence, "nightly")
        self.assertTrue(evidence["qualified"])
        self.assertEqual(len(evidence["cells"]), 17)
        self.assertEqual(
            sum(not cell["qualified"] for cell in evidence["cells"]), 3
        )

    def test_failed_advisory_cell_does_not_claim_standalone_qualification(self) -> None:
        policy = next(
            cell
            for cell in self.matrix["evidence_sets"]["nightly"]
            if not cell["gating"]
        )
        with tempfile.TemporaryDirectory() as directory:
            cell = self.build_policy_cell(
                Path(directory),
                "nightly",
                policy,
                status="failed",
                runner_status="failed",
            )
        evidence = RENDER.evidence_document([cell])
        self.assertFalse(evidence["qualified"])

    def test_support_projection_is_generated_from_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cell = self.build_policy_cell(
                root, "pr", self.matrix["evidence_sets"]["pr"][0]
            )
            evidence = RENDER.evidence_document([cell])
            support = root / "SUPPORT.md"
            support.write_text(
                f"before\n{RENDER.BEGIN}\nold\n{RENDER.END}\nafter\n",
                encoding="utf-8",
            )
            RENDER.write_outputs(root / "output", evidence, support)
            projected = (root / "output/SUPPORT.md").read_text(encoding="utf-8")
        self.assertIn("| 4.3.1 | full | plaintext |", projected)
        self.assertNotIn("\nold\n", projected)

    def test_json_matrix_is_canonical(self) -> None:
        self.assertEqual(self.matrix["schema_version"], 2)
        self.assertEqual(
            list(self.matrix["kafka_versions"]),
            ["4.3.1", "4.2.1", "4.1.2", "4.0.2", "3.9.2", "3.8.1", "3.7.2"],
        )
        self.assertEqual(
            set(self.matrix["profiles"]),
            {"compatibility-smoke", "full", "security-smoke", "classic", "share"},
        )
        self.assertEqual(set(self.matrix["evidence_sets"]), {"pr", "nightly"})
        self.assertEqual(len(self.matrix["evidence_sets"]["pr"]), 13)
        self.assertTrue(
            all(cell["gating"] for cell in self.matrix["evidence_sets"]["pr"])
        )
        self.assertEqual(len(self.matrix["evidence_sets"]["nightly"]), 17)
        self.assertEqual(
            self.matrix["profiles"]["full"]["securities"],
            [
                "plaintext",
                "tls_custom",
                "sasl_plaintext_plain",
                "sasl_plaintext_scram_sha_256",
                "sasl_plaintext_scram_sha_512",
                "sasl_tls_custom_plain",
                "sasl_tls_custom_scram_sha_256",
                "sasl_tls_custom_scram_sha_512",
            ],
        )
        self.assertEqual(
            self.matrix["profiles"]["security-smoke"]["securities"],
            [
                "tls_custom",
                "sasl_plaintext_plain",
                "sasl_tls_custom_scram_sha_512",
            ],
        )
        for profile in ("full", "security-smoke"):
            for security in self.matrix["profiles"][profile]["securities"]:
                scenarios = RENDER.required_scenarios(
                    self.matrix, profile, security
                )
                if "tls" in security:
                    self.assertIn("tls_hostname_rejection", scenarios)
                if "sasl" in security:
                    self.assertIn("sasl_wrong_secret_rejection", scenarios)
        self.assertIn(
            "kip848_multi_member_commit_shutdown_resume",
            self.matrix["profiles"]["full"]["scenarios"],
        )
        self.assertNotIn(
            "kip848_multi_member_commit_shutdown_resume",
            self.matrix["profiles"]["classic"]["scenarios"],
        )
        self.assertNotIn(
            "kip848_initial_assignment",
            self.matrix["profiles"]["full"]["scenarios"],
        )
        self.assertEqual(len(self.matrix["profiles"]["full"]["scenarios"]), 14)
        self.assertEqual(len(self.matrix["profiles"]["classic"]["scenarios"]), 13)
        self.assertEqual(
            self.matrix["profiles"]["share"]["scenarios"],
            ["share_group_acknowledgement_lifecycle"],
        )
        expected_share = ["4.3.1", "4.2.1", "4.1.2"]
        for evidence_set in ("pr", "nightly"):
            self.assertEqual(
                [
                    cell["kafka_version"]
                    for cell in self.matrix["evidence_sets"][evidence_set]
                    if cell["profile"] == "share"
                ],
                expected_share,
            )
        self.assertEqual(
            self.matrix["profiles"]["classic"]["scenarios"],
            [
                scenario
                for scenario in self.matrix["profiles"]["full"]["scenarios"]
                if scenario != "kip848_multi_member_commit_shutdown_resume"
            ],
        )
        self.assertNotIn(
            "fetch_survives_leader_movement",
            self.matrix["profiles"]["full"]["scenarios"],
        )
        self.assertIn(
            "classic_member_shutdown_commit_resume",
            self.matrix["profiles"]["full"]["scenarios"],
        )
        self.assertNotIn(
            "classic_member_death_commit_resume",
            self.matrix["profiles"]["full"]["scenarios"],
        )
        truthful_scenarios = {
            "producer_delivers_after_leader_movement",
            "consumer_fetch_recovers_across_leader_movement",
            "producer_cancellation_preserves_delivery_certainty",
            "cluster_usable_after_broker_restart",
            "group_usable_after_coordinator_restart",
        }
        stale_scenarios = {
            "producer_retries_leader_movement",
            "producer_cancellation_ambiguous_delivery",
            "broker_restart_metadata_refresh",
            "coordinator_loss_leader_change",
        }
        for profile in ("full", "classic"):
            scenarios = set(self.matrix["profiles"][profile]["scenarios"])
            self.assertTrue(truthful_scenarios.issubset(scenarios))
            self.assertTrue(stale_scenarios.isdisjoint(scenarios))

    def test_workflow_include_lists_match_policy_exactly(self) -> None:
        workflow = (ROOT / ".github/workflows/qualification.yml").read_text(
            encoding="utf-8"
        )
        pr_section = workflow.split("  qualification-pr:", 1)[1].split(
            "\n  qualification-gate:", 1
        )[0]
        nightly_section = workflow.split("  qualification-matrix:", 1)[1].split(
            "\n  qualification-aggregate:", 1
        )[0]
        aggregate_section = workflow.split("  qualification-aggregate:", 1)[1]
        row = re.compile(
            r"^\s+- \{ kafka_version: ([^,]+), profile: ([^,]+), "
            r"security: ([^,}]+)(?:, gating: (true|false))? \}$",
            re.MULTILINE,
        )
        pr_cells = [match.groups()[:3] for match in row.finditer(pr_section)]
        expected_pr = [
            (cell["kafka_version"], cell["profile"], cell["security"])
            for cell in self.matrix["evidence_sets"]["pr"]
        ]
        self.assertEqual(pr_cells, expected_pr)
        nightly_cells = [
            (*match.groups()[:3], match.group(4) == "true")
            for match in row.finditer(nightly_section)
        ]
        expected_nightly = [
            (
                cell["kafka_version"],
                cell["profile"],
                cell["security"],
                cell["gating"],
            )
            for cell in self.matrix["evidence_sets"]["nightly"]
        ]
        self.assertEqual(nightly_cells, expected_nightly)
        self.assertNotIn("release-crate-graph", workflow)
        self.assertIn("--require-complete-set pr", workflow)
        self.assertIn("--require-complete-set nightly", workflow)
        self.assertIn('test "$QUALIFICATION_RESULT" = success', workflow)
        self.assertIn('test "$POLICY_RESULT" = success', workflow)
        self.assertIn("name: qualification-pr-aggregate-", workflow)
        self.assertIn('if [[ "$POLICY_RESULT" != success ]]', aggregate_section)
        self.assertNotIn("MATRIX_RESULT", aggregate_section)
        architecture_gate = (ROOT / "scripts/check-architecture").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "PYTHONDONTWRITEBYTECODE=1 python3 -m unittest qualification.test_render -v",
            architecture_gate,
        )

    def test_compose_and_tls_assets_keep_security_boundaries(self) -> None:
        cluster = (ROOT / "qualification/compose/cluster.yml").read_text(
            encoding="utf-8"
        )
        smoke = (ROOT / "qualification/compose/pr-smoke.yml").read_text(
            encoding="utf-8"
        )
        for compose in (cluster, smoke):
            self.assertNotIn("SHARE_COORDINATOR_STATE_TOPIC", compose)
            published = [
                line.strip()
                for line in compose.splitlines()
                if re.match(r'^\s+- "[^\"]+:\d+"$', line)
            ]
            self.assertTrue(published)
            self.assertTrue(all(line.startswith('- "127.0.0.1:') for line in published))
        tls = (ROOT / "qualification/tls.cnf").read_text(encoding="utf-8")
        self.assertIn("DNS.1 = localhost", tls)
        self.assertNotRegex(tls, r"(?m)^IP\.\d+\s*=")

    def test_share_profile_enables_one_bounded_v1_cluster(self) -> None:
        overlay = (ROOT / "qualification/compose/share.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(
            overlay.count(
                "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS: "
                "classic,consumer,share"
            ),
            1,
        )
        self.assertIn("KAFKA_GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS: 1000", overlay)
        self.assertIn("KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS: 2000", overlay)
        self.assertNotIn("ports:", overlay)

        runner = (ROOT / "scripts/run-qualification").read_text(encoding="utf-8")
        self.assertIn("security-smoke | full | classic | share", runner)
        self.assertIn("test_name=share_matrix", runner)
        self.assertIn('compose/share.yml")', runner)
        self.assertIn("--feature share.version=1", runner)
        self.assertIn('required_evidence+=(share-feature.txt)', runner)

    def test_combined_sasl_tls_uses_one_complete_secret_mount(self) -> None:
        runner = (ROOT / "scripts/run-qualification").read_text(encoding="utf-8")
        self.assertIn('export KAFKA_TLS_DIR="$secret_dir"', runner)
        self.assertIn('export KAFKA_SASL_DIR="$secret_dir"', runner)
        self.assertLess(runner.index("trap cleanup EXIT"), runner.index("mktemp -d"))
        self.assertLess(runner.index("trap cleanup EXIT"), runner.index("generate-tls"))
        self.assertLess(
            runner.index("trap cleanup EXIT"),
            runner.index('cp "$repo_root/qualification/sasl/broker_jaas.conf"'),
        )

        with tempfile.TemporaryDirectory() as directory:
            common = str(Path(directory).resolve())
            environment = os.environ.copy()
            environment.update(
                {
                    "IMAGE": "apache/kafka:4.3.1",
                    "KAFKA_EXTERNAL_PROTOCOL": "SASL_SSL",
                    "KAFKA_TLS_DIR": common,
                    "KAFKA_TLS_PASSWORD": "qualification-test-password",
                    "KAFKA_SASL_DIR": common,
                }
            )
            command = [
                "docker",
                "compose",
                "-f",
                str(ROOT / "qualification/compose/cluster.yml"),
                "-f",
                str(ROOT / "qualification/compose/cluster-tls.yml"),
                "-f",
                str(ROOT / "qualification/compose/cluster-sasl.yml"),
                "config",
                "--format",
                "json",
            ]
            completed = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                env=environment,
            )
        rendered = json.loads(completed.stdout)
        for service in rendered["services"].values():
            mounts = [
                volume
                for volume in service["volumes"]
                if volume["target"] == "/etc/kafka/secrets"
            ]
            self.assertEqual(len(mounts), 1)
            self.assertEqual(mounts[0]["source"], common)
            self.assertTrue(mounts[0]["read_only"])
            broker_environment = service["environment"]
            self.assertEqual(
                broker_environment["KAFKA_SSL_KEYSTORE_LOCATION"],
                "/etc/kafka/secrets/kafka.keystore.p12",
            )
            self.assertEqual(
                broker_environment["KAFKA_SSL_TRUSTSTORE_LOCATION"],
                "/etc/kafka/secrets/kafka.truststore.p12",
            )
            self.assertEqual(
                broker_environment["KAFKA_SSL_KEYSTORE_PASSWORD"],
                "qualification-test-password",
            )
            self.assertNotIn("KAFKA_SSL_KEYSTORE_FILENAME", broker_environment)
            self.assertEqual(
                broker_environment["KAFKA_OPTS"],
                "-Djava.security.auth.login.config=/etc/kafka/secrets/broker_jaas.conf",
            )
            self.assertTrue(
                broker_environment["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"].endswith(
                    "EXTERNAL:SASL_SSL"
                )
            )


if __name__ == "__main__":
    unittest.main()
