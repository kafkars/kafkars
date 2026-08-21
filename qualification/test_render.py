"""Fail-closed qualification evidence renderer tests."""

import argparse
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("qualification_render", ROOT / "qualification/render.py")
assert SPEC is not None and SPEC.loader is not None
RENDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RENDER)


class RendererTests(unittest.TestCase):
    def arguments(self, events: Path) -> argparse.Namespace:
        return argparse.Namespace(
            matrix=ROOT / "qualification/matrix.json",
            profile="pr-smoke",
            kafka_version="4.3.1",
            security="plaintext",
            image="apache/kafka:4.3.1",
            image_digest="sha256:" + "a" * 64,
            client_sha="b" * 40,
            driver_sha="c" * 40,
            wire_sha="d" * 40,
            events=events,
        )

    def test_complete_exact_evidence_qualifies(self) -> None:
        matrix = RENDER.load_json(ROOT / "qualification/matrix.json")
        scenarios = RENDER.required_scenarios(matrix, "pr-smoke")
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            events.write_text("".join(f"{name}\tpassed\t1\n" for name in scenarios))
            cell = RENDER.build_cell(self.arguments(events))
        self.assertTrue(cell["qualified"])
        self.assertEqual(cell["duration_ms"], len(scenarios))

    def test_missing_scenario_cannot_qualify(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            events.write_text("client_readiness\tpassed\t1\n")
            cell = RENDER.build_cell(self.arguments(events))
        self.assertFalse(cell["qualified"])
        self.assertIn("missing", {item["status"] for item in cell["scenarios"]})

    def test_mutable_or_malformed_provenance_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            events.write_text("")
            arguments = self.arguments(events)
            arguments.image_digest = "latest"
            with self.assertRaisesRegex(ValueError, "exact sha256"):
                RENDER.build_cell(arguments)

    def test_merge_revalidates_stored_summary(self) -> None:
        matrix = RENDER.load_json(ROOT / "qualification/matrix.json")
        scenarios = RENDER.required_scenarios(matrix, "pr-smoke")
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            events.write_text("".join(f"{name}\tpassed\t1\n" for name in scenarios))
            cell = RENDER.build_cell(self.arguments(events))
        cell["qualified"] = False
        with self.assertRaisesRegex(ValueError, "summary conflicts"):
            RENDER.validate_cell(matrix, cell)

    def test_merge_rejects_mixed_crate_graphs(self) -> None:
        matrix = RENDER.load_json(ROOT / "qualification/matrix.json")
        scenarios = RENDER.required_scenarios(matrix, "pr-smoke")
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            events.write_text("".join(f"{name}\tpassed\t1\n" for name in scenarios))
            first = RENDER.build_cell(self.arguments(events))
            second = dict(first, security="tls", client_sha="e" * 40)
        with self.assertRaisesRegex(ValueError, "one exact crate graph"):
            RENDER.evidence_document([first, second])

    def test_complete_profile_rejects_missing_cells(self) -> None:
        matrix = RENDER.load_json(ROOT / "qualification/matrix.json")
        scenarios = RENDER.required_scenarios(matrix, "nightly")
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "events.tsv"
            events.write_text("".join(f"{name}\tpassed\t1\n" for name in scenarios))
            arguments = self.arguments(events)
            arguments.profile = "nightly"
            evidence = RENDER.evidence_document([RENDER.build_cell(arguments)])
        with self.assertRaisesRegex(ValueError, "evidence is incomplete"):
            RENDER.require_complete_profile(matrix, evidence, "nightly")

    def test_support_projection_is_generated_from_evidence(self) -> None:
        matrix = RENDER.load_json(ROOT / "qualification/matrix.json")
        scenarios = RENDER.required_scenarios(matrix, "pr-smoke")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            events = root / "events.tsv"
            events.write_text("".join(f"{name}\tpassed\t1\n" for name in scenarios))
            evidence = RENDER.evidence_document([RENDER.build_cell(self.arguments(events))])
            support = root / "SUPPORT.md"
            support.write_text(f"before\n{RENDER.BEGIN}\nold\n{RENDER.END}\nafter\n")
            RENDER.write_outputs(root / "output", evidence, support)
            projected = (root / "output/SUPPORT.md").read_text()
        self.assertIn("| 4.3.1 | pr-smoke | plaintext |", projected)
        self.assertNotIn("\nold\n", projected)

    def test_json_matrix_is_canonical(self) -> None:
        matrix_path = ROOT / "qualification/matrix.json"
        matrix = json.loads(matrix_path.read_text())
        self.assertEqual(matrix["schema_version"], 1)
        self.assertEqual(RENDER.required_scenarios(matrix, "release"), RENDER.required_scenarios(matrix, "nightly"))


if __name__ == "__main__":
    unittest.main()
