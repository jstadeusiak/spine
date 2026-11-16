import contextlib
import copy
import csv
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import collector_ingest_audit as audit


class Fixture:
    def __init__(self, directory, mode="boundaries"):
        self.directory, self.mode = directory, mode
        artifact = b"retained ingestion diagnostic fixture\n"
        sha = hashlib.sha256(artifact).hexdigest()
        (directory / "artifacts").mkdir(parents=True)
        (directory / "artifacts" / (sha + ".jar")).write_bytes(artifact)
        self.manifest = dict(audit.FIXED, bundleSha256=sha, observerMode=mode, java="fixture Java 21", os="fixture OS",
                             processors=16, maxHeapBytes=536870912, aeron="1.53.0", agrona="2.6.0", sbe="1.38.1",
                             driver="SHARED", termBytes=1048576, ipcMtuBytes=1024, archiveFileSyncLevel=0,
                             clusterNodes=1, clusterSessionTimeoutSeconds=120, producerRetryNanos=10000000,
                             ownerIdle="Backoff(100,10,1000,100000)", originNanos=1000)
        self.manifest["fixedSourceProducerBindings"] = [{"sourceId": 7, "producerId": 10}, {"sourceId": 8, "producerId": 11}]
        self.start = 100_000_000
        self.frames, self.commands, reports = [], [], []
        canonical_sequence = 0
        for source, (batch, count, payload) in audit.PLAN.items():
            for index in range(count + 2):
                canonical_sequence += 1
                intended = self.start + min(max(0, index - 2) * batch, 900) * 1_000_000_000 // 150
                receive = intended + 100 if index >= 2 else self.start - 1000
                complete, ready = receive + 20, receive + 30
                command = dict(source=source, kind="definition" if index == 0 else "acknowledgement" if index == 1 else "data",
                               producerId=source + 3, producerSequence=index + 1, frame=index,
                               receiveNanos=-1 if index == 0 else receive, completeNanos=-1 if index == 0 else complete,
                               readyNanos=ready, recordedConsumerObservedNanos=ready + 100, canonicalSequence=canonical_sequence)
                command.update(dict(capturePublicationObservedNanos=ready + 10, captureEndPosition=(index + 1) * 256,
                                    captureFenceLowerNanos=ready + 10, captureFenceUpperNanos=ready + 40,
                                    canonicalPublicationObservedNanos=ready + 50, canonicalEndPosition=canonical_sequence * 256,
                                    outputFenceLowerNanos=ready + 50, outputFenceUpperNanos=ready + 80)
                               if mode == "boundaries" else dict.fromkeys(audit.BOUNDARY_COLUMNS))
                self.commands.append(command)
                if index >= 2:
                    frame = dict(command)
                    frame.pop("kind")
                    frame.update(phase=audit.phase((index - 2) * batch), firstTradeId=100000 + (index - 2) * batch,
                                 trades=batch, payloadBytes=payload, intendedNanos=intended, attemptedNanos=intended + 5,
                                 writeFlushCompletedNanos=intended + 15)
                    self.frames.append(frame)
            reports.append(dict(sourceId=source, plannedTrades=1028, plannedFrames=count, attemptedFrames=count,
                                completedWrites=count, completeSubmittedCommands=count + 2,
                                admittedDataFramesFromCanonicalOutput=count, applicationPayloadBytes=payload,
                                rawBytesPerDataFrame=payload + (source == 8), recordedFollowerCommands=count + 2,
                                capturePublicationCommands=count + 2 if mode == "boundaries" else 0,
                                captureRecordingFences=count + 2 if mode == "boundaries" else 0,
                                canonicalPublicationCommands=count + 2 if mode == "boundaries" else 0,
                                outputRecordingFences=count + 2 if mode == "boundaries" else 0, phases=[]))
        self.report = dict(status="passed", failure=None, observerMode=mode, measurementStartNanos=self.start,
                           drainCompletedNanos=self.start + 6_000_000_500, canonicalEventsObserved=1289,
                           completeCanonicalHead=1289, acknowledgements=2, receiptDrainComplete=True,
                           snapshotRecordingFenceComplete=True, sources=reports)
        if mode == "boundaries":
            self.report["observer"] = dict(polls=10000, totalPollNanos=500000, maxPollNanos=1000, maxOwnerLoopGapNanos=2000,
                recordings=[dict(source=source, streamId=stream, recordingId=index, sessionId=100 + index,
                                 startPosition=0, counterId=index, counterRegistrationId=10 + index, archiveId=1)
                            for index, (source, stream) in enumerate(((0, 1001), (7, 8210), (8, 8211)))])
            for recording in self.report["observer"]["recordings"]:
                recording["completeCommands"] = recording["recordingFences"] = 1289 if recording["source"] == 0 else audit.PLAN[recording["source"]][1] + 2
        self.recompute()

    def recompute(self):
        for source in self.report["sources"]:
            source["phases"] = []
            for phase in audit.PHASES:
                rows = [row for row in self.frames if row["source"] == source["sourceId"] and row["phase"] == phase]
                item = dict(phase=phase, plannedFrames=len(rows), plannedTrades=sum(row["trades"] for row in rows))
                item.update(audit.metrics(rows, audit.BASE_METRICS | audit.BOUNDARY_METRICS))
                source["phases"].append(item)

    def correlate(self):
        by_frame = {(row["source"], row["frame"]): row for row in self.frames}
        for command in self.commands:
            if command["kind"] == "data":
                for key in command:
                    if key != "kind":
                        command[key] = by_frame[(command["source"], command["frame"])][key]

    def write(self):
        for name, value in (("manifest.json", self.manifest), ("report.json", self.report)):
            (self.directory / name).write_text(json.dumps(value), encoding="utf-8")
        for name, rows in (("frames.csv", self.frames), ("commands.csv", self.commands)):
            with (self.directory / name).open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
        return self.directory


class AuditTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def fixture(self, mode="boundaries", name="run"):
        return Fixture(self.root / name, mode)

    def test_complete_modes_and_required_manifest_fields(self):
        for mode in ("off", "boundaries"):
            with self.subTest(mode=mode):
                result = audit.audit(self.fixture(mode, mode).write())
                self.assertEqual((result["frames"], result["trades"], result["canonicalHead"]), (1285, 2056, 1289))
                self.assertTrue(result["commandCoverage"])
        missing = self.fixture("off", "missing")
        missing.manifest.pop("observerMode")
        with self.assertRaisesRegex(ValueError, "observer mode"):
            audit.audit(missing.write())

    def test_nearest_rank_values_are_exact_and_small_p99_is_maximum(self):
        self.assertEqual(audit.stats(range(1, 151)), dict(count=150, min=1, max=150, p50=75, p99=149, p999=150))
        self.assertEqual(audit.stats([30, 10, 20]), dict(count=3, min=10, max=30, p50=20, p99=30, p999=30))
        self.assertEqual(audit.stats([]), dict(count=0, min=None, max=None, p50=None, p99=None, p999=None))

    def test_absolute_monotonic_origin_can_be_negative_but_must_be_integer(self):
        fixture = self.fixture()
        fixture.manifest["originNanos"] = -1234
        self.assertEqual(audit.audit(fixture.write())["status"], "passed")
        fixture.manifest["originNanos"] = "-1234"
        with self.assertRaisesRegex(ValueError, "originNanos"):
            audit.audit(fixture.write())

    def test_negative_cross_observer_deltas_remain_valid(self):
        fixture = self.fixture()
        for frame in fixture.frames:
            frame["captureFenceUpperNanos"] = frame["readyNanos"] + 90
            frame["outputFenceUpperNanos"] = frame["readyNanos"] + 110
        fixture.correlate()
        fixture.recompute()
        result = audit.audit(fixture.write())
        metrics = result["sources"][7]["baseline"]
        self.assertEqual(metrics["captureFenceUpperToCanonicalPublicationNanos"]["min"], -40)
        self.assertEqual(metrics["outputFenceUpperToRecordedConsumerNanos"]["min"], -10)

    def test_missing_frames_commands_and_fences_fail(self):
        for change in (lambda value: value.frames.pop(), lambda value: value.commands.pop(),
                       lambda value: value.frames[0].__setitem__("captureFenceUpperNanos", None),
                       lambda value: value.frames[0].__setitem__("outputFenceUpperNanos", -1)):
            with self.subTest(change=change):
                fixture = self.fixture(name="missing-" + str(id(change)))
                change(fixture)
                with self.assertRaises(ValueError):
                    audit.audit(fixture.write())

    def test_identity_sequence_phase_trade_and_position_failures(self):
        changes = (lambda value: value.frames[0].__setitem__("source", 9),
                   lambda value: value.frames[0].__setitem__("producerId", 11),
                   lambda value: value.frames[0].__setitem__("producerSequence", 4),
                   lambda value: value.frames[0].__setitem__("firstTradeId", 100001),
                   lambda value: value.frames[0].__setitem__("trades", 4),
                   lambda value: value.frames[0].__setitem__("phase", "baseline"),
                   lambda value: value.frames[0].__setitem__("intendedNanos", value.start + 1),
                   lambda value: value.commands[0].__setitem__("canonicalSequence", 2),
                   lambda value: value.frames[0].__setitem__("captureEndPosition", 33),
                   lambda value: value.commands[1].__setitem__("captureEndPosition", 256),
                   lambda value: value.commands[1].__setitem__("canonicalEndPosition", 256))
        for index, change in enumerate(changes):
            with self.subTest(index=index):
                fixture = self.fixture(name="identity-" + str(index))
                change(fixture)
                with self.assertRaises(ValueError):
                    audit.audit(fixture.write())

    def test_source_canonical_reorder_fails_even_with_unique_global_sequences(self):
        fixture = self.fixture()
        left, right = fixture.commands[:2]
        left["canonicalSequence"], right["canonicalSequence"] = right["canonicalSequence"], left["canonicalSequence"]
        left["canonicalEndPosition"], right["canonicalEndPosition"] = right["canonicalEndPosition"], left["canonicalEndPosition"]
        with self.assertRaisesRegex(ValueError, "reorders source"):
            audit.audit(fixture.write())

    def test_off_requires_command_table_and_positive_available_sequence(self):
        fixture = self.fixture("off")
        fixture.write()
        (fixture.directory / "commands.csv").unlink()
        with self.assertRaisesRegex(ValueError, "command table"):
            audit.audit(fixture.directory)
        fixture = self.fixture("off", "sequence")
        fixture.frames[0]["canonicalSequence"] = 0
        with self.assertRaisesRegex(ValueError, "canonicalSequence"):
            audit.audit(fixture.write())

    def test_bad_time_brackets_counter_metadata_quantiles_and_totals_fail(self):
        changes = (lambda value: value.frames[0].__setitem__("captureFenceLowerNanos", value.frames[0]["captureFenceUpperNanos"] + 1),
                   lambda value: value.frames[0].__setitem__("canonicalPublicationObservedNanos", value.frames[0]["readyNanos"] - 1),
                   lambda value: value.report["observer"]["recordings"][0].__setitem__("counterRegistrationId", -1),
                   lambda value: value.report["observer"]["recordings"][0].__setitem__("streamId", 8210),
                   lambda value: value.report["observer"]["recordings"].pop(),
                   lambda value: value.report["sources"][0]["phases"][1]["readyToRecordedConsumerNanos"].__setitem__("p99", 99),
                   lambda value: value.report.__setitem__("completeCanonicalHead", 1288),
                   lambda value: value.report.__setitem__("receiptDrainComplete", False),
                   lambda value: value.report.__setitem__("status", "failed"))
        for index, change in enumerate(changes):
            with self.subTest(index=index):
                fixture = self.fixture(name="metadata-" + str(index))
                change(fixture)
                with self.assertRaises(ValueError):
                    audit.audit(fixture.write())

    def test_optional_lower_bounds_and_off_mode_samples(self):
        fixture = self.fixture()
        for frame in fixture.frames:
            frame["captureFenceLowerNanos"] = None
            frame["outputFenceLowerNanos"] = -1
        fixture.correlate()
        fixture.recompute()
        result = audit.audit(fixture.write())
        self.assertEqual(result["sources"][7]["warmup"]["captureFenceBracketNanos"]["count"], 0)
        off = self.fixture("off", "off")
        off.frames[0]["captureFenceUpperNanos"] = 10
        with self.assertRaisesRegex(ValueError, "off mode"):
            audit.audit(off.write())

    def test_artifact_integrity_and_duplicate_json_fields_fail(self):
        fixture = self.fixture()
        fixture.write()
        artifact = next((fixture.directory / "artifacts").glob("*.jar"))
        artifact.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "digest"):
            audit.audit(fixture.directory)
        fixture = self.fixture(name="duplicate")
        fixture.write()
        (fixture.directory / "report.json").write_text('{"status":"passed","status":"failed"}', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "duplicate JSON"):
            audit.audit(fixture.directory)

    def test_comparison_is_descriptive_and_rejects_workload_changes(self):
        off, on = self.fixture("off", "off"), self.fixture("boundaries", "on")
        left, right = audit.audit(off.write()), audit.audit(on.write())
        result = audit.compare(right, left)
        self.assertIn("no causal overhead guarantee", result["meaning"])
        self.assertEqual(result["sources"][7]["baseline"]["readyToRecordedConsumerNanos"]["p99"], 0)
        broken = copy.deepcopy(right)
        broken["manifest"]["ownerIdle"] = "different"
        with self.assertRaisesRegex(ValueError, "environment differs"):
            audit.compare(left, broken)
        with self.assertRaisesRegex(ValueError, "off and boundaries"):
            audit.compare(left, left)

    def test_cli_retains_success_failures_and_pair_results(self):
        off, on = self.fixture("off", "off").write(), self.fixture("boundaries", "on").write()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(audit.main([str(off), str(on), "--compare"]), 0)
        self.assertEqual(len(json.loads(output.getvalue())["comparisons"]), 1)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(audit.main([str(off), str(self.root / "missing")]), 1)
        result = json.loads(output.getvalue())
        self.assertEqual([item["status"] for item in result["audits"]], ["passed", "failed"])


if __name__ == "__main__":
    unittest.main()
