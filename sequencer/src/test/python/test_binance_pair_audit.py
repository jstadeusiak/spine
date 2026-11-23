import base64
import copy
import hashlib
import io
import json
import struct
import tempfile
import unittest
from decimal import Decimal, localcontext
from pathlib import Path
from unittest.mock import patch

import binance_pair_audit as audit

NOW = 1_791_244_800_000_000
PACKAGE = "spine."


def binary(trades, price_exp=-2, quantity_exp=-4, transaction=NOW - 10, event=NOW, symbol=b"BTCUSDT"):
    return (struct.pack("<HHHHqqbbHI", 18, 10000, 1, 0, event, transaction, price_exp, quantity_exp, 25, len(trades))
            + b"".join(struct.pack("<qqqB", *trade) for trade in trades) + bytes([len(symbol)]) + symbol)


def trade(trade_id, price="100.01", quantity="0.0025", maker=True, transaction=NOW - 10, event=NOW):
    return json.dumps(dict(e="trade", E=event, s="BTCUSDT", t=trade_id, p=price, q=quantity, T=transaction, m=maker, M=True))


class Captures:
    """Synthetic retained exports, not a substitute for a live comparison or an executable JAR test."""
    def __init__(self, path, local_synthetic=False):
        self.path = path
        self.local_synthetic = local_synthetic
        self.rows, self.current, self.frames, self.raw_bytes = [], {}, {7: 0, 8: 0}, {7: 0, 8: 0}
        self.expected = {}
        self.artifact = b"test-only retained executable fixture\n"
        self.sha = hashlib.sha256(self.artifact).hexdigest()
        self.artifact_path = path / "artifacts" / (self.sha + ".jar")
        self.artifact_path.parent.mkdir()
        self.artifact_path.write_bytes(self.artifact)

    def definition(self, source, configuration, clock):
        tagged = source == 8
        parameters = dict(format="SBE" if tagged else "JSON", timeUnit="MICROSECOND",
                          subscription=dict(method="SUBSCRIBE", params=["btcusdt@trade"], id=1),
                          applicationHeartbeat=None, rfcPingPong="JDK WebSocket implementation", java="fixture JDK 21",
                          harness=PACKAGE + "sequencer.BinancePairSmoke", exchangeTimestamps="Unix epoch microseconds",
                          taggedFraming=tagged, maxApplicationPayloadBytes=65499 if tagged else 65500)
        if tagged:
            parameters.update(sbeSchemaId=1, sbeSchemaVersion=0, sbeSchemaSha256=audit.SCHEMA_SHA256, sbeSchemaUrl=audit.SCHEMA_URL)
        if self.local_synthetic:
            parameters["provenance"] = audit.LOCAL_PROVENANCE
        profile = dict(profile="spine.capture.v1", venue="binance-spot", channel="trade",
                       parser=dict(artifact=self.artifact_path.as_uri(), sha256=self.sha,
                                   entrypoint=PACKAGE + "feeds.WebSocketCollector", parameters=parameters),
                       raw=dict(boundary="one complete WebSocket application message",
                                representation="tagged bytes: 0=UTF-8 text, 1=binary" if tagged else "UTF-8",
                                transformation="fixture framing; RFC controls excluded"),
                       native=dict(namespace="binance Spot BTCUSDT trade id t", representation="absent; identity retained in raw message"),
                       clock=dict(domain="shared process monotonic elapsed nanoseconds", origin=str(100 + clock), resolutionNanos=1,
                                  receive="first application-message callback", complete="last application-message callback assembled",
                                  ready="immediately before capture submission"))
        row = self.metadata(source, configuration, clock, -1, -1, 10)
        row.update(kind="definition", provider="binance-sbe" if tagged else "binance-public", product="BTCUSDT",
                   session=("ws-sbe-" if tagged else "ws-json-") + str(configuration), collector=PACKAGE + "feeds.WebSocketCollector",
                   profile=json.dumps(profile), clockMeaning="retained shared process relative origin", syncUncertaintyNanos=-1)
        self.current[source] = dict(configuration=configuration, clock=clock, session=row["session"], frames=0)
        self.expected.setdefault(clock, {})[source] = dict(sourceId=source, rawBase64=[], tradeIds=[])
        self.rows.append(row)

    def metadata(self, source, configuration, clock, receive, complete, ready):
        return dict(sequence=len(self.rows) + 1, logId=42, sourceId=source, configuration=configuration, clockId=clock,
                    sequencedTimeNanos=NOW * 1000 + len(self.rows), receiveTimeNanos=receive, completeTimeNanos=complete, readyTimeNanos=ready)

    def observation(self, source, raw, receive, trade_ids=()):
        state = self.current[source]
        state["frames"] += 1
        encoded = raw.encode("utf-8") if source == 7 else raw
        row = self.metadata(source, state["configuration"], state["clock"], receive, receive + 1, receive + 2)
        row.update(kind="observation", session=state["session"], rawFrameSequence=state["frames"], subeventIndex=0,
                   rawByteLength=len(encoded), nativeNumericId=audit.NULL_NATIVE_ID)
        row["raw" if source == 7 else "rawBase64"] = raw if source == 7 else base64.b64encode(raw).decode("ascii")
        self.rows.append(row)
        expected = self.expected[state["clock"]][source]
        expected["rawBase64"].append(base64.b64encode(encoded).decode("ascii"))
        expected["tradeIds"].extend(trade_ids)
        self.frames[source] += 1
        self.raw_bytes[source] += len(encoded)

    def run(self, clock, previous_head=0, start=100, end=1000):
        self.write_rows("canonical-" + str(clock) + ".jsonl", self.rows)
        self.write_rows("definitions-" + str(clock) + ".jsonl", [row for row in self.rows if row["kind"] == "definition"])
        report = dict(mode=audit.LOCAL_MODE if self.local_synthetic else audit.LIVE_MODE,
                      jsonEndpoint="ws://127.0.0.1:19231/ws" if self.local_synthetic else audit.JSON_ENDPOINT,
                      sbeEndpoint="ws://127.0.0.1:19232/ws" if self.local_synthetic else audit.SBE_ENDPOINT,
                      seconds=1, clockId=clock, canonicalHead=len(self.rows), previousCanonicalHead=previous_head,
                      measurementStartTimeNanos=start, measurementEndTimeNanos=end,
                      clusterSessionTimeoutNs=120_000_000_000,
                      definitions=sum(row["kind"] == "definition" for row in self.rows), cumulativeFrames=sum(self.frames.values()),
                      cumulativeRawBytes=sum(self.raw_bytes.values()), bundleSha256=self.sha, timestampViolations=0,
                      sources=[dict(sourceId=source, configuration=self.current[source]["configuration"], clockId=clock,
                                    session=self.current[source]["session"], currentSessionFrames=self.current[source]["frames"],
                                    cumulativeFrames=self.frames[source], cumulativeRawBytes=self.raw_bytes[source]) for source in (7, 8)])
        if self.local_synthetic:
            report["credentialKind"] = "local-dummy"
            manifest = dict(provenance=audit.LOCAL_PROVENANCE,
                            runs=[dict(clockId=number, sources=[sources[source] for source in (7, 8)],
                                       expectedMatchedTrades=len(set(sources[7]["tradeIds"])),
                                       jsonEndpoint=report["jsonEndpoint"], sbeEndpoint=report["sbeEndpoint"])
                                  for number, sources in sorted(self.expected.items())])
            (self.path / "local-expected.json").write_text(json.dumps(manifest), encoding="utf-8")
        (self.path / ("run-" + str(clock) + ".json")).write_text(json.dumps(report) + "\n", encoding="utf-8")
        return report

    def write_rows(self, name, rows):
        (self.path / name).write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")

    def paired(self, clock=1, configuration=1, trade_id=100):
        self.definition(7, configuration, clock)
        self.definition(8, configuration, clock)
        self.observation(7, '{"result":null,"id":1}', 20)
        self.observation(8, b'\0{"result":null,"id":1}', 25)
        self.observation(7, trade(trade_id), 200, [trade_id])
        self.observation(8, b"\1" + binary([(trade_id, 10001, 25, 1)]), 200, [trade_id])


class DecoderTest(unittest.TestCase):
    def test_exact_retained_schema_and_independent_multitrade_golden(self):
        self.assertEqual(audit.validate_schema(), audit.SCHEMA_SHA256)
        fixture = json.loads((Path(__file__).parent / "fixtures/trades_golden.json").read_text())
        with localcontext() as context:
            context.prec = 3  # Neither SBE nor JSON interpretation may round to the process Decimal context.
            decoded = audit.sbe_trades(bytes.fromhex(fixture["sbeHex"]), NOW + 100)
        actual = [dict(id=item["id"], price=str(item["price"]), quantity=str(item["quantity"]), maker=item["maker"],
                       tradeTimeMicros=item["tradeTimeMicros"], eventTimeMicros=item["eventTimeMicros"], childIndex=item["childIndex"])
                  for item in decoded]
        self.assertEqual(actual, fixture["expected"])
        self.assertGreater(actual[0]["id"], 2 ** 53)

    def test_schema_download_is_verified_before_it_replaces_a_file_and_reused_offline(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stream_1_0.xml"
            path.write_bytes(b"old bytes")
            with patch.object(audit, "urlopen", return_value=io.BytesIO(b"wrong release")):
                with self.assertRaisesRegex(ValueError, "schema digest differs"):
                    audit.fetch_schema(path)
            self.assertEqual(path.read_bytes(), b"old bytes")
            with patch.object(audit, "urlopen", return_value=io.BytesIO(audit.SCHEMA_FILE.read_bytes())) as fetch:
                self.assertEqual(audit.fetch_schema(path), audit.SCHEMA_SHA256)
                self.assertEqual(audit.fetch_schema(path), audit.SCHEMA_SHA256)
                fetch.assert_called_once_with(audit.SCHEMA_URL, timeout=30)

    def test_header_lengths_unknown_version_template_schema_and_trailing_bytes_fail(self):
        valid = binary([(1, 10001, 25, 1)])
        for offset, value in ((0, 19), (2, 10001), (4, 2), (6, 1), (26, 24)):
            broken = bytearray(valid)
            struct.pack_into("<H", broken, offset, value)
            with self.subTest(offset=offset), self.assertRaises(ValueError):
                audit.sbe_trades(broken, NOW + 100)
        for broken in (valid[:7], valid[:-1], valid + b"\0", valid[:32], binary([])):
            with self.subTest(size=len(broken)), self.assertRaises(ValueError):
                audit.sbe_trades(broken, NOW + 100)
        broken = bytearray(valid)
        struct.pack_into("<I", broken, 28, 0xFFFF_FFFF)
        with self.assertRaisesRegex(ValueError, "group dimensions"):
            audit.sbe_trades(broken, NOW + 100)

    def test_negative_zero_invalid_flags_symbol_utf8_and_future_timestamp_fail(self):
        for payload in (binary([(0, 10001, 25, 1)]), binary([(1, 0, 25, 1)]), binary([(1, 10001, -1, 1)]),
                        binary([(1, 10001, 25, 2)]), binary([(1, 10001, 25, 1)], symbol=b"ETHUSDT"),
                        binary([(1, 10001, 25, 1)], symbol=b"\xff"), binary([(1, 10001, 25, 1)], event=NOW + 101)):
            with self.subTest(payload=payload.hex()), self.assertRaises((ValueError, UnicodeError)):
                audit.sbe_trades(payload, NOW + 100)

    def test_required_exponents_reject_reserved_int8_null(self):
        for price, quantity in ((-128, -4), (-2, -128)):
            with self.subTest(price=price, quantity=quantity), self.assertRaisesRegex(ValueError, "reserved null"):
                audit.sbe_trades(binary([(1, 10001, 25, 1)], price, quantity), NOW + 100)

    def test_json_controls_unknown_fields_duplicates_and_exact_decimals(self):
        self.assertEqual(audit.json_frame('{"result":null,"id":1}', NOW)[1], "acknowledgement")
        self.assertEqual(audit.json_frame('{"e":"serverShutdown","E":' + str(NOW) + '}', NOW)[1], "shutdown")
        self.assertEqual(audit.json_frame('{"e":"serverShutdown","E":' + str(NOW // 1000) + '}', NOW)[1], "shutdown")
        for timestamp in (NOW * 1000, -1, True):
            with self.subTest(shutdown=timestamp), self.assertRaises(ValueError):
                audit.json_frame(json.dumps(dict(e="serverShutdown", E=timestamp)), NOW)
        for payload in ('{"result":"error","id":1}', '{"code":1,"msg":"bad subscription"}',
                        '{"result":null,"id":1,"id":1}', trade(1).replace('"M": true', '"futureField": true'),
                        trade(1, price="NaN"), trade(1, quantity="0"), trade(True), trade(1, event=NOW + 1)):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                audit.json_frame(payload, NOW)
        item = audit.json_frame(trade(2 ** 53 + 1, price="92233720368.54775807", quantity="0.000000000001"), NOW)[0][0]
        self.assertEqual(item["price"], Decimal("92233720368.54775807"))
        self.assertEqual(item["quantity"], Decimal("0.000000000001"))

    def test_nearest_rank_signed_quantiles(self):
        self.assertEqual(audit.quantiles([-100, -10, 0, 20]), dict(count=4, min=-100, max=20, p50=-10, p99=20, p999=20))
        self.assertEqual(audit.quantiles([])["p99"], None)


class PairedAuditTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.capture = Captures(self.path)

    def test_complete_export_controls_matching_exact_payload_and_report(self):
        self.capture.paired()
        self.capture.run(1)
        result = audit.audit(self.path)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["canonicalHead"], 6)
        self.assertEqual(result["payloadMismatchCount"], 0)
        self.assertEqual(result["receiveDeltaSbeMinusJsonNanos"]["p50"], 0)
        self.assertEqual([session["acknowledgements"] for session in result["sessions"]], [1, 1])
        self.assertFalse(result["upstreamCompletenessEstablished"])

    def test_multitrade_frame_children_inherit_parent_receive_and_event_time_need_not_match(self):
        self.capture.definition(7, 1, 1)
        self.capture.definition(8, 1, 1)
        self.capture.observation(7, trade(100, event=NOW - 5), 200)
        self.capture.observation(7, trade(101), 210)
        self.capture.observation(8, b"\1" + binary([(100, 10001, 25, 1), (101, 10001, 25, 1)]), 215)
        self.capture.observation(7, trade(102), 230)
        self.capture.observation(8, b"\1" + binary([(102, 10001, 25, 1)]), 230)
        self.capture.run(1)
        result = audit.audit(self.path)
        self.assertEqual(result["status"], "passed")
        self.assertEqual([session["trades"] for session in result["sessions"]], [3, 3])
        self.assertEqual(result["clocks"][0]["matchedUniqueTrades"], 3)
        self.assertEqual(result["payloadMismatchCount"], 0)
        self.assertEqual(result["receiveDeltaSbeMinusJsonNanos"]["count"], 1)

    def test_duplicates_unmatched_startup_interior_and_tail_are_diagnostics(self):
        self.capture.definition(7, 1, 1)
        self.capture.definition(8, 1, 1)
        self.capture.observation(7, trade(90), 50)  # Startup before the explicit paired measurement window.
        self.capture.observation(7, trade(100), 200)
        self.capture.observation(8, b"\1" + binary([(100, 10001, 25, 1)]), 200)
        self.capture.observation(7, trade(100), 210)
        self.capture.observation(7, trade(101), 220)  # Interior unmatched is not inferred to be a transport loss.
        self.capture.observation(7, trade(102), 250)
        self.capture.observation(8, b"\1" + binary([(102, 10001, 25, 1)]), 250)
        self.capture.observation(8, b"\1" + binary([(103, 10001, 25, 1)]), 1100)  # Tail outside the declared interval.
        self.capture.run(1)
        result = audit.audit(self.path)
        self.assertEqual(result["status"], "passed")
        clock = result["clocks"][0]
        self.assertEqual(clock["sources"][0]["unmatchedByObservedWindow"], dict(startup=1, interior=1, tail=0))
        self.assertEqual(clock["sources"][1]["unmatchedByObservedWindow"], dict(startup=0, interior=0, tail=1))
        self.assertEqual(clock["duplicateTradeIds"][0]["occurrences"], 2)
        self.assertEqual(result["receiveDeltaSbeMinusJsonNanos"]["count"], 2)

    def test_crossfeed_and_conflicting_duplicate_payload_mismatches_fail_acceptance(self):
        self.capture.paired()
        self.capture.observation(7, trade(100, price="100.02"), 220)
        self.capture.observation(7, trade(101, maker=False), 250)
        self.capture.observation(8, b"\1" + binary([(101, 10001, 25, 1)]), 250)
        self.capture.run(1)
        result = audit.audit(self.path)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["payloadMismatchCount"], 2)
        self.assertEqual({item["kind"] for item in result["clocks"][0]["payloadMismatches"]},
                         {"crossFeedPayload", "conflictingDuplicate"})

    def test_zero_actual_trades_or_no_matched_overlap_cannot_pass(self):
        self.capture.definition(7, 1, 1)
        self.capture.definition(8, 1, 1)
        self.capture.observation(7, '{"result":null,"id":1}', 200)
        self.capture.observation(8, b'\0{"result":null,"id":1}', 200)
        self.capture.run(1)
        with self.assertRaisesRegex(ValueError, "actual trades"):
            audit.audit(self.path)
        self.capture.observation(7, trade(100), 220)
        self.capture.observation(8, b"\1" + binary([(999, 10001, 25, 1)]), 220)
        self.capture.run(1)
        self.assertEqual(audit.audit(self.path)["status"], "failed")

    def test_bad_tag_binary_as_text_noncanonical_base64_and_wrong_bound_fail(self):
        self.capture.paired()
        self.capture.run(1)
        original = copy.deepcopy(self.capture.rows)
        for field, value in (("rawBase64", "Ag=="), ("rawBase64", "!not-base64!"), ("rawBase64", "AB=="),
                             ("rawBase64", base64.b64encode(b"\0" + trade(100).encode()).decode()),
                             ("rawByteLength", 65501)):
            self.capture.rows = copy.deepcopy(original)
            self.capture.rows[-1][field] = value
            self.capture.write_rows("canonical-1.jsonl", self.capture.rows)
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                audit.audit(self.path)

    def test_profile_unknown_fields_schema_digest_shared_clock_and_artifact_tampering_fail(self):
        self.capture.paired()
        self.capture.run(1)
        original = copy.deepcopy(self.capture.rows)
        for section, field, value in (("parser", "future", 1), ("clock", "origin", "different"),
                                      ("parameters", "sbeSchemaVersion", 1), ("parameters", "sbeSchemaSha256", "0" * 64)):
            self.capture.rows = copy.deepcopy(original)
            profile = json.loads(self.capture.rows[1]["profile"])
            target = profile["parser"]["parameters"] if section == "parameters" else profile[section]
            target[field] = value
            self.capture.rows[1]["profile"] = json.dumps(profile)
            self.capture.write_rows("canonical-1.jsonl", self.capture.rows)
            with self.subTest(section=section, field=field), self.assertRaises(ValueError):
                audit.audit(self.path)
        self.capture.rows = original
        self.capture.run(1)
        self.capture.artifact_path.write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "executable hash"):
            audit.audit(self.path)

    def test_canonical_gap_raw_gap_binding_timestamp_and_definition_sidecar_fail(self):
        self.capture.paired()
        self.capture.run(1)
        original = copy.deepcopy(self.capture.rows)
        for field, value in (("sequence", 99), ("rawFrameSequence", 99), ("clockId", 99), ("subeventIndex", 1),
                             ("receiveTimeNanos", -1), ("completeTimeNanos", 199), ("rawByteLength", 1)):
            self.capture.rows = copy.deepcopy(original)
            self.capture.rows[-1][field] = value
            self.capture.write_rows("canonical-1.jsonl", self.capture.rows)
            with self.subTest(field=field), self.assertRaises(ValueError):
                audit.audit(self.path)
        self.capture.rows = original
        self.capture.run(1)
        self.capture.write_rows("definitions-1.jsonl", [])
        with self.assertRaisesRegex(ValueError, "sidecar"):
            audit.audit(self.path)

    def test_restart_exact_prefix_source_configs_independent_and_clock_scoped_ids(self):
        self.capture.paired()
        self.capture.run(1)
        previous = len(self.capture.rows)
        self.capture.definition(7, 3, 2)
        self.capture.definition(8, 5, 2)
        self.capture.observation(7, trade(100), 200)
        self.capture.observation(8, b"\1" + binary([(100, 10001, 25, 1)]), 200)
        self.capture.run(2, previous)
        result = audit.audit(self.path)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["runs"], 2)
        self.assertEqual(result["matchedWithinObservedOverlap"], 2)
        self.assertEqual(len(result["restartPrefixChecks"]), 1)
        self.assertEqual([clock["duplicateTradeIds"] for clock in result["clocks"]], [[], []])
        old = self.path / "canonical-1.jsonl"
        old.write_bytes(old.read_bytes().replace(b'"receiveTimeNanos":200', b'"receiveTimeNanos":201', 1))
        with self.assertRaises(ValueError):
            audit.audit(self.path)

    def test_report_source_totals_and_lineage_are_verified(self):
        self.capture.paired()
        report = self.capture.run(1)
        report["sources"][0]["cumulativeFrames"] += 1
        (self.path / "run-1.json").write_text(json.dumps(report))
        with self.assertRaisesRegex(ValueError, "summary"):
            audit.audit(self.path)

    def test_prior_matched_clock_cannot_mask_an_unmatched_later_clock(self):
        self.capture.paired()
        self.capture.run(1)
        previous = len(self.capture.rows)
        self.capture.definition(7, 3, 2)
        self.capture.definition(8, 5, 2)
        self.capture.observation(7, trade(200), 200)
        self.capture.observation(8, b"\1" + binary([(999, 10001, 25, 1)]), 200)
        self.capture.run(2, previous)
        result = audit.audit(self.path)
        self.assertEqual(result["matchedWithinObservedOverlap"], 1)
        self.assertEqual([clock["matchedWithinObservedOverlap"] for clock in result["clocks"]], [1, 0])
        self.assertEqual(result["status"], "failed")
        self.assertEqual([run["clusterSessionTimeoutNs"] for run in result["runTimes"]], [120_000_000_000] * 2)

    def test_invalid_retained_session_timeout_is_rejected(self):
        self.capture.paired()
        report = self.capture.run(1)
        for value in (True, 10_000_000_000, "120000000000"):
            report["clusterSessionTimeoutNs"] = value
            (self.path / "run-1.json").write_text(json.dumps(report))
            with self.subTest(timeout=value), self.assertRaisesRegex(ValueError, "session timeout"):
                audit.audit(self.path)

    def test_retained_logged_time_makes_audit_independent_of_current_wall_clock(self):
        self.capture.paired()
        self.capture.run(1)
        with patch("time.time_ns", return_value=0):
            before = audit.audit(self.path)
        with patch("time.time_ns", return_value=NOW * 10_000):
            after = audit.audit(self.path)
        self.assertEqual(before, after)
        self.assertEqual(before["status"], "passed")
        self.capture.rows[-1]["sequencedTimeNanos"] = (NOW - 86_400_000_001) * 1000
        self.capture.write_rows("canonical-1.jsonl", self.capture.rows)
        with self.assertRaisesRegex(ValueError, "epoch microseconds"):
            audit.audit(self.path)


class LocalSyntheticAuditTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.capture = Captures(self.path, local_synthetic=True)
        self.capture.paired()
        self.report = self.capture.run(1)

    def write_report(self, value):
        (self.path / "run-1.json").write_text(json.dumps(value), encoding="utf-8")

    def test_explicit_opt_in_is_deterministic_and_cannot_be_reported_as_live(self):
        with self.assertRaises(ValueError):
            audit.audit(self.path)
        with patch("time.time_ns", return_value=0):
            first = audit.audit(self.path, local_synthetic=True)
        with patch("time.time_ns", return_value=NOW * 10_000):
            second = audit.audit(self.path, local_synthetic=True)
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "passed-local-synthetic")
        self.assertEqual(first["mode"], audit.LOCAL_MODE)
        self.assertTrue(first["synthetic"])
        self.assertFalse(first["venueData"])
        self.assertTrue(first["expectedRawVerified"])
        self.assertTrue(first["expectedTradeOrderVerified"])
        self.assertTrue(first["expectedPayloadsEquivalent"])
        self.assertEqual(first["matchedWithinObservedOverlap"], 1)

    def test_relabeled_mode_and_endpoints_still_cannot_hide_source_provenance(self):
        relabeled = dict(self.report, mode=audit.LIVE_MODE, jsonEndpoint=audit.JSON_ENDPOINT, sbeEndpoint=audit.SBE_ENDPOINT)
        del relabeled["credentialKind"]
        self.write_report(relabeled)
        with self.assertRaisesRegex(ValueError, "parser parameters"):
            audit.audit(self.path)

    def test_only_distinct_exact_ipv4_loopback_peers_are_accepted(self):
        for endpoint in ("ws://localhost:19231/ws", "wss://127.0.0.1:19231/ws", "ws://127.0.0.2:19231/ws",
                         "ws://127.0.0.1:19231/ws?timeUnit=MICROSECOND", "ws://127.0.0.1:19231/ws#fragment",
                         "ws://dummy@127.0.0.1:19231/ws", "ws://127.0.0.1:019231/ws", "ws://127.0.0.1:0/ws",
                         "ws://127.0.0.1:65536/ws", "ws://127.0.0.1/ws", self.report["sbeEndpoint"]):
            self.write_report(dict(self.report, jsonEndpoint=endpoint))
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                audit.audit(self.path, local_synthetic=True)

    def test_local_dummy_credential_attestation_is_required_without_retaining_key(self):
        for value in (None, True, "venue-key"):
            self.write_report(dict(self.report, credentialKind=value))
            with self.subTest(kind=value), self.assertRaisesRegex(ValueError, "dummy credential"):
                audit.audit(self.path, local_synthetic=True)
        missing = dict(self.report)
        del missing["credentialKind"]
        self.write_report(missing)
        with self.assertRaisesRegex(ValueError, "run report"):
            audit.audit(self.path, local_synthetic=True)

    def test_each_source_requires_the_exact_local_profile_attestation(self):
        original = copy.deepcopy(self.capture.rows)
        for value in (None, True, "live Binance venue data"):
            self.capture.rows = copy.deepcopy(original)
            profile = json.loads(self.capture.rows[1]["profile"])
            if value is None:
                del profile["parser"]["parameters"]["provenance"]
            else:
                profile["parser"]["parameters"]["provenance"] = value
            self.capture.rows[1]["profile"] = json.dumps(profile)
            self.capture.run(1)
            with self.subTest(provenance=value), self.assertRaises(ValueError):
                audit.audit(self.path, local_synthetic=True)

    def test_local_mode_keeps_exact_payload_matching_gate(self):
        self.capture.observation(7, trade(101, price="100.02"), 250, [101])
        self.capture.observation(8, b"\1" + binary([(101, 10001, 25, 1)]), 250, [101])
        self.capture.run(1)
        result = audit.audit(self.path, local_synthetic=True)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["payloadMismatchCount"], 1)
        self.assertTrue(result["synthetic"])
        self.assertFalse(result["expectedPayloadsEquivalent"])

    def test_manifest_binds_each_raw_control_frame_and_native_trade_order(self):
        path = self.path / "local-expected.json"
        original = json.loads(path.read_text())
        for change in ("removeAck", "swapFrames", "extraFrame", "wrongTradeOrder", "wrongTradeId", "wrongBase64"):
            manifest = copy.deepcopy(original)
            source = manifest["runs"][0]["sources"][0]
            if change == "removeAck":
                source["rawBase64"].pop(0)
            elif change == "swapFrames":
                source["rawBase64"].reverse()
            elif change == "extraFrame":
                source["rawBase64"].append(source["rawBase64"][-1])
            elif change == "wrongTradeOrder":
                source["tradeIds"] = [100, 100]
            elif change == "wrongTradeId":
                source["tradeIds"] = [999]
            else:
                source["rawBase64"][0] = "AB=="
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.subTest(change=change), self.assertRaises(ValueError):
                audit.audit(self.path, local_synthetic=True)

    def test_manifest_requires_retained_endpoints_clock_count_and_unique_match_count(self):
        path = self.path / "local-expected.json"
        original = json.loads(path.read_text())
        for field, value in (("clockId", 2), ("expectedMatchedTrades", 2), ("expectedMatchedTrades", True),
                             ("jsonEndpoint", "ws://127.0.0.1:29231/ws"), ("sbeEndpoint", "ws://127.0.0.1:29232/ws")):
            manifest = copy.deepcopy(original)
            manifest["runs"][0][field] = value
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                audit.audit(self.path, local_synthetic=True)
        for manifest in (dict(original, provenance="venue data"), dict(original, futureField=1), dict(original, runs=[])):
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                audit.audit(self.path, local_synthetic=True)
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            audit.audit(self.path, local_synthetic=True)

    def test_local_restart_manifest_verifies_both_clocks_with_exact_prefix(self):
        previous = len(self.capture.rows)
        self.capture.paired(clock=2, configuration=3, trade_id=101)
        self.capture.run(2, previous)
        first = audit.audit(self.path, local_synthetic=True)
        second = audit.audit(self.path, local_synthetic=True)
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "passed-local-synthetic")
        self.assertEqual(first["runs"], 2)
        self.assertEqual(len(first["restartPrefixChecks"]), 1)
        self.assertEqual([clock["expectedMatchedTrades"] for clock in first["clocks"]], [1, 1])
        self.assertEqual([clock["payloadEquivalentUniqueTrades"] for clock in first["clocks"]], [1, 1])

    def test_local_restart_requires_a_distinct_retained_origin(self):
        previous = len(self.capture.rows)
        self.capture.paired(clock=2, configuration=3, trade_id=101)
        for row in self.capture.rows:
            if row["kind"] == "definition" and row["clockId"] == 2:
                profile = json.loads(row["profile"])
                profile["clock"]["origin"] = "101"
                row["profile"] = json.dumps(profile)
        self.capture.run(2, previous)
        with self.assertRaisesRegex(ValueError, "origin was reused"):
            audit.audit(self.path, local_synthetic=True)


if __name__ == "__main__":
    unittest.main()
