#!/usr/bin/env python3
"""Cold, deterministic interpretation of retained JSON/SBE trade capture; no upstream completeness claim."""

import base64
import binascii
import json
import re
import struct
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote, urlsplit
from urllib.request import urlopen

import binance_json_audit as common

require, decode, integer, keys = common.require, common.decode, common.integer, common.keys
SCHEMA_SHA256 = "6ea328467e144311b1f1efff38e9fe613829997f041dd02a3b7077885d10a1f7"
SCHEMA_URL = ("https://raw.githubusercontent.com/binance/binance-spot-api-docs/"
              "bed5d358dbe98bfa4722b6c00c27be1e8f0ba40b/sbe/schemas/stream_1_0.xml")
SCHEMA_FILE = Path(__file__).parent / "fixtures" / "upstream" / "stream_1_0.xml"
HEADER, ROOT, GROUP, TRADE = (struct.Struct(pattern) for pattern in ("<HHHH", "<qqbb", "<HI", "<qqqB"))
NULL_NATIVE_ID = "18446744073709551615"
MAX_RAW_BYTES = 65_500
JSON_ENDPOINT = "wss://data-stream.binance.vision/ws?timeUnit=MICROSECOND"
SBE_ENDPOINT = "wss://stream-sbe.binance.com/ws"
LIVE_MODE = "binance-spot-pair"
LOCAL_MODE = "binance-spot-pair-local-synthetic"
LOCAL_PROVENANCE = "local scripted Binance-shape fixture; not venue data"
RUN_FIELDS = ("mode jsonEndpoint sbeEndpoint seconds clockId canonicalHead previousCanonicalHead "
              "measurementStartTimeNanos measurementEndTimeNanos clusterSessionTimeoutNs definitions "
              "cumulativeFrames cumulativeRawBytes bundleSha256 timestampViolations sources")
BASE_FIELDS = "kind sequence logId sourceId configuration clockId sequencedTimeNanos receiveTimeNanos completeTimeNanos readyTimeNanos"
DEFINITION_FIELDS = BASE_FIELDS + " provider product session collector profile clockMeaning syncUncertaintyNanos"
OBSERVATION_FIELDS = BASE_FIELDS + " session rawFrameSequence subeventIndex rawByteLength nativeNumericId"


def validate_schema(path=SCHEMA_FILE):
    """The four fixed Python layouts are checked against the exact retained official XML, not a moving URL."""
    require(path.is_file(), f"missing {path}; run binance_pair_audit.py --fetch-schema")
    require(common.digest(path) == SCHEMA_SHA256, "pinned SBE schema digest differs")
    schema = ET.parse(path).getroot()
    require((schema.get("id"), schema.get("version"), schema.get("byteOrder")) == ("1", "0", "littleEndian"),
            "SBE schema identity/order differs")
    types = schema.find("types")
    header = types.find("composite[@name='messageHeader']")
    group = types.find("composite[@name='groupSizeEncoding']")
    message = schema.find("{http://fixprotocol.io/2016/sbe}message[@name='TradesStreamEvent']")
    require(message is not None and message.get("id") == "10000", "SBE trade template differs")
    require([(field.get("name"), field.get("primitiveType")) for field in header] ==
            [(name, "uint16") for name in ("blockLength", "templateId", "schemaId", "version")], "SBE header differs")
    require([(field.get("name"), field.get("primitiveType")) for field in group] ==
            [("blockLength", "uint16"), ("numInGroup", "uint32")], "SBE group dimensions differ")
    require([(field.get("name"), field.get("type")) for field in message.findall("field")] ==
            [("eventTime", "utcTimestampUs"), ("transactTime", "utcTimestampUs"),
             ("priceExponent", "exponent8"), ("qtyExponent", "exponent8")], "SBE root fields differ")
    trades = message.find("group")
    require(trades.get("name") == "trades" and [(field.get("name"), field.get("type")) for field in trades] ==
            [("id", "tradeId"), ("price", "mantissa64"), ("qty", "mantissa64"),
             ("isBuyerMaker", "boolEnum"), ("isBestMatch", "boolEnum")], "SBE trade fields differ")
    require(trades[-1].get("presence") == "constant" and trades[-1].get("valueRef") == "boolEnum.True",
            "SBE best-match constant differs")
    require([(field.get("name"), field.get("type")) for field in message.findall("data")] ==
            [("symbol", "varString8")], "SBE symbol field differs")
    return SCHEMA_SHA256


def fetch_schema(path=SCHEMA_FILE):
    """Keep a verified copy for offline audits. Install it only after validation succeeds."""
    if path.is_file() and common.digest(path) == SCHEMA_SHA256:
        return validate_schema(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=path.parent) as staging:
        candidate = Path(staging) / path.name
        with urlopen(SCHEMA_URL, timeout=30) as response:
            candidate.write_bytes(response.read())
        digest = validate_schema(candidate)
        candidate.replace(path)
        return digest


def epoch(value, upper):
    require(integer(value, 1) and 1_577_836_800_000_000 <= value <= upper,
            "exchange timestamp is not plausible epoch microseconds")


def decimal_text(value):
    require(type(value) is str and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value) is not None,
            "price/quantity must be exact decimal text")
    number = Decimal(value)
    require(number.is_finite() and number > 0, "nonpositive/nonfinite price or quantity")
    return number


def scaled(mantissa, exponent):
    require(mantissa > 0, "nonpositive SBE price or quantity")
    digits = Decimal(mantissa).as_tuple()
    return Decimal((digits.sign, digits.digits, exponent))


def json_frame(payload, upper):
    raw = decode(payload)
    require(type(raw) is dict, "application JSON is not an object")
    if set(raw) == {"result", "id"}:
        require(raw["result"] is None and type(raw["id"]) is int and raw["id"] == 1,
                "subscription acknowledgement differs")
        return [], "acknowledgement"
    if raw.get("e") == "serverShutdown":
        keys(raw, "e E", "shutdown")
        require(integer(raw["E"], 1), "invalid shutdown timestamp")
        epoch(raw["E"] * 1000 if raw["E"] < 1_577_836_800_000_000 else raw["E"], upper)
        return [], "shutdown"
    require(set(raw) in ({"e", "E", "s", "t", "p", "q", "T", "m"},
                         {"e", "E", "s", "t", "p", "q", "T", "m", "M"}), "unknown JSON trade fields")
    require(raw["e"] == "trade" and raw["s"] == "BTCUSDT" and integer(raw["t"], 1)
            and raw["t"] <= 0x7FFF_FFFF_FFFF_FFFF and type(raw["m"]) is bool
            and ("M" not in raw or type(raw["M"]) is bool), "invalid JSON trade identity/flags")
    epoch(raw["E"], upper)
    epoch(raw["T"], upper)
    return [dict(id=raw["t"], price=decimal_text(raw["p"]), quantity=decimal_text(raw["q"]),
                 maker=raw["m"], tradeTimeMicros=raw["T"], eventTimeMicros=raw["E"], childIndex=0)], "trade"


def sbe_trades(payload, upper):
    require(len(payload) >= HEADER.size + ROOT.size + GROUP.size + 1, "truncated SBE trade message")
    require(HEADER.unpack_from(payload) == (ROOT.size, 10000, 1, 0), "unsupported SBE header/template/schema/version")
    event, transaction, price_exp, quantity_exp = ROOT.unpack_from(payload, HEADER.size)
    require(price_exp != -128 and quantity_exp != -128, "SBE exponent is the reserved null value")
    epoch(event, upper)
    epoch(transaction, upper)
    row_length, count = GROUP.unpack_from(payload, HEADER.size + ROOT.size)
    start = HEADER.size + ROOT.size + GROUP.size
    require(row_length == TRADE.size and 0 < count <= 0x7FFF_FFFF, "invalid SBE trade group dimensions")
    symbol_offset = start + count * row_length
    require(symbol_offset < len(payload), "truncated SBE trade group")
    symbol_length = payload[symbol_offset]
    require(symbol_offset + 1 + symbol_length == len(payload), "SBE symbol length/trailing bytes differ")
    require(payload[symbol_offset + 1:].decode("utf-8") == "BTCUSDT", "SBE trade symbol differs")
    result = []
    for index in range(count):
        trade_id, price, quantity, maker = TRADE.unpack_from(payload, start + index * row_length)
        require(trade_id > 0 and maker in (0, 1), "invalid SBE trade identity/maker flag")
        result.append(dict(id=trade_id, price=scaled(price, price_exp), quantity=scaled(quantity, quantity_exp),
                           maker=bool(maker), tradeTimeMicros=transaction, eventTimeMicros=event, childIndex=index))
    return result


def profile(row, data, verified, local_synthetic=False):
    value = decode(row["profile"])
    keys(value, "profile venue channel parser raw native clock", "profile")
    require(value["profile"] == "spine.capture.v1" and value["venue"] == "binance-spot"
            and value["channel"] == "trade", "unsupported interpretation profile")
    for name, expected in (("parser", "artifact sha256 entrypoint parameters"), ("raw", "boundary representation transformation"),
                           ("native", "namespace representation"), ("clock", "domain origin resolutionNanos receive complete ready")):
        keys(value[name], expected, name)
        for field, text in value[name].items():
            if field not in ("parameters", "resolutionNanos"):
                require(type(text) is str and text.strip(), name + "." + field + " must be a nonempty string")
    require(integer(value["clock"]["resolutionNanos"], 1), "invalid nominal clock resolution")
    require(re.fullmatch(r"-?[0-9]+", value["clock"]["origin"]) is not None, "invalid retained clock origin")
    binary = row["sourceId"] == 8
    parameters = value["parser"]["parameters"]
    expected = "format timeUnit subscription applicationHeartbeat rfcPingPong java harness exchangeTimestamps taggedFraming maxApplicationPayloadBytes"
    if binary:
        expected += " sbeSchemaId sbeSchemaVersion sbeSchemaSha256 sbeSchemaUrl"
    if local_synthetic:
        expected += " provenance"
    keys(parameters, expected, "parser parameters")
    if local_synthetic:
        require(parameters["provenance"] == LOCAL_PROVENANCE, "local source provenance differs")
    require(parameters["format"] == ("SBE" if binary else "JSON") and parameters["timeUnit"] == "MICROSECOND"
            and parameters["applicationHeartbeat"] is None and parameters["taggedFraming"] is binary
            and parameters["maxApplicationPayloadBytes"] == MAX_RAW_BYTES - binary, "capture parameters differ")
    for name in ("rfcPingPong", "java", "harness", "exchangeTimestamps"):
        require(type(parameters[name]) is str and parameters[name].strip(), "invalid parser parameter " + name)
    subscription = parameters["subscription"]
    keys(subscription, "method params id", "subscription")
    require(subscription["method"] == "SUBSCRIBE" and subscription["params"] == ["btcusdt@trade"]
            and type(subscription["id"]) is int and subscription["id"] == 1, "subscription differs")
    require(value["raw"]["representation"] == ("tagged bytes: 0=UTF-8 text, 1=binary" if binary else "UTF-8"),
            "unsupported raw encoding")
    require(value["raw"]["boundary"] == "one complete WebSocket application message"
            and value["native"]["namespace"] == "binance Spot BTCUSDT trade id t"
            and value["native"]["representation"] == "absent; identity retained in raw message"
            and value["clock"]["domain"] == "shared process monotonic elapsed nanoseconds",
            "raw/native/clock interpretation differs")
    if binary:
        require(type(parameters["sbeSchemaId"]) is int and parameters["sbeSchemaId"] == 1
                and type(parameters["sbeSchemaVersion"]) is int and parameters["sbeSchemaVersion"] == 0
                and parameters["sbeSchemaSha256"] == SCHEMA_SHA256 and parameters["sbeSchemaUrl"] == SCHEMA_URL,
                "SBE schema provenance differs")
    sha = value["parser"]["sha256"]
    require(re.fullmatch(r"[0-9a-f]{64}", sha) is not None, "invalid retained SHA-256")
    artifact = urlsplit(value["parser"]["artifact"])
    require(artifact.scheme == "file" and not artifact.netloc and not artifact.query and not artifact.fragment
            and Path(unquote(artifact.path)).name == sha + ".jar", "artifact reference differs from retained digest")
    if sha not in verified:
        require(common.digest(data / "artifacts" / (sha + ".jar")) == sha, "retained executable hash differs")
        verified.add(sha)
    return value, sha


def signature(trade):
    return trade["price"], trade["quantity"], trade["maker"], trade["tradeTimeMicros"]


def quantiles(values):
    values = sorted(values)
    if not values:
        return dict(count=0, min=None, max=None, p50=None, p99=None, p999=None)
    result = dict(count=len(values), min=values[0], max=values[-1])
    for name, numerator, denominator in (("p50", 50, 100), ("p99", 99, 100), ("p999", 999, 1000)):
        result[name] = values[(len(values) * numerator + denominator - 1) // denominator - 1]
    return result


def compare(sessions, windows=None):
    """Match identities only within the same shared process clock; latency uses common observed trade-time windows."""
    clocks, reports, deltas = defaultdict(dict), [], []
    mismatch_count = matched_count = 0
    for session in sessions:
        require(session["sourceId"] not in clocks[session["clockId"]], "multiple sessions share one source clock")
        clocks[session["clockId"]][session["sourceId"]] = session
    for clock, sources in sorted(clocks.items()):
        require(set(sources) == {7, 8} and all(source["trades"] for source in sources.values()), "both feeds must contain actual trades")
        indexes = {source: defaultdict(list) for source in (7, 8)}
        for source in (7, 8):
            for trade in sources[source]["trades"]:
                indexes[source][trade["id"]].append(trade)
        start = max(sources[source]["trades"][0]["receiveTimeNanos"] for source in (7, 8))
        end = min(sources[source]["trades"][-1]["receiveTimeNanos"] for source in (7, 8))
        measured_start, measured_end = (windows or {}).get(clock, (start, end))
        start, end = max(start, measured_start), min(end, measured_end)
        common_ids = set(indexes[7]) & set(indexes[8])
        mismatches, duplicates, local_deltas, conflicting_ids = [], [], [], set()
        for source in (7, 8):
            for trade_id, instances in sorted(indexes[source].items()):
                if len(instances) > 1:
                    duplicates.append(dict(sourceId=source, tradeId=trade_id, occurrences=len(instances),
                                           frames=[dict(sequence=t["sequence"], rawFrameSequence=t["rawFrameSequence"],
                                                        childIndex=t["childIndex"]) for t in instances]))
                if len({signature(trade) for trade in instances}) > 1:
                    conflicting_ids.add(trade_id)
                    mismatches.append(dict(kind="conflictingDuplicate", sourceId=source, tradeId=trade_id))
        equivalent = 0
        for trade_id in sorted(common_ids):
            left, right = indexes[7][trade_id][0], indexes[8][trade_id][0]
            if signature(left) != signature(right):
                mismatches.append(dict(kind="crossFeedPayload", tradeId=trade_id,
                                       fields=[field for field in ("price", "quantity", "maker", "tradeTimeMicros")
                                               if left[field] != right[field]]))
                continue
            if trade_id in conflicting_ids:
                continue
            equivalent += 1
            if start <= left["receiveTimeNanos"] <= end and start <= right["receiveTimeNanos"] <= end:
                local_deltas.append(right["receiveTimeNanos"] - left["receiveTimeNanos"])
        unmatched = []
        for source, other in ((7, 8), (8, 7)):
            missing = sorted(set(indexes[source]) - set(indexes[other]))
            counts = dict(startup=0, interior=0, tail=0)
            for trade_id in missing:
                receive = indexes[source][trade_id][0]["receiveTimeNanos"]
                counts["startup" if receive < start else "tail" if receive > end else "interior"] += 1
            interior_ids = sum(start <= trades[0]["receiveTimeNanos"] <= end for trades in indexes[source].values())
            unmatched.append(dict(sourceId=source, uniqueTradeIds=len(indexes[source]), unmatchedUniqueTrades=len(missing),
                                  uniqueTradeIdsWithinObservedOverlap=interior_ids,
                                  observedOverlapMatchCoverage=len(local_deltas) / interior_ids if interior_ids else None,
                                  unmatchedByObservedWindow=counts, unmatchedTradeIds=missing))
        matched_count += len(local_deltas)
        mismatch_count += len(mismatches)
        deltas.extend(local_deltas)
        reports.append(dict(clockId=clock, observedOverlapStartNanos=start, observedOverlapEndNanos=end,
                            measurementStartTimeNanos=measured_start, measurementEndTimeNanos=measured_end,
                            matchedUniqueTrades=len(common_ids), matchedWithinObservedOverlap=len(local_deltas),
                            payloadEquivalentUniqueTrades=equivalent, matchedOutsideObservedOverlap=equivalent - len(local_deltas),
                            receiveDeltaSbeMinusJsonNanos=quantiles(local_deltas), sources=unmatched,
                            duplicateTradeIds=duplicates, payloadMismatches=mismatches))
    return dict(clocks=reports, matchedWithinObservedOverlap=matched_count, payloadMismatchCount=mismatch_count,
                receiveDeltaSbeMinusJsonNanos=quantiles(deltas), quantileMethod="nearest rank",
                matchKey="binance-spot + BTCUSDT + shared clock/session overlap + native trade id",
                childReceiveTime="first callback timestamp of its complete raw parent frame",
                eventTimeEqualityRequired=False, upstreamCompletenessEstablished=False,
                shutdownTimestampPolicy="plausible epoch milliseconds or microseconds; controls excluded from trade matching")


def audit_export(path, definition_path, report, data, verified, windows=None, local_synthetic=False):
    events, definitions, sessions, current, clock_bindings = common.rows(path), [], [], {}, {}
    source_totals = {source: dict(frames=0, bytes=0) for source in (7, 8)}
    for sequence, row in enumerate(events, 1):
        require(type(row) is dict and type(row.get("sequence")) is int and row["sequence"] == sequence
                and type(row.get("logId")) is int and row["logId"] == 42
                and type(row.get("sourceId")) is int and row["sourceId"] in (7, 8), "canonical sequence/log/source differs")
        source, configuration, clock = (row[name] for name in ("sourceId", "configuration", "clockId"))
        require(integer(configuration, 1) and configuration < 0xFFFF_FFFF and integer(clock, 1) and clock < 0xFFFF_FFFF
                and integer(row["sequencedTimeNanos"]), "invalid numeric binding/sequence time")
        if row["kind"] == "definition":
            keys(row, DEFINITION_FIELDS, "definition row")
            prior = current.get(source)
            require(prior is None or configuration > prior["configuration"] and clock > prior["clockId"], "source binding did not advance")
            session_name = ("ws-json-" if source == 7 else "ws-sbe-") + str(configuration)
            require(row["provider"] == ("binance-public" if source == 7 else "binance-sbe")
                    and row["product"] == "BTCUSDT" and row["session"] == session_name, "source/session differs")
            require(row["receiveTimeNanos"] == -1 and row["completeTimeNanos"] == -1 and integer(row["readyTimeNanos"])
                    and row["syncUncertaintyNanos"] == -1, "definition clock contract differs")
            interpretation, sha = profile(row, data, verified, local_synthetic)
            binding = tuple(interpretation["clock"][name] for name in ("domain", "origin", "resolutionNanos"))
            require(clock not in clock_bindings or clock_bindings[clock] == binding, "shared source clock meanings differ")
            require(not local_synthetic or all(prior_clock == clock or prior[1] != binding[1]
                                               for prior_clock, prior in clock_bindings.items()), "local restart clock origin was reused")
            clock_bindings[clock] = binding
            session = dict(sourceId=source, configuration=configuration, clockId=clock, session=session_name,
                           frames=0, rawBytes=0, acknowledgements=0, shutdowns=0, trades=[], bundleSha256=sha,
                           clockOrigin=interpretation["clock"]["origin"],
                           firstReceiveTimeNanos=None, lastReceiveTimeNanos=None, lastReadyTimeNanos=None)
            if local_synthetic:
                session["rawFrames"] = []
            sessions.append(session)
            current[source] = session
            definitions.append(row)
            continue
        keys(row, OBSERVATION_FIELDS + (" raw" if source == 7 else " rawBase64"), "observation row")
        require(row["kind"] == "observation" and source in current, "unbound or unknown observation")
        session = current[source]
        require(configuration == session["configuration"] and clock == session["clockId"] and row["session"] == session["session"]
                and type(row["rawFrameSequence"]) is int and row["rawFrameSequence"] == session["frames"] + 1
                and type(row["subeventIndex"]) is int and row["subeventIndex"] == 0, "raw frame sequence/binding differs")
        receive, complete, ready = (row[key] for key in ("receiveTimeNanos", "completeTimeNanos", "readyTimeNanos"))
        require(integer(receive) and integer(complete) and integer(ready) and receive <= complete <= ready
                and (session["lastReadyTimeNanos"] is None or session["lastReadyTimeNanos"] <= receive), "capture timestamp ordering differs")
        require(row["nativeNumericId"] == NULL_NATIVE_ID and integer(row["rawByteLength"], 1)
                and row["rawByteLength"] <= MAX_RAW_BYTES, "raw bound/native identity differs")
        upper = row["sequencedTimeNanos"] // 1000 + 86_400_000_000
        if source == 7:
            require(type(row["raw"]) is str, "JSON raw is not text")
            require(len(row["raw"]) <= MAX_RAW_BYTES, "JSON application frame exceeds capture bound")
            raw = row["raw"].encode("utf-8")
            require(len(raw) == row["rawByteLength"], "raw byte length differs")
            children, kind = json_frame(row["raw"], upper)
        else:
            require(type(row["rawBase64"]) is str and len(row["rawBase64"]) <= (MAX_RAW_BYTES + 2) // 3 * 4,
                    "SBE raw is not bounded Base64 text")
            try:
                raw = base64.b64decode(row["rawBase64"], validate=True)
            except (binascii.Error, ValueError) as failure:
                raise ValueError("invalid tagged Base64") from failure
            require(base64.b64encode(raw).decode("ascii") == row["rawBase64"] and raw and raw[0] in (0, 1),
                    "invalid/noncanonical transport tag/Base64")
            require(len(raw) == row["rawByteLength"], "raw byte length differs")
            if raw[0] == 0:
                children, kind = json_frame(raw[1:].decode("utf-8"), upper)
                require(not children, "SBE text frame contains market trade data")
            else:
                children, kind = sbe_trades(raw[1:], upper), "trade"
        require(len(raw) == row["rawByteLength"], "raw byte length differs")
        if local_synthetic:
            session["rawFrames"].append(base64.b64encode(raw).decode("ascii"))
        for child in children:
            child.update(sequence=sequence, rawFrameSequence=row["rawFrameSequence"], receiveTimeNanos=receive,
                         completeTimeNanos=complete, readyTimeNanos=ready)
            session["trades"].append(child)
        session["acknowledgements"] += kind == "acknowledgement"
        session["shutdowns"] += kind == "shutdown"
        session["frames"] += 1
        session["rawBytes"] += len(raw)
        if session["firstReceiveTimeNanos"] is None:
            session["firstReceiveTimeNanos"] = receive
        session["lastReceiveTimeNanos"], session["lastReadyTimeNanos"] = receive, ready
        source_totals[source]["frames"] += 1
        source_totals[source]["bytes"] += len(raw)
    require(sessions and set(current) == {7, 8}, "both source definitions required")
    require(common.rows(definition_path) == definitions, "definition sidecar differs from canonical history")
    require(report["canonicalHead"] == len(events) and report["cumulativeFrames"] == sum(s["frames"] for s in sessions)
            and report["cumulativeRawBytes"] == sum(s["rawBytes"] for s in sessions) and report["definitions"] == len(sessions)
            and report["timestampViolations"] == 0
            and all(s["clockId"] == report["clockId"] and s["bundleSha256"] == report["bundleSha256"] for s in current.values()),
            "run report differs from captured history")
    require(type(report["sources"]) is list and len(report["sources"]) == 2, "run source summaries differ")
    for summary in report["sources"]:
        keys(summary, "sourceId configuration clockId session currentSessionFrames cumulativeFrames cumulativeRawBytes", "run source")
        source = summary["sourceId"]
        require(type(source) is int and source in current, "unknown run source")
        session = current[source]
        require(summary == dict(sourceId=source, configuration=session["configuration"], clockId=session["clockId"], session=session["session"],
                                currentSessionFrames=session["frames"], cumulativeFrames=source_totals[source]["frames"],
                                cumulativeRawBytes=source_totals[source]["bytes"]), "run source summary differs")
    require({summary["sourceId"] for summary in report["sources"]} == {7, 8}, "duplicate run source summary")
    result = compare(sessions, windows)
    result["sessions"] = [dict((key, value) for key, value in session.items() if key != "trades") |
                          dict(trades=len(session["trades"]), firstTradeId=session["trades"][0]["id"],
                               lastTradeId=session["trades"][-1]["id"], firstTradeReceiveTimeNanos=session["trades"][0]["receiveTimeNanos"],
                               lastTradeReceiveTimeNanos=session["trades"][-1]["receiveTimeNanos"]) for session in sessions]
    if local_synthetic:
        for output, session in zip(result["sessions"], sessions):
            output["tradeIds"] = [trade["id"] for trade in session["trades"]]
    return result


def loopback_endpoint(value):
    require(type(value) is str, "local endpoint is not text")
    endpoint = urlsplit(value)
    port = endpoint.port
    require(port is not None and 1 <= port <= 65535 and value == "ws://127.0.0.1:" + str(port) + "/ws",
            "local endpoint must be exact unencrypted IPv4 loopback /ws with an explicit port")


def verify_local_manifest(data, result, run_times):
    path = data / "local-expected.json"
    manifest = decode(path.read_text(encoding="utf-8"))
    keys(manifest, "provenance runs", "local expected manifest")
    require(manifest["provenance"] == LOCAL_PROVENANCE and type(manifest["runs"]) is list,
            "local expected provenance/runs differ")
    clocks = {clock["clockId"]: clock for clock in result["clocks"]}
    retained_runs = {run["clockId"]: run for run in run_times}
    sessions = {(session["clockId"], session["sourceId"]): session for session in result["sessions"]}
    require(len(manifest["runs"]) == len(clocks), "local expected run count differs")
    seen = set()
    for run in manifest["runs"]:
        keys(run, "clockId sources expectedMatchedTrades jsonEndpoint sbeEndpoint", "local expected run")
        clock = run["clockId"]
        require(integer(clock, 1) and clock in clocks and clock not in seen
                and integer(run["expectedMatchedTrades"], 1), "local expected clock/match count differs")
        seen.add(clock)
        require(all(run[field] == retained_runs[clock][field] for field in ("jsonEndpoint", "sbeEndpoint")),
                "local expected source endpoints differ")
        require(type(run["sources"]) is list and len(run["sources"]) == 2, "local expected sources differ")
        expected_ids = {}
        for source in run["sources"]:
            keys(source, "sourceId rawBase64 tradeIds", "local expected source")
            source_id = source["sourceId"]
            require(type(source_id) is int and source_id in (7, 8) and source_id not in expected_ids
                    and type(source["rawBase64"]) is list and source["rawBase64"]
                    and type(source["tradeIds"]) is list and source["tradeIds"], "local expected source/frame/trade shape differs")
            for frame in source["rawBase64"]:
                require(type(frame) is str and len(frame) <= (MAX_RAW_BYTES + 2) // 3 * 4, "local expected frame is not bounded Base64")
                raw = base64.b64decode(frame, validate=True)
                require(0 < len(raw) <= MAX_RAW_BYTES and base64.b64encode(raw).decode("ascii") == frame,
                        "local expected frame is not canonical bounded Base64")
            require(all(integer(trade_id, 1) and trade_id <= 0x7FFF_FFFF_FFFF_FFFF for trade_id in source["tradeIds"]),
                    "local expected trade identity differs")
            captured = sessions[clock, source_id]
            require(captured["rawFrames"] == source["rawBase64"], "local expected complete raw frame order differs")
            require(captured["tradeIds"] == source["tradeIds"], "local expected decoded trade order differs")
            expected_ids[source_id] = set(source["tradeIds"])
        count = run["expectedMatchedTrades"]
        require(expected_ids[7] == expected_ids[8] and len(expected_ids[7]) == count
                and clocks[clock]["matchedUniqueTrades"] == count
                and all(source["unmatchedUniqueTrades"] == 0 for source in clocks[clock]["sources"]),
                "local expected complete matched trade coverage differs")
        clocks[clock]["expectedMatchedTrades"] = count
    for session in result["sessions"]:
        del session["rawFrames"], session["tradeIds"]
    result.update(localExpectedSha256=common.digest(path), expectedRawVerified=True, expectedTradeOrderVerified=True,
                  expectedPayloadsEquivalent=all(clock["payloadEquivalentUniqueTrades"] == clock["expectedMatchedTrades"]
                                                 for clock in result["clocks"]))


def audit(data, local_synthetic=False):
    validate_schema()
    runs = sorted((int(match.group(1)), path) for path in data.glob("run-*.json")
                  if (match := re.fullmatch(r"run-([1-9][0-9]*)\.json", path.name)))
    require(runs, "no completed paired runs")
    verified, checks, previous, previous_head = set(), [], None, 0
    run_times, windows = [], {}
    for number, path in runs:
        report = decode(path.read_text(encoding="utf-8"))
        keys(report, RUN_FIELDS + (" credentialKind" if local_synthetic else ""), "run report")
        require(report["mode"] == (LOCAL_MODE if local_synthetic else LIVE_MODE)
                and type(report["clockId"]) is int and report["clockId"] == number
                and report["previousCanonicalHead"] == previous_head, "paired run lineage/endpoints differ")
        if local_synthetic:
            loopback_endpoint(report["jsonEndpoint"])
            loopback_endpoint(report["sbeEndpoint"])
            require(report["jsonEndpoint"] != report["sbeEndpoint"] and report["credentialKind"] == "local-dummy",
                    "local peers/dummy credential provenance differs")
        else:
            require(report["jsonEndpoint"] == JSON_ENDPOINT and report["sbeEndpoint"] == SBE_ENDPOINT,
                    "live paired endpoints differ")
        measured_start, measured_end = report["measurementStartTimeNanos"], report["measurementEndTimeNanos"]
        require(integer(measured_start) and integer(measured_end) and measured_start < measured_end
                and integer(report["seconds"], 1), "invalid paired measurement interval")
        require(type(report["clusterSessionTimeoutNs"]) is int and report["clusterSessionTimeoutNs"] == 120_000_000_000,
                "paired Cluster session timeout differs")
        windows[number] = measured_start, measured_end
        canonical = data / ("canonical-" + str(number) + ".jsonl")
        if previous is not None:
            common.prefix(previous, canonical)
            checks.append(dict(previous=previous.name, current=canonical.name, identicalPrefix=True))
        result = audit_export(canonical, data / ("definitions-" + str(number) + ".jsonl"), report, data, verified, windows, local_synthetic)
        latest = [s for s in result["sessions"] if s["clockId"] == number]
        run_times.append(dict(clockId=number, firstReceiveTimeNanos=min(s["firstReceiveTimeNanos"] for s in latest),
                              lastReceiveTimeNanos=max(s["lastReceiveTimeNanos"] for s in latest),
                              clusterSessionTimeoutNs=report["clusterSessionTimeoutNs"],
                              jsonEndpoint=report["jsonEndpoint"], sbeEndpoint=report["sbeEndpoint"]))
        previous, previous_head = canonical, report["canonicalHead"]
    if local_synthetic:
        verify_local_manifest(data, result, run_times)
    result.update(status=("passed-local-synthetic" if local_synthetic else "passed") if result["payloadMismatchCount"] == 0
                  and all(clock["matchedWithinObservedOverlap"] > 0 for clock in result["clocks"]) else "failed",
                  mode=LOCAL_MODE if local_synthetic else LIVE_MODE, synthetic=local_synthetic, venueData=not local_synthetic,
                  runs=len(runs), canonicalHead=previous_head, runTimes=run_times, restartPrefixChecks=checks,
                  retainedArtifactSha256=sorted(verified), sbeSchemaSha256=SCHEMA_SHA256)
    return result


def main():
    if sys.argv[1:] == ["--fetch-schema"]:
        print(fetch_schema(), SCHEMA_FILE)
        return 0
    if sys.argv[1:] == ["--check-schema"]:
        print(validate_schema(), SCHEMA_FILE)
        return 0
    local_synthetic = sys.argv[1:2] == ["--local-synthetic"]
    require(len(sys.argv) == (3 if local_synthetic else 2),
            "usage: binance_pair_audit.py --fetch-schema | --check-schema | [--local-synthetic] DATA_DIR")
    data = Path(sys.argv[-1]).resolve()
    result = audit(data, local_synthetic)
    (data / "pair-audit.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, separators=(",", ":")))
    return 0 if result["status"] in ("passed", "passed-local-synthetic") else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, ArithmeticError, UnicodeError, struct.error) as failure:
        print("paired audit failed: " + str(failure), file=sys.stderr)
        sys.exit(1)
