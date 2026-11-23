#!/usr/bin/env python3
"""Cold audit of BinanceJsonSmoke exports; upstream trade-ID gaps are diagnostics."""

import hashlib
import json
import re
import sys
import time
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote, urlsplit


def require(condition, message):
    if not condition:
        raise ValueError(message)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def decode(text):
    def invalid(value):
        raise ValueError("nonfinite JSON number: " + value)
    return json.loads(text, object_pairs_hook=pairs, parse_constant=invalid)


def rows(path):
    with path.open(encoding="utf-8") as source:
        return [decode(line) for line in source]


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def keys(value, expected, label):
    require(type(value) is dict and set(value) == set(expected.split()), label + " fields differ")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            result.update(block)
    return result.hexdigest()


def prefix(previous, current):
    with previous.open("rb") as old, current.open("rb") as new:
        for block in iter(lambda: old.read(1 << 20), b""):
            require(new.read(len(block)) == block, "canonical restart export changed previous bytes")


def profile(row, data, verified):
    value = decode(row["profile"])
    keys(value, "profile venue channel parser raw native clock", "profile")
    require(value["profile"] == "spine.capture.v1" and value["venue"] == "binance-spot"
            and value["channel"] == "trade", "unsupported interpretation profile")
    keys(value["parser"], "artifact sha256 entrypoint parameters", "parser")
    keys(value["raw"], "boundary representation transformation", "raw")
    keys(value["native"], "namespace representation", "native")
    keys(value["clock"], "domain origin resolutionNanos receive complete ready", "clock")
    for name in ("parser", "raw", "native", "clock"):
        for key, field in value[name].items():
            if key not in ("parameters", "resolutionNanos"):
                require(type(field) is str and field.strip(), name + "." + key + " must be a nonempty string")
    require(integer(value["clock"]["resolutionNanos"], 1), "invalid clock resolution")
    require(value["raw"]["representation"] == "UTF-8"
            and value["native"]["representation"] == "absent; identity retained in raw JSON", "unsupported raw/native encoding")
    parameters = value["parser"]["parameters"]
    require(type(parameters) is dict and parameters.get("format") == "JSON"
            and parameters.get("timeUnit") == "MICROSECOND"
            and "applicationHeartbeat" in parameters and parameters["applicationHeartbeat"] is None,
            "JSON/microsecond/heartbeat parameters differ")
    subscription = parameters.get("subscription")
    keys(subscription, "method params id", "subscription")
    require(subscription["method"] == "SUBSCRIBE" and subscription["params"] == ["btcusdt@trade"]
            and type(subscription["id"]) is int and subscription["id"] == 1, "subscription differs")
    sha = value["parser"]["sha256"]
    require(re.fullmatch(r"[0-9a-f]{64}", sha) is not None, "invalid retained SHA-256")
    artifact = urlsplit(value["parser"]["artifact"])
    require(artifact.scheme == "file" and not artifact.query and not artifact.fragment
            and Path(unquote(artifact.path)).name == sha + ".jar", "artifact reference differs from retained digest")
    if sha not in verified:
        require(digest(data / "artifacts" / (sha + ".jar")) == sha, "retained executable hash differs")
        verified.add(sha)
    return sha


def audit_export(path, definition_path, report, data, verified):
    events, definitions = rows(path), []
    sessions, current, seen_ids = [], None, set()
    raw_bytes = frames = 0
    future = time.time_ns() // 1000 + 86_400_000_000
    for sequence, row in enumerate(events, 1):
        require(row["sequence"] == sequence and row["logId"] == 42 and row["sourceId"] == 7,
                "canonical sequence/log/source differs")
        configuration, clock = row["configuration"], row["clockId"]
        require(integer(configuration, 1) and configuration < 0xFFFF_FFFF
                and integer(clock, 1) and clock < 0xFFFF_FFFF, "invalid numeric binding")
        if row["kind"] == "definition":
            require(current is None or configuration > current["configuration"] and clock > current["clockId"],
                    "configuration/clock did not advance")
            require(row["provider"] == "binance-public" and row["product"] == "BTCUSDT"
                    and row["session"] == "ws-json-" + str(configuration), "definition source/session differs")
            require(row["receiveTimeNanos"] == -1 and row["completeTimeNanos"] == -1
                    and integer(row["readyTimeNanos"]) and row["syncUncertaintyNanos"] == -1, "definition clock contract differs")
            sha = profile(row, data, verified)
            current = dict(configuration=configuration, clockId=clock, session=row["session"], frames=0,
                           trades=0, acknowledgements=0, rawBytes=0, firstTradeId=None, lastTradeId=None,
                           observedForwardIdGapRanges=0, observedForwardIdGapSize=0,
                           duplicateTradeIds=0, decreasingTradeIds=0, bundleSha256=sha)
            seen_ids = set()
            sessions.append(current)
            definitions.append(row)
            continue
        require(row["kind"] == "observation" and current is not None, "unbound or unknown canonical message")
        require(configuration == current["configuration"] and clock == current["clockId"]
                and row["session"] == current["session"] and row["rawFrameSequence"] == current["frames"] + 1,
                "raw frame sequence/binding differs")
        receive, complete, ready = (row[key] for key in ("receiveTimeNanos", "completeTimeNanos", "readyTimeNanos"))
        require(integer(receive) and integer(complete) and integer(ready) and receive <= complete <= ready,
                "receive <= complete <= ready failed")
        require(row["nativeNumericId"] == "18446744073709551615", "native numeric ID must be unavailable")
        require(type(row["raw"]) is str and integer(row["rawByteLength"], 1)
                and len(row["raw"].encode("utf-8")) == row["rawByteLength"], "raw UTF-8 byte count differs")
        raw = decode(row["raw"])
        require(type(raw) is dict, "application frame is not an object")
        if set(raw) == {"result", "id"}:
            require(raw["result"] is None and type(raw["id"]) is int and raw["id"] == 1, "subscription acknowledgement differs")
            current["acknowledgements"] += 1
        else:
            require(raw.get("e") == "trade" and raw.get("s") == "BTCUSDT", "unexpected Binance application frame")
            trade_id = raw["t"]
            require(integer(trade_id, 1) and type(raw["m"]) is bool, "invalid trade ID/maker flag")
            for name in ("p", "q"):
                require(type(raw[name]) is str, "price/quantity must retain decimal text")
                number = Decimal(raw[name])
                require(number.is_finite() and number > 0, "nonpositive/nonfinite price or quantity")
            require(all(integer(raw[key], 1) and 1_577_836_800_000_000 <= raw[key] <= future for key in ("E", "T")),
                    "exchange event/trade timestamp is not plausible epoch microseconds")
            previous = current["lastTradeId"]
            current["duplicateTradeIds"] += trade_id in seen_ids
            current["decreasingTradeIds"] += previous is not None and trade_id < previous
            seen_ids.add(trade_id)
            if previous is None:
                current["firstTradeId"] = trade_id
            elif trade_id > previous + 1:
                current["observedForwardIdGapRanges"] += 1
                current["observedForwardIdGapSize"] += trade_id - previous - 1
            current["lastTradeId"] = trade_id
            current["trades"] += 1
        current["frames"] += 1
        current["rawBytes"] += row["rawByteLength"]
        frames += 1
        raw_bytes += row["rawByteLength"]
    require(sessions and all(session["trades"] > 0 for session in sessions), "each session must contain live trades")
    require(rows(definition_path) == definitions, "definitions sidecar differs from canonical history")
    require(report["canonicalHead"] == len(events) and report["cumulativeFrames"] == frames
            and report["cumulativeRawBytes"] == raw_bytes and report["definitions"] == len(sessions)
            and report["configuration"] == current["configuration"] and report["clockId"] == current["clockId"]
            and report["currentSessionFrames"] == current["frames"] and report["bundleSha256"] == current["bundleSha256"]
            and report["timestampViolations"] == 0, "run report differs from captured history")
    return sessions, frames, raw_bytes, len(events)


def main():
    require(len(sys.argv) == 2, "usage: binance_json_audit.py DATA_DIR")
    data = Path(sys.argv[1]).resolve()
    runs = sorted((int(match.group(1)), path) for path in data.glob("run-*.json")
                  if (match := re.fullmatch(r"run-([1-9][0-9]*)\.json", path.name)))
    require(runs, "no completed smoke runs")
    verified, checks, previous, previous_head = set(), [], None, 0
    for number, path in runs:
        report = decode(path.read_text(encoding="utf-8"))
        require(report["mode"] == "binance-spot-json" and report["configuration"] == number
                and report["endpoint"] == "wss://data-stream.binance.vision/ws?timeUnit=MICROSECOND"
                and report["previousCanonicalHead"] == previous_head, "run lineage/configuration differs")
        canonical = data / ("canonical-" + str(number) + ".jsonl")
        if previous is not None:
            prefix(previous, canonical)
            checks.append(dict(previous=previous.name, current=canonical.name, identicalPrefix=True))
        sessions, frames, raw_bytes, head = audit_export(canonical, data / ("definitions-" + str(number) + ".jsonl"),
                                                        report, data, verified)
        previous, previous_head = canonical, head
    result = dict(status="passed", runs=len(runs), canonicalHead=head, frames=frames, rawBytes=raw_bytes,
                  trades=sum(session["trades"] for session in sessions), acknowledgements=sum(session["acknowledgements"] for session in sessions),
                  sessionCount=len(sessions), sessions=sessions, restartPrefixChecks=checks,
                  retainedArtifactSha256=sorted(verified), upstreamIdOrderingAndGapsAreDiagnostics=True,
                  sessionBoundaryTradeIdSteps=[dict(previousConfiguration=old["configuration"], configuration=new["configuration"],
                                                   idStep=new["firstTradeId"] - old["lastTradeId"])
                                              for old, new in zip(sessions, sessions[1:])])
    text = json.dumps(result, indent=2) + "\n"
    (data / "audit.json").write_text(text, encoding="utf-8")
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, ArithmeticError) as failure:
        print("audit failed: " + str(failure), file=sys.stderr)
        sys.exit(1)
