#!/usr/bin/env python3
"""Independent cold checks of retained local ingestion diagnostics; no performance budget or causal claim."""

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path


PHASES = ("warmup", "baseline", "burst")
PLAN = {7: (1, 1028, 137), 8: (4, 257, 140)}
BASE_METRICS = {
    "scheduleLatenessNanos": ("attemptedNanos", "intendedNanos"),
    "plannedToReceiveNanos": ("receiveNanos", "intendedNanos"),
    "completeToReadyNanos": ("readyNanos", "completeNanos"),
    "readyToRecordedConsumerNanos": ("recordedConsumerObservedNanos", "readyNanos"),
}
BOUNDARY_METRICS = {
    "readyToCaptureFenceUpperNanos": ("captureFenceUpperNanos", "readyNanos"),
    "captureFenceUpperToCanonicalPublicationNanos": ("canonicalPublicationObservedNanos", "captureFenceUpperNanos"),
    "canonicalPublicationToOutputFenceUpperNanos": ("outputFenceUpperNanos", "canonicalPublicationObservedNanos"),
    "outputFenceUpperToRecordedConsumerNanos": ("recordedConsumerObservedNanos", "outputFenceUpperNanos"),
    "captureFenceBracketNanos": ("captureFenceUpperNanos", "captureFenceLowerNanos"),
    "outputFenceBracketNanos": ("outputFenceUpperNanos", "outputFenceLowerNanos"),
}
BOUNDARY_COLUMNS = ("capturePublicationObservedNanos", "captureEndPosition", "captureFenceLowerNanos",
                    "captureFenceUpperNanos", "canonicalPublicationObservedNanos", "canonicalEndPosition",
                    "outputFenceLowerNanos", "outputFenceUpperNanos")
FIXED = dict(workload="fixed-local-collector-ingest-v1", warmupSeconds=2, warmupTradesPerSource=300,
             baselineSeconds=4, baselineTradesPerSource=600, baselineJsonFramesPerSecond=150,
             baselineSbeFramesPerSecond=37.5, burstTradesPerSource=128, finalDrainSeconds=30,
             jsonPlannedFrames=1028, sbePlannedFrames=257, sbeBatchTrades=4)
COMPARABLE = tuple(FIXED) + ("bundleSha256", "java", "os", "processors", "maxHeapBytes", "aeron", "agrona", "sbe",
                            "driver", "termBytes", "ipcMtuBytes", "archiveFileSyncLevel", "clusterNodes",
                            "clusterSessionTimeoutSeconds", "producerRetryNanos", "ownerIdle")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, label, minimum=None):
    require(type(value) is int, label + " must be an integer")
    require(minimum is None or value >= minimum, label + " below minimum")
    return value


def json_file(path):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON field " + key)
            result[key] = value
        return result
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)


def csv_file(path):
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames and len(reader.fieldnames) == len(set(reader.fieldnames)), "duplicate/missing CSV header")
        result = []
        for row in reader:
            require(None not in row and None not in row.values(), "CSV row/header length differs")
            result.append({key: value if key in ("phase", "kind") else None if value == "" else int(value)
                           for key, value in row.items()})
        return result


def stats(values):
    ordered = sorted(values)
    count = len(ordered)
    if not count:
        return dict(count=0, min=None, max=None, p50=None, p99=None, p999=None)
    return dict(count=count, min=ordered[0], max=ordered[-1],
                p50=ordered[(count * 50 + 99) // 100 - 1],
                p99=ordered[(count * 99 + 99) // 100 - 1],
                p999=ordered[(count * 999 + 999) // 1000 - 1])


def phase(trade):
    return "warmup" if trade < 300 else "baseline" if trade < 900 else "burst"


def present(row, key):
    return row.get(key) is not None and row[key] >= 0


def times(row, definition=False):
    for key in ("readyNanos", "recordedConsumerObservedNanos"):
        number(row.get(key), key, 0)
    require(row["readyNanos"] <= row["recordedConsumerObservedNanos"], "follower precedes ready")
    if definition:
        require(row.get("receiveNanos") == row.get("completeNanos") == -1, "definition observation times differ")
    else:
        number(row.get("receiveNanos"), "receiveNanos", 0)
        number(row.get("completeNanos"), "completeNanos", 0)
        require(row["receiveNanos"] <= row["completeNanos"] <= row["readyNanos"], "callback times out of order")


def boundaries(row, mode):
    if mode == "off":
        require(all(row.get(key) in (None, -1) for key in BOUNDARY_COLUMNS), "off mode contains observer samples")
        if row.get("canonicalSequence") not in (None, -1):
            number(row["canonicalSequence"], "canonicalSequence", 1)
        return
    for key in ("capturePublicationObservedNanos", "captureFenceUpperNanos", "canonicalPublicationObservedNanos", "outputFenceUpperNanos"):
        number(row.get(key), key, 0)
        require(row["readyNanos"] <= row[key], key + " precedes ready")
    for stem in ("capture", "output"):
        lower, upper = row.get(stem + "FenceLowerNanos"), row[stem + "FenceUpperNanos"]
        require(lower in (None, -1) or type(lower) is int and 0 <= lower <= upper, stem + " fence bracket differs")
    for key in ("captureEndPosition", "canonicalEndPosition"):
        number(row.get(key), key, 1)
        require(row[key] % 32 == 0, key + " is not frame aligned")
    number(row.get("canonicalSequence"), "canonicalSequence", 1)


def metrics(rows, names):
    return {name: stats(row[left] - row[right] for row in rows if present(row, left) and present(row, right))
            for name, (left, right) in names.items()}


def audit(directory):
    directory = Path(directory).resolve()
    manifest, report = (json_file(directory / name) for name in ("manifest.json", "report.json"))
    for key, expected in FIXED.items():
        require(manifest.get(key) == expected, "fixed workload differs: " + key)
    number(manifest.get("originNanos"), "originNanos")  # System.nanoTime's absolute origin may be negative.
    mode = manifest.get("observerMode")
    require(mode in ("off", "boundaries") and report.get("observerMode") == mode, "observer mode differs")
    require(manifest.get("fixedSourceProducerBindings") == [{"sourceId": 7, "producerId": 10}, {"sourceId": 8, "producerId": 11}],
            "fixed source/producer bindings differ")
    sha = manifest.get("bundleSha256")
    require(type(sha) is str and re.fullmatch(r"[0-9a-f]{64}", sha), "invalid executable SHA-256")
    artifact = directory / "artifacts" / (sha + ".jar")
    with artifact.open("rb") as stream:
        require(hashlib.file_digest(stream, "sha256").hexdigest() == sha, "retained executable digest differs")
    require(report.get("status") == "passed" and report.get("failure") is None, "profile reported failure")
    start, end = (number(report.get(key), key, 0) for key in ("measurementStartNanos", "drainCompletedNanos"))
    require(start <= end <= start + 36_000_000_000, "fixed final drain deadline differs")
    require(report.get("canonicalEventsObserved") == report.get("completeCanonicalHead") == 1289
            and report.get("acknowledgements") == 2, "complete canonical accounting differs")
    require(report.get("receiptDrainComplete") is True and report.get("snapshotRecordingFenceComplete") is True,
            "receipt/snapshot fence incomplete")
    rows = csv_file(directory / "frames.csv")
    require(len(rows) == 1285, "planned frame coverage differs")
    sources = report.get("sources")
    require(type(sources) is list and len(sources) == 2 and {item.get("sourceId") for item in sources} == {7, 8},
            "reported sources differ")
    verified = {}
    for source, (batch, count, payload) in PLAN.items():
        selected = [row for row in rows if row.get("source") == source]
        require(len(selected) == count and {row.get("frame") for row in selected} == set(range(2, count + 2)),
                "missing/duplicate source frame")
        selected.sort(key=lambda row: row["frame"])
        declared = next(item for item in sources if item["sourceId"] == source)
        for key, expected in dict(plannedTrades=1028, plannedFrames=count, attemptedFrames=count, completedWrites=count,
                                  completeSubmittedCommands=count + 2, admittedDataFramesFromCanonicalOutput=count,
                                  applicationPayloadBytes=payload, rawBytesPerDataFrame=payload + (source == 8)).items():
            require(declared.get(key) == expected, "source accounting differs: " + key)
        for key in ("capturePublicationCommands", "captureRecordingFences", "canonicalPublicationCommands", "outputRecordingFences"):
            require(declared.get(key) == (count + 2 if mode == "boundaries" else 0), "observer source accounting differs: " + key)
        require(declared.get("recordedFollowerCommands") == count + 2, "follower command accounting differs")
        for index, row in enumerate(selected):
            trade = index * batch
            require(row.get("trades") == batch and row.get("firstTradeId") == 100000 + trade
                    and row.get("payloadBytes") == payload and row.get("phase") == phase(trade), "trade/phase/frame plan differs")
            require(row.get("intendedNanos") == start + min(trade, 900) * 1_000_000_000 // 150, "intended schedule differs")
            number(row.get("attemptedNanos"), "attemptedNanos", row["intendedNanos"])
            number(row.get("writeFlushCompletedNanos"), "writeFlushCompletedNanos", row["attemptedNanos"])
            times(row)
            require(row["receiveNanos"] >= row["intendedNanos"] and end >= max(row["recordedConsumerObservedNanos"], row["writeFlushCompletedNanos"]),
                    "frame outside fixed measurement interval")
            require(row.get("producerId") == source + 3 and row.get("producerSequence") == index + 3, "producer identity/sequence differs")
            number(row.get("canonicalSequence"), "canonicalSequence", 1)
            boundaries(row, mode)
        phases = declared.get("phases")
        require(type(phases) is list and len(phases) == 3 and {item.get("phase") for item in phases} == set(PHASES), "reported phases differ")
        verified[source] = {}
        for name in PHASES:
            subset = [row for row in selected if row["phase"] == name]
            declared_phase = next(item for item in phases if item["phase"] == name)
            require(declared_phase.get("plannedFrames") == len(subset) and declared_phase.get("plannedTrades") == len(subset) * batch,
                    "phase accounting differs")
            computed = metrics(subset, BASE_METRICS | BOUNDARY_METRICS)
            for metric, expected in computed.items():
                require(declared_phase.get(metric) == expected, "nearest-rank statistics differ: " + metric)
            verified[source][name] = computed
    require({row["source"] for row in rows} == {7, 8}, "unknown CSV source")
    require((directory / "commands.csv").exists(), "profile lacks complete command table")
    commands(directory, rows, report, mode)
    return dict(status="passed", directory=str(directory), observerMode=mode, frames=1285, trades=2056,
                canonicalHead=1289, drainNanos=end - start, sources=verified, manifest=manifest,
                observer=report.get("observer"), commandCoverage=True)


def commands(directory, frames, report, mode):
    rows = csv_file(directory / "commands.csv")
    require(len(rows) == 1289, "complete command coverage differs")
    by_frame = {(row["source"], row["frame"]): row for row in frames}
    for source, (_, count, _) in PLAN.items():
        selected = sorted((row for row in rows if row.get("source") == source), key=lambda row: row.get("producerSequence", -1))
        require(len(selected) == count + 2, "command source coverage differs")
        for index, row in enumerate(selected):
            kind = "definition" if index == 0 else "acknowledgement" if index == 1 else "data"
            require(row.get("producerId") == source + 3 and row.get("producerSequence") == index + 1
                    and row.get("frame") == index and row.get("kind") == kind, "command identity/sequence/kind differs")
            times(row, index == 0)
            boundaries(row, mode)
            if index >= 2:
                data = by_frame[(source, index)]
                require(all(row.get(key) == value for key, value in data.items() if key not in
                            ("phase", "firstTradeId", "trades", "payloadBytes", "intendedNanos", "attemptedNanos", "writeFlushCompletedNanos")),
                        "frame/command correlation differs")
        if mode == "boundaries":
            require(all(left["captureEndPosition"] < right["captureEndPosition"] for left, right in zip(selected, selected[1:])),
                    "capture positions are not monotonic")
        if mode == "boundaries" or any(row.get("canonicalSequence") not in (None, -1) for row in selected):
            require(all(present(row, "canonicalSequence") for row in selected)
                    and all(left["canonicalSequence"] < right["canonicalSequence"] for left, right in zip(selected, selected[1:])),
                    "canonical sequence reorders source commands")
    require({row["source"] for row in rows} == {7, 8}, "unknown command source")
    sequences = [row.get("canonicalSequence") for row in rows]
    if mode == "boundaries" or any(value not in (None, -1) for value in sequences):
        require(set(sequences) == set(range(1, 1290)), "canonical sequence coverage differs")
    if mode != "boundaries":
        observer = report.get("observer")
        if observer is not None:
            require(all(observer.get(key) == 0 for key in ("polls", "totalPollNanos", "maxPollNanos"))
                    and observer.get("recordings") == [], "off observer accounting differs")
            number(observer.get("maxOwnerLoopGapNanos"), "maxOwnerLoopGapNanos", 0)
        return
    ordered = sorted(rows, key=lambda row: row["canonicalSequence"])
    require(all(left["canonicalEndPosition"] < right["canonicalEndPosition"] for left, right in zip(ordered, ordered[1:])),
            "canonical positions are not monotonic")
    observer = report.get("observer")
    require(type(observer) is dict, "observer report absent")
    for key in ("polls", "totalPollNanos", "maxPollNanos", "maxOwnerLoopGapNanos"):
        number(observer.get(key), "observer." + key, 0)
    require(observer["polls"] > 0 and observer["maxPollNanos"] <= observer["totalPollNanos"], "observer polling accounting differs")
    bindings = observer.get("recordings")
    require(type(bindings) is list and len(bindings) == 3 and {item.get("source") for item in bindings} == {0, 7, 8},
            "recording bindings absent/duplicated")
    for item in bindings:
        source = item["source"]
        require(item.get("streamId") == {0: 1001, 7: 8210, 8: 8211}[source], "recording stream differs")
        for key in ("recordingId", "startPosition", "counterId", "counterRegistrationId", "archiveId"):
            number(item.get(key), "recording." + key, 0)
        number(item.get("sessionId"), "recording.sessionId")
        require(item["startPosition"] % 32 == 0, "recording start is not frame aligned")
        relevant = rows if source == 0 else [row for row in rows if row["source"] == source]
        require(item.get("completeCommands") == item.get("recordingFences") == len(relevant), "recording command/fence coverage differs")
        position = "canonicalEndPosition" if source == 0 else "captureEndPosition"
        require(all(row[position] > item["startPosition"] for row in relevant), "end position precedes recording start")
    require(len({item["archiveId"] for item in bindings}) == 1
            and all(len({item[key] for item in bindings}) == 3 for key in ("recordingId", "sessionId", "counterId", "counterRegistrationId")),
            "recording bindings are not distinct")


def compare(left, right):
    require({left["observerMode"], right["observerMode"]} == {"off", "boundaries"}, "comparison needs off and boundaries runs")
    off, on = (left, right) if left["observerMode"] == "off" else (right, left)
    for key in COMPARABLE:
        require(off["manifest"].get(key) == on["manifest"].get(key), "comparison workload/environment differs: " + key)
    differences = {}
    for source in PLAN:
        differences[source] = {}
        for name in PHASES:
            differences[source][name] = {metric: {quantile: on["sources"][source][name][metric][quantile] - off["sources"][source][name][metric][quantile]
                                                   for quantile in ("min", "p50", "p99", "p999", "max")}
                                         for metric in BASE_METRICS}
    return dict(offDirectory=off["directory"], boundariesDirectory=on["directory"],
                meaning="descriptive boundaries-minus-off timing differences; scheduling/JIT/run variation and subscriber flow control are included; no causal overhead guarantee or changed budget",
                drainDifferenceNanos=on["drainNanos"] - off["drainNanos"], sources=differences, observer=on["observer"])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path)
    parser.add_argument("--compare", action="store_true", help="compare adjacent off/boundaries pairs descriptively")
    args = parser.parse_args(argv)
    results, failures = [], []
    for directory in args.directories:
        try:
            results.append(audit(directory))
        except (ValueError, OSError, KeyError, TypeError) as error:
            results.append(dict(status="failed", directory=str(directory.resolve()), failure=str(error)))
            failures.append(str(error))
    comparisons = []
    if args.compare:
        try:
            require(not failures and len(results) % 2 == 0, "comparison requires complete successful adjacent pairs")
            comparisons = [compare(results[index], results[index + 1]) for index in range(0, len(results), 2)]
        except (ValueError, KeyError, TypeError) as error:
            failures.append(str(error))
    for result in results:
        result.pop("manifest", None)
    print(json.dumps(dict(status="failed" if failures else "passed", audits=results, comparisons=comparisons, failures=failures), sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
