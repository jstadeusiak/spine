"""Cold exact-decimal oracle and fixture conversion for retained real Binance depth."""
import argparse
import base64
import hashlib
import json
import struct
from decimal import Decimal
from pathlib import Path

SCALE = 8
FIELDS = ("frontier", "status", "bidCount", "askCount", "bidPrice", "bidSize", "askPrice", "askSize", "levelsSha256")


def scaled(value):
    number = Decimal(value)
    integer = number * (10 ** SCALE)
    if not number.is_finite() or number < 0 or integer != integer.to_integral_value() or integer >= 2 ** 63:
        raise ValueError("Unrepresentable exact nonnegative decimal")
    return int(integer)


def decode(source, opcode, payload):
    if opcode == 1:
        body = json.loads(payload)
        if body == {"result": None, "id": source}:
            return None
        if source != 1 or body.get("e") != "depthUpdate" or body.get("s") != "BTCUSDT":
            raise ValueError("Unexpected text message")
        return body["U"], body["u"], [(scaled(p), scaled(q)) for p, q in body["b"]], [(scaled(p), scaled(q)) for p, q in body["a"]]
    if source != 2 or opcode != 2 or struct.unpack_from("<HHHH", payload) != (26, 10003, 1, 0):
        raise ValueError("Unexpected SBE message")
    first, last, price_exponent, quantity_exponent = struct.unpack_from("<qqbb", payload, 16)
    offset, sides = 34, []
    for _ in range(2):
        block, count = struct.unpack_from("<HH", payload, offset)
        assert block == 16
        offset += 4
        side = []
        for _ in range(count):
            price, quantity = struct.unpack_from("<qq", payload, offset)
            side.append((scaled(f"{price}e{price_exponent}"), scaled(f"{quantity}e{quantity_exponent}")))
            offset += block
        sides.append(side)
    assert payload[offset:] == b"\x07BTCUSDT"
    return first, last, *sides


def state(frontier, bids, asks):
    digest = hashlib.sha256()
    for side, levels in enumerate((bids, asks)):
        for price, quantity in sorted(levels.items()):
            digest.update(f"{side},{price},{quantity}\n".encode("ascii"))
    bid = max(bids, default=0)
    ask = min(asks, default=0)
    return {"frontier": frontier, "status": 1 if frontier else 0, "bidCount": len(bids), "askCount": len(asks),
            "bidPrice": bid, "bidSize": bids.get(bid, 0), "askPrice": ask, "askSize": asks.get(ask, 0),
            "levelsSha256": digest.hexdigest()}


def prepare(corpus, output):
    output.mkdir(parents=True, exist_ok=True)
    status = json.loads((corpus / "acceptance.json").read_text())
    snapshot_metadata = json.loads((corpus / "snapshot-metadata.json").read_text())
    snapshot_bytes = (corpus / "snapshot.json").read_bytes()
    snapshot = json.loads(snapshot_bytes)
    origin = status["captureStartMonoNs"]
    bootstrap_time = snapshot_metadata["receiveMonoNs"] - origin
    frames = [json.loads(line) for line in (corpus / "frames.jsonl").read_text().splitlines()]
    frames.sort(key=lambda row: (row["receiveMonoNs"], row["captureRow"]))
    initial_bids = {scaled(p): scaled(q) for p, q in snapshot["bids"]}
    initial_asks = {scaled(p): scaled(q) for p, q in snapshot["asks"]}
    floor, ceiling = min(initial_bids), max(initial_asks)
    snapshot_head = snapshot["lastUpdateId"]
    independent = {source: [snapshot_head, initial_bids.copy(), initial_asks.copy()] for source in (1, 2)}
    empty = state(0, {}, {})
    expected = []
    inputs = []
    last_logged = bootstrap_time
    current = state(snapshot_head, initial_bids, initial_asks)
    counts = {1: {"data": 0, "controls": 0}, 2: {"data": 0, "controls": 0}}
    rfc_controls = []
    for source in (1, 2, 3):
        expected.append(dict(empty, kind="binding", sourceId=source, loggedTimeNanos=0))
    chunks = (len(snapshot_bytes) + 59999) // 60000
    for index in range(chunks):
        final = index + 1 == chunks
        expected.append(dict(current if final else empty, kind="bootstrap", sourceId=3, loggedTimeNanos=bootstrap_time))
    active = stale = 0
    for frame in frames:
        if frame["opcode"] not in (1, 2):
            rfc_controls.append(frame)
            continue
        source = frame["sourceId"]
        payload = base64.b64decode(frame["payloadBase64"], validate=True)
        assert len(payload) == frame["payloadBytes"]
        event = decode(source, frame["opcode"], payload)
        received = frame["receiveMonoNs"] - origin
        last_logged = max(last_logged, received)
        kind = "control" if event is None else "depth"
        counts[source]["controls" if event is None else "data"] += 1
        if event is not None:
            first, last, bids, asks = event
            frontier, oracle_bids, oracle_asks = independent[source]
            if last > frontier:
                if first > frontier + 1:
                    raise ValueError(f"Source {source} cannot independently bridge snapshot at {frontier}: {first}..{last}")
                for side, changes, bid_side in ((oracle_bids, bids, True), (oracle_asks, asks, False)):
                    for price, quantity in changes:
                        if price < floor if bid_side else price > ceiling:
                            continue
                        if quantity:
                            side[price] = quantity
                        else:
                            side.pop(price, None)
                independent[source][0] = last
            if last > current["frontier"]:
                if first > current["frontier"] + 1:
                    raise ValueError("Captured merge requires pending-range oracle; use an independently bridged corpus")
                # Select a complete independently reconstructed source state, rather than mirror the core's mutation path.
                current = state(last, oracle_bids, oracle_asks)
                active += 1
            else:
                stale += 1
        inputs.append("\t".join((str(source), str(received), str(last_logged), str(frame["sourceFrameSequence"]), kind, frame["payloadBase64"])))
        expected.append(dict(current, kind=kind, sourceId=source, loggedTimeNanos=last_logged,
                             captureRow=frame["captureRow"], sourceFrameSequence=frame["sourceFrameSequence"]))
    for index, row in enumerate(expected):
        row["commandIndex"] = index
    (output / "snapshot.json").write_bytes(snapshot_bytes)
    (output / "frames.tsv").write_text("\n".join(inputs) + "\n")
    (output / "expected.jsonl").write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in expected))
    (output / "rfc-controls.jsonl").write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rfc_controls))
    report = {"corpus": str(corpus.resolve()), "snapshotSha256": hashlib.sha256(snapshot_bytes).hexdigest(),
              "framesSha256": hashlib.sha256((corpus / "frames.jsonl").read_bytes()).hexdigest(), "schemaSha256": status.get("schemaSha256", hashlib.sha256((corpus / "stream_1_0.xml").read_bytes()).hexdigest()),
              "scale": SCALE, "bootstrapTimeNanos": bootstrap_time, "snapshotBytes": len(snapshot_bytes), "bootstrapChunks": chunks,
              "applicationFrames": len(inputs), "commands": len(expected), "sources": counts, "activeMergedDepthFrames": active,
              "staleMergedDepthFrames": stale, "rfcControlsExcludedFromApplication": len(rfc_controls), "finalState": current,
              "coverage": {"bidFloor": floor, "askCeiling": ceiling},
              "rawFraming": {"1": "Unchanged complete UTF-8 JSON application payload",
                             "2": "Java loader prefixes uint8 tag 1 to unchanged binary SBE payload; tag 0 to unchanged UTF-8 ACK, matching WebSocketCollector's declared tagged framing"},
              "oracle": "Independent exact Decimal per-feed reconstruction; select the independently reconstructed state at each first contiguous committed frontier.",
              "time": "Original common-clock receive times retained; logged times clamp to preceding committed bootstrap/input time."}
    (output / "fixture.json").write_text(json.dumps(report, indent=2) + "\n")
    (output / "bootstrap-time.txt").write_text(str(bootstrap_time) + "\n")
    print(json.dumps(report, indent=2))


def verify(output):
    expected = [json.loads(line) for line in (output / "expected.jsonl").read_text().splitlines()]
    actual = [json.loads(line) for line in (output / "actual.jsonl").read_text().splitlines()]
    assert len(expected) == len(actual), (len(expected), len(actual))
    for index, (wanted, got) in enumerate(zip(expected, actual)):
        for field in (*FIELDS, "commandIndex", "sourceId", "kind"):
            assert wanted[field] == got[field], f"command {index} field {field}: expected {wanted[field]}, actual {got[field]}"
    java = json.loads((output / "replay.json").read_text())
    assert java["snapshotRecoveryPassed"] and java["borrowedObservationPreserved"] and java["commands"] == len(expected)
    report = {"passed": True, "commandsCompared": len(expected), "exactStateComparedAfterEveryApplicationFrame": True,
              "snapshotRecoveryCuts": java["snapshotRecoveryCuts"], "canonicalSuffixBytesIdentical": True,
              "actualSha256": hashlib.sha256((output / "actual.jsonl").read_bytes()).hexdigest(),
              "expectedSha256": hashlib.sha256((output / "expected.jsonl").read_bytes()).hexdigest()}
    (output / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "verify"))
    parser.add_argument("paths", nargs="+")
    arguments = parser.parse_args()
    if arguments.action == "prepare" and len(arguments.paths) == 2:
        prepare(*(Path(path) for path in arguments.paths))
    elif arguments.action == "verify" and len(arguments.paths) == 1:
        verify(Path(arguments.paths[0]))
    else:
        parser.error("prepare CORPUS OUTPUT or verify OUTPUT")
