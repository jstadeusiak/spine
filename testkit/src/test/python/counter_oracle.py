"""Independent literal counter oracle: Python struct and fixed logical facts, no Spine/codec execution.

Run explicitly only when reviewing an intentional wire/fixture contract change. Tests read the retained outputs;
they never regenerate their expected bytes from the implementation under test.
"""
from pathlib import Path
import hashlib
import json
import struct

out = Path(__file__).resolve().parents[1] / "resources" / "counter-oracle"
out.mkdir(parents=True, exist_ok=True)

# producer, producer sequence, delta, logged time, admission, total, domain accepted, canonical sequence
facts = [
    (31, 1, 3, 100, 1, 3, 1, 1),
    (31, 2, 5, 101, 1, 8, 1, 2),
    (31, 2, 5, 999, 2, 8, 1, 0),
    (41, 1, 7, 102, 1, 15, 1, 3),
    (31, 3, -1, 103, 1, 15, 0, 4),
    (31, 4, 9223372036854775807, 104, 1, 15, 0, 5),
    (31, 5, 8, 105, 1, 23, 1, 6),
    (31, 7, 1, 106, 7, 23, 0, 0),
    (31, 6, 2, 107, 1, 25, 1, 7),
]


def header(block, template, schema=101, version=1):
    return struct.pack("<HHHH", block, template, schema, version)


def framed(data):
    return struct.pack(">I", len(data)) + data


commands = bytearray()
events = bytearray()
cache = {}
for producer, submitted, delta, time, admission, total, accepted, canonical in facts:
    command = header(8, 1, 501, 1) + struct.pack("<q", delta)
    proposal = header(60, 3) + struct.pack("<QQQIIIqqqI", 17, producer, submitted, 1, 1, 1, 90, 91, 92, 16) + command
    commands += struct.pack(">q", time) + framed(proposal)
    if admission != 1:
        events += framed(b"")
        continue
    result = header(17, 2, 501, 1) + struct.pack("<qqB", total, time, accepted)
    payload = result + struct.pack("<I", 16) + command
    event = header(60, 1) + struct.pack("<QQqIIIqqqI", 17, canonical, time, 1, 1, 1, 90, 91, 92, len(payload)) + payload
    events += framed(event)
    cache[producer] = (struct.pack("<QQQq", producer, submitted, canonical, time)
                       + hashlib.sha256(proposal).digest() + struct.pack("<I", 25) + result)

state = header(8, 3, 501, 1) + struct.pack("<q", 25)
snapshot = bytearray(header(60, 6) + struct.pack("<QQIII", 17, 7, 501, 1, 1) + bytes(32)
                     + struct.pack("<HH", 64, 2) + cache[31] + cache[41] + struct.pack("<I", 16) + state)
snapshot[36:68] = hashlib.sha256(snapshot[:36] + snapshot[68:]).digest()
assert len(snapshot) == 278
(out / "commands-and-time.bin").write_bytes(commands)
(out / "canonical.bin").write_bytes(events)
(out / "final-snapshot.bin").write_bytes(snapshot)
report = {"commands": len(facts), "snapshotCuts": len(facts) + 1,
          "canonicalSha256": hashlib.sha256(events).hexdigest(),
          "finalSnapshotSha256": hashlib.sha256(framed(snapshot)).hexdigest()}
(out / "expected.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report, indent=2))
