"""Compare three JVMs against the same immutable Java 21 core replay executable."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def run_logged(command, directory, name):
    with (directory / f"{name}.stdout.log").open("w") as stdout, \
            (directory / f"{name}.stderr.log").open("w") as stderr:
        subprocess.run(command, stdout=stdout, stderr=stderr, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--java", required=True, nargs=3, type=Path,
                        metavar="JAVA", help="Java executables in baseline, candidate one, candidate two order")
    args = parser.parse_args()
    bundle, output = args.bundle.resolve(), args.output.resolve()
    if platform.system() != "Darwin":
        raise RuntimeError("This evidence runner uses macOS /usr/bin/time -l; no cross-platform RSS unit assumption")
    for key in ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
        if os.environ.get(key):
            raise RuntimeError(f"Unset {key} before a controlled JVM comparison")
    for row in json.loads((bundle / "files.json").read_text()):
        path = bundle / row["path"]
        if path.stat().st_size != row["bytes"] or digest(path) != row["sha256"]:
            raise RuntimeError("Immutable input digest differs: " + row["path"])
    config = json.loads((bundle / "runtime/execution.json").read_text())
    classpath = os.pathsep.join(str(bundle / "runtime" / row["entry"]) for row in config["classpathEntries"])
    oracle = bundle / "runtime/sources/sequencer/src/test/python/binance_book_replay.py"
    baseline_verification = json.loads((bundle / "recorded/verification.json").read_text())
    baseline_replay = json.loads((bundle / "recorded/replay.json").read_text())
    output.mkdir(parents=True, exist_ok=False)
    jvms = []
    for index, java in enumerate(args.java):
        java = java.resolve()
        directory = output / f"jvm-{index}"
        directory.mkdir()
        run_logged([str(java), "-version"], directory, "version")
        run_logged([str(java), "-XX:+UseG1GC", "-Xms512m", "-Xmx512m", "-XX:+PrintFlagsFinal", "-version"],
                   directory, "flags")
        release = java.parent.parent / "release"
        (directory / "release.txt").write_bytes(release.read_bytes())
        jvms.append({"index": index, "java": str(java), "javaSha256": digest(java),
                     "libjvmSha256": digest(java.parent.parent / "lib/server/libjvm.dylib"),
                     "version": (directory / "version.stderr.log").read_text().strip()})
    summary = {"platform": platform.platform(), "architecture": platform.machine(), "cpuCount": os.cpu_count(),
               "bundle": str(bundle), "bundleManifestSha256": digest(bundle / "files.json"),
               "scriptSha256": digest(Path(__file__)), "jvms": jvms, "rounds": 3,
               "order": [[(round_number + index) % 3 for index in range(3)] for round_number in range(3)],
               "measurement": "Frozen core only, 40 full-corpus warmup replays and 400 measured replays; explicit G1, 512 MiB heap. "
                              "Transport excluded. Whole Java process CPU/RSS includes cold oracle hashing, restore and warmup. "
                              "GC/safepoint logging enabled equally. Timings are independent run distributions, not pooled percentiles.",
               "passed": False, "runs": []}
    write_json(output / "summary.json", summary)
    for round_number, order in enumerate(summary["order"]):
        for index in order:
            directory = output / f"round-{round_number + 1}-jvm-{index}"
            directory.mkdir()
            arguments = config["argumentsFromFrozenDirectory"].copy()
            arguments[arguments.index("-cp") + 1] = classpath
            arguments[-2:] = [str(directory), "400"]
            command = ["/usr/bin/time", "-l", jvms[index]["java"], "-XX:+UseG1GC",
                       f"-Xlog:gc*,safepoint:file={directory / 'gc-safepoint.log'}:time,uptime,level,tags", *arguments]
            row = {"round": round_number + 1, "jvm": index, "directory": str(directory), "command": command,
                   "passed": False}
            write_json(directory / "invocation.json", row)
            try:
                run_logged([sys.executable, "-I", str(oracle), "prepare", str(bundle / "corpus"), str(directory)], directory, "prepare")
                run_logged(command, directory, "java")
                run_logged([sys.executable, "-I", str(oracle), "verify", str(directory)], directory, "verify")
                verification = json.loads((directory / "verification.json").read_text())
                replay = json.loads((directory / "replay.json").read_text())
                profile = json.loads((directory / "profile.json").read_text())
                if verification != baseline_verification or replay != baseline_replay:
                    raise RuntimeError("Oracle, recovery cuts or exact final snapshot differs from the retained baseline")
                if profile["warmupReplays"] != 40 or profile["measuredReplays"] != 400:
                    raise RuntimeError("Frozen warmup or measurement count differs")
                timing = (directory / "java.stderr.log").read_text()
                cpu = re.search(r"([\d.]+)\s+real\s+([\d.]+)\s+user\s+([\d.]+)\s+sys", timing)
                rss = re.search(r"(\d+)\s+maximum resident set size", timing)
                if cpu is None or rss is None:
                    raise RuntimeError("macOS process CPU or maximum RSS measurement missing")
                row.update(passed=True, profile=profile, verification=verification,
                           realSeconds=float(cpu[1]), userSeconds=float(cpu[2]), systemSeconds=float(cpu[3]),
                           maximumRssBytes=int(rss[1]))
            except Exception as error:
                row["failure"] = repr(error)
                raise
            finally:
                write_json(directory / "invocation.json", row)
                summary["runs"].append(row)
                write_json(output / "summary.json", summary)
            print(json.dumps({key: row[key] for key in ("round", "jvm", "passed", "profile", "maximumRssBytes")}), flush=True)
    summary["passed"] = True
    write_json(output / "summary.json", summary)


if __name__ == "__main__":
    main()
