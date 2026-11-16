"""Compile/run an outside-checkout consumer using only locally staged Maven coordinates."""
import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--java-version", choices=("21", "25"), default="25")
parser.add_argument("--spine-version", default="1.0.0")
args = parser.parse_args()
template = Path(__file__).resolve().parent
root = template.parents[3]
repository = root / "build/repository"
if not repository.is_dir():
    parser.error("run ./gradlew stagePublications before the publication consumer gate")
with tempfile.TemporaryDirectory(prefix="spine-publication-consumer-") as temporary:
    consumer = Path(temporary)
    for name in ("settings.gradle.kts", "build.gradle.kts"):
        shutil.copy2(template / name, consumer / name)
    shutil.copytree(template / "src", consumer / "src")
    command = [str(root / "gradlew"), "-p", str(consumer), "run", "optionalBinance", "--no-daemon",
               "--refresh-dependencies", f"-PjavaVersion={args.java_version}",
               f"-PspineVersion={args.spine_version}", f"-PspineRepository={repository}"]
    result = subprocess.run(command, cwd=consumer, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    report = root / "build/reports/publication-consumer" / f"java{args.java_version}.log"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(result.stdout)
    print(result.stdout, end="")
    print("Publication consumer log:", report)
    raise SystemExit(result.returncode)
