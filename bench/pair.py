#!/usr/bin/env python3
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import subprocess


def main():
    parser = argparse.ArgumentParser(
        description="Alternate two benchmark builds and keep each result."
    )

    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    parser.add_argument("--before-source", required=True)
    parser.add_argument("--after-source", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=8)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--flags", default="")

    args = parser.parse_args()

    if args.rounds < 1:
        parser.error("rounds must be positive")

    if args.cpu is not None:
        if not hasattr(os, "sched_setaffinity"):
            parser.error("CPU affinity needs sched_setaffinity on this host")

        os.sched_setaffinity(0, {args.cpu})

    executables = {
        name: path.resolve() for name, path in [("before", args.before), ("after", args.after)]
    }
    settings = {
        name: os.environ.get(name)
        for name in ["BACKEND", "WORK", "CLIENTS", "CALLS", "REPEATS", "RUNTIME", "CAPACITY"]
    }

    metadata = {
        "source": {"before": args.before_source, "after": args.after_source},
        "binary": {
            name: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for name, path in executables.items()
        },
        "settings": settings,
        "flags": args.flags or os.environ.get("RUSTFLAGS", ""),
        "lockfile_sha256": hashlib.sha256(
            Path(__file__).with_name("Cargo.lock").read_bytes()
        ).hexdigest(),
        "cpu": args.cpu,
        "host": platform.platform(),
        "rustc": subprocess.check_output(["rustc", "--version", "--verbose"], text=True).strip(),
        "probes": [],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)

    metadata_path = args.output.with_suffix(".json")

    with args.output.open("w", newline="") as output:
        writer = None
        fields = None

        for round_index in range(args.rounds):
            order = ["before", "after"] if round_index % 2 == 0 else ["after", "before"]

            for name in order:
                result = subprocess.run(
                    [str(executables[name])],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=300,
                )
                rows = list(csv.DictReader(io.StringIO(result.stdout)))

                if not rows:
                    raise RuntimeError(f"{name} produced no measurements")

                if writer is None:
                    fields = list(rows[0])
                    writer = csv.DictWriter(output, fieldnames=["round", "build", *fields])

                    writer.writeheader()

                for row in rows:
                    if list(row) != fields:
                        raise RuntimeError("benchmark columns differ between the builds")

                    writer.writerow({"round": round_index, "build": name, **row})

                output.flush()
                metadata["probes"].append(
                    {"round": round_index, "build": name, "stderr": result.stderr}
                )
                metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
