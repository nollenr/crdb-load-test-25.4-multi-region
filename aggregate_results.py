#!/usr/bin/env python
"""Aggregate regional benchmark JSON results."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate benchmark JSON files from multiple regional runners."
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Zero or more local JSON result files produced by benchmark_crdb_pipeline.py",
    )
    parser.add_argument(
        "--remote-host",
        action="append",
        default=[],
        help="Remote host or private IP to fetch a JSON result file from over passwordless SSH/SCP. Repeat as needed.",
    )
    parser.add_argument(
        "--remote-file",
        help="Filename to fetch from each remote host, relative to --remote-dir. Required if --remote-host is used.",
    )
    parser.add_argument(
        "--remote-dir",
        default="/home/ec2-user/Pipeline-Test",
        help="Remote directory containing the JSON result file. Default: /home/ec2-user/Pipeline-Test",
    )
    parser.add_argument(
        "--ssh-user",
        default="ec2-user",
        help="SSH username for remote hosts. Default: ec2-user",
    )
    return parser.parse_args()


def load_results(path_str: str) -> list[dict[str, object]]:
    path = Path(path_str)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("mode") == "both":
        return payload["results"]
    return [payload]


def format_number(value: float) -> str:
    return f"{value:,.2f}"


def fetch_remote_results(
    remote_hosts: list[str],
    remote_dir: str,
    remote_file: str,
    ssh_user: str,
) -> list[str]:
    if not shutil.which("scp"):
        raise RuntimeError("The `scp` command was not found on this system.")

    temp_dir = Path(tempfile.mkdtemp(prefix="pipeline-test-results-"))
    fetched_files: list[str] = []

    for host in remote_hosts:
        remote_path = f"{ssh_user}@{host}:{remote_dir.rstrip('/')}/{remote_file}"
        local_path = temp_dir / f"{host.replace(':', '_')}-{Path(remote_file).name}"
        subprocess.run(
            ["scp", "-q", remote_path, str(local_path)],
            check=True,
        )
        fetched_files.append(str(local_path))

    return fetched_files


def main() -> int:
    args = parse_args()
    all_results: list[dict[str, object]] = []
    file_names = list(args.files)

    if args.remote_host and not args.remote_file:
        print("ERROR: --remote-file is required when using --remote-host.", file=sys.stderr)
        return 2

    if args.remote_host:
        try:
            file_names.extend(
                fetch_remote_results(
                    remote_hosts=args.remote_host,
                    remote_dir=args.remote_dir,
                    remote_file=args.remote_file,
                    ssh_user=args.ssh_user,
                )
            )
        except Exception as exc:
            print(f"ERROR fetching remote files: {exc}", file=sys.stderr)
            return 1

    if not file_names:
        print("ERROR: Provide at least one local file or one --remote-host source.", file=sys.stderr)
        return 2

    for file_name in file_names:
        try:
            all_results.extend(load_results(file_name))
        except Exception as exc:
            print(f"ERROR reading {file_name}: {exc}", file=sys.stderr)
            return 1

    if not all_results:
        print("No benchmark results were found.", file=sys.stderr)
        return 1

    mode = all_results[0]["mode"]
    if any(result["mode"] != mode for result in all_results):
        print("ERROR: All aggregated files must be from the same benchmark mode.", file=sys.stderr)
        return 1

    total_iterations = sum(int(result["iterations_completed"]) for result in all_results)
    total_rows = sum(int(result["rows_inserted"]) for result in all_results)
    total_rows_per_second = sum(float(result["rows_per_second"]) for result in all_results)
    total_txns_per_second = sum(
        float(result.get("transactions_per_second", int(result["iterations_completed"]) / float(result["elapsed_seconds"])))
        for result in all_results
    )
    total_workers = sum(int(result.get("worker_count", 0)) for result in all_results)
    total_processes = sum(int(result.get("process_count", 0)) for result in all_results)
    total_retries = sum(int(result.get("retry_count", 0)) for result in all_results)

    print("Aggregate Benchmark Summary:")
    print(f"  mode                : {mode}")
    print(f"  regional result files: {len(all_results)}")
    print(f"  total workers       : {total_workers:,}")
    print(f"  total processes     : {total_processes:,}")
    print(f"  total retries       : {total_retries:,}")
    print(f"  total iterations    : {total_iterations:,}")
    print(f"  total rows          : {total_rows:,}")
    print(f"  aggregate txns/sec  : {format_number(total_txns_per_second)}")
    print(f"  aggregate rows/sec  : {format_number(total_rows_per_second)}")
    print()
    print("Per-region breakdown:")
    for result in all_results:
        region_label = result["region_label"]
        elapsed_seconds = float(result["elapsed_seconds"])
        iterations_completed = int(result["iterations_completed"])
        txns_per_second = float(
            result.get("transactions_per_second", iterations_completed / elapsed_seconds if elapsed_seconds else 0.0)
        )
        rows_per_second = float(result["rows_per_second"])
        worker_count = int(result.get("worker_count", 0))
        process_count = int(result.get("process_count", 0))
        retry_count = int(result.get("retry_count", 0))
        avg_latency_ms = float(result.get("avg_latency_ms", 0.0))
        p95_latency_ms = float(result.get("p95_latency_ms", 0.0))
        p99_latency_ms = float(result.get("p99_latency_ms", 0.0))
        print(
            f"  {region_label}: "
            f"txns/sec {format_number(txns_per_second)}, "
            f"rows/sec {format_number(rows_per_second)}, "
            f"workers {worker_count}, "
            f"processes {process_count}, "
            f"retries {retry_count}, "
            f"avg/p95/p99 latency {format_number(avg_latency_ms)}/{format_number(p95_latency_ms)}/{format_number(p99_latency_ms)} ms, "
            f"run_id {result['benchmark_run_id']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
