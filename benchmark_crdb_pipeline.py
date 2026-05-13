#!/usr/bin/env python
"""CockroachDB insert benchmark using psycopg3 with multi-process workers."""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import queue
import socket
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import mean
from time import perf_counter
from uuid import UUID, uuid4

import psycopg
from psycopg import capabilities

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - depends on local install
    ConnectionPool = None


ROWS_PER_ITERATION = 7
ITERATIONS_DEFAULT = 10_000
WORKERS_DEFAULT = 1
PROCESSES_AUTO = 0
MAX_RETRIES_DEFAULT = 5
PROGRESS_SECONDS_DEFAULT = 5.0
PIPELINE_DEPTH_DEFAULT = 8
TABLE_NAMES = ("table_a", "table_b", "table_c", "table_d")
BENCHMARK_MODES = ("non-pipeline", "pipeline", "both")
PIPELINE_STYLES = ("transaction", "deep")
PROGRESS_FLUSH_EVERY = 25
PAYLOAD_TEXT_LENGTHS = {
    "table_a": (195, 225),
    "table_b": (120, 155),
    "table_b_detail": (80, 105),
    "table_c": (205, 235),
    "table_d": (200, 230),
}

DDL_STATEMENTS = (
    """
    CREATE TABLE table_a (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        benchmark_run_id UUID NOT NULL,
        logical_txn_id UUID NOT NULL,
        tenant_code STRING NOT NULL,
        account_ref STRING NOT NULL,
        customer_segment STRING NOT NULL,
        source_region STRING NOT NULL,
        amount DECIMAL(18, 2) NOT NULL,
        tax_amount DECIMAL(18, 2) NOT NULL,
        fee_amount DECIMAL(18, 2) NOT NULL,
        event_ts TIMESTAMPTZ NOT NULL,
        settled_at TIMESTAMPTZ NOT NULL,
        effective_date DATE NOT NULL,
        priority_code INT8 NOT NULL,
        is_active BOOL NOT NULL,
        is_reversal BOOL NOT NULL,
        score_bucket INT8 NOT NULL,
        payload_text STRING NOT NULL
    ) LOCALITY REGIONAL BY ROW
    """,
    """
    CREATE TABLE table_b (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        benchmark_run_id UUID NOT NULL,
        logical_txn_id UUID NOT NULL,
        order_ref STRING NOT NULL,
        merchant_id STRING NOT NULL,
        channel_code STRING NOT NULL,
        currency_code STRING NOT NULL,
        processing_date DATE NOT NULL,
        submission_date DATE NOT NULL,
        retry_count INT8 NOT NULL,
        batch_number INT8 NOT NULL,
        approval_code STRING NOT NULL,
        authorization_ts TIMESTAMPTZ NOT NULL,
        clearing_ts TIMESTAMPTZ NOT NULL,
        is_manual_review BOOL NOT NULL,
        region_code STRING NOT NULL,
        attributes JSONB NOT NULL,
        payload_text STRING NOT NULL
    ) LOCALITY REGIONAL BY ROW
    """,
    """
    CREATE TABLE table_c (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        benchmark_run_id UUID NOT NULL,
        logical_txn_id UUID NOT NULL,
        source_system STRING NOT NULL,
        workflow_name STRING NOT NULL,
        owner_team STRING NOT NULL,
        risk_score FLOAT8 NOT NULL,
        model_version STRING NOT NULL,
        status STRING NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL,
        status_date DATE NOT NULL,
        review_count INT8 NOT NULL,
        escalation_level INT8 NOT NULL,
        is_final BOOL NOT NULL,
        reserve_amount DECIMAL(18, 2) NOT NULL,
        case_type STRING NOT NULL,
        payload_text STRING NOT NULL
    ) LOCALITY REGIONAL BY ROW
    """,
    """
    CREATE TABLE table_d (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        benchmark_run_id UUID NOT NULL,
        logical_txn_id UUID NOT NULL,
        line_number INT8 NOT NULL,
        sku STRING NOT NULL,
        category_code STRING NOT NULL,
        warehouse_code STRING NOT NULL,
        quantity INT8 NOT NULL,
        unit_price DECIMAL(18, 2) NOT NULL,
        extended_price DECIMAL(18, 2) NOT NULL,
        discount_amount DECIMAL(18, 2) NOT NULL,
        tax_code STRING NOT NULL,
        shipment_date DATE NOT NULL,
        promised_date DATE NOT NULL,
        fulfillment_ts TIMESTAMPTZ NOT NULL,
        is_backordered BOOL NOT NULL,
        line_status STRING NOT NULL,
        payload_text STRING NOT NULL
    ) LOCALITY REGIONAL BY ROW
    """,
)

INSERT_A_SQL = """
    INSERT INTO table_a (
        benchmark_run_id,
        logical_txn_id,
        tenant_code,
        account_ref,
        customer_segment,
        source_region,
        amount,
        tax_amount,
        fee_amount,
        event_ts,
        settled_at,
        effective_date,
        priority_code,
        is_active,
        is_reversal,
        score_bucket,
        payload_text
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_B_SQL = """
    INSERT INTO table_b (
        benchmark_run_id,
        logical_txn_id,
        order_ref,
        merchant_id,
        channel_code,
        currency_code,
        processing_date,
        submission_date,
        retry_count,
        batch_number,
        approval_code,
        authorization_ts,
        clearing_ts,
        is_manual_review,
        region_code,
        attributes,
        payload_text
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::JSONB, %s)
"""

INSERT_C_SQL = """
    INSERT INTO table_c (
        benchmark_run_id,
        logical_txn_id,
        source_system,
        workflow_name,
        owner_team,
        risk_score,
        model_version,
        status,
        created_at,
        updated_at,
        status_date,
        review_count,
        escalation_level,
        is_final,
        reserve_amount,
        case_type,
        payload_text
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_D_SQL = """
    INSERT INTO table_d (
        benchmark_run_id,
        logical_txn_id,
        line_number,
        sku,
        category_code,
        warehouse_code,
        quantity,
        unit_price,
        extended_price,
        discount_amount,
        tax_code,
        shipment_date,
        promised_date,
        fulfillment_ts,
        is_backordered,
        line_status,
        payload_text
    )
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


@dataclass(frozen=True)
class IterationPayload:
    benchmark_run_id: UUID
    logical_txn_id: UUID
    table_a_params: tuple[object, ...]
    table_b_params: tuple[object, ...]
    table_c_params: tuple[object, ...]
    table_d_params: tuple[object, ...]


@dataclass(frozen=True)
class WorkerOutcome:
    worker_id: int
    iterations_completed: int
    retry_count: int
    latencies_ms: list[float]
    gateway_region: str


@dataclass(frozen=True)
class ProcessAssignment:
    process_id: int
    worker_global_ids: list[int]


@dataclass(frozen=True)
class ProcessConfig:
    process_id: int
    worker_global_ids: list[int]
    total_workers: int
    db_uri: str
    benchmark_run_id: UUID
    warmup_benchmark_run_id: UUID | None
    mode: str
    pipeline_style: str
    pipeline_depth: int
    max_retries: int
    region_label: str
    work_mode: str
    iterations: int
    duration_seconds: float | None
    warmup_seconds: float
    seed: int
    application_name_prefix: str
    workload_start_event: object
    measurement_start_event: object
    workload_start_epoch: object
    measurement_start_epoch: object


@dataclass(frozen=True)
class BenchmarkResult:
    name: str
    mode: str
    benchmark_run_id: UUID
    work_mode: str
    iterations_completed: int
    rows_inserted: int
    elapsed_seconds: float
    started_at_utc: str
    finished_at_utc: str
    worker_count: int
    process_count: int
    pool_size_total: int
    retry_count: int
    txns_per_second: float
    rows_per_second: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    pipeline_style: str
    pipeline_depth: int
    application_name_prefix: str
    gateway_connection_counts: dict[str, int]
    warmup_seconds: float
    requested_duration_seconds: float | None
    warmup_benchmark_run_id: UUID | None


class ProgressTracker:
    def __init__(
        self,
        label: str,
        progress_seconds: float,
        total_iterations: int | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        self.label = label
        self.progress_seconds = progress_seconds
        self.total_iterations = total_iterations
        self.duration_seconds = duration_seconds
        self.started = perf_counter()
        self.completed = 0
        self.retries = 0

    def reset(self) -> None:
        self.started = perf_counter()
        self.completed = 0
        self.retries = 0

    def update(self, completed_delta: int, retries_delta: int) -> None:
        self.completed += completed_delta
        self.retries += retries_delta

    def log_snapshot(self, final: bool = False) -> None:
        elapsed = perf_counter() - self.started
        txns_per_second = self.completed / elapsed if elapsed > 0 else 0.0

        if self.total_iterations is not None:
            pct = (self.completed / self.total_iterations) * 100.0 if self.total_iterations else 100.0
            remaining = (
                ((self.total_iterations - self.completed) / txns_per_second)
                if txns_per_second > 0 and self.completed < self.total_iterations
                else 0.0
            )
            prefix = "Final progress" if final else self.label
            log(
                f"{prefix}: {self.completed:,}/{self.total_iterations:,} ({pct:.0f}%) | "
                f"elapsed {format_duration(elapsed)} | eta {format_duration(remaining)} | "
                f"txns/sec {txns_per_second:,.2f} | retries {self.retries:,}"
            )
            return

        remaining = max(0.0, (self.duration_seconds or 0.0) - elapsed)
        pct = (elapsed / self.duration_seconds * 100.0) if self.duration_seconds else 100.0
        prefix = "Final progress" if final else self.label
        log(
            f"{prefix}: {self.completed:,} txns | elapsed {format_duration(elapsed)} | "
            f"remaining {format_duration(remaining)} | time {pct:.0f}% | "
            f"txns/sec {txns_per_second:,.2f} | retries {self.retries:,}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark non-pipeline vs pipeline-mode inserts on CockroachDB."
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=ITERATIONS_DEFAULT,
        help=f"Number of transaction iterations to run in iteration mode. Default: {ITERATIONS_DEFAULT}",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        help="Run in sustained duration mode for this many seconds instead of fixed-iteration mode.",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=0.0,
        help="Additional warmup time to run before measured duration mode begins. Default: 0",
    )
    parser.add_argument(
        "--mode",
        choices=BENCHMARK_MODES,
        help="Benchmark mode to run: non-pipeline, pipeline, or both.",
    )
    parser.add_argument(
        "--pipeline-style",
        choices=PIPELINE_STYLES,
        default="transaction",
        help="Pipeline mode style. 'transaction' pipelines within one transaction; "
        "'deep' batches multiple transactions per pipeline sync. Default: transaction",
    )
    parser.add_argument(
        "--pipeline-depth",
        type=int,
        default=PIPELINE_DEPTH_DEFAULT,
        help=f"How many transactions to batch per pipeline sync in deep pipeline mode. Default: {PIPELINE_DEPTH_DEFAULT}",
    )
    parser.add_argument(
        "--setup-tables",
        action="store_true",
        help="Drop and recreate the benchmark tables before the selected test run.",
    )
    parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Drop and recreate the benchmark tables, then exit without running a benchmark.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=WORKERS_DEFAULT,
        help="Total active workers across all processes. Each worker uses one persistent pooled connection. Default: 1",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=PROCESSES_AUTO,
        help="Number of worker processes. Use 0 for an automatic choice based on CPU count. Default: 0",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=MAX_RETRIES_DEFAULT,
        help="Maximum retry attempts per transaction for retryable CockroachDB errors. Default: 5",
    )
    parser.add_argument(
        "--progress-seconds",
        type=float,
        default=PROGRESS_SECONDS_DEFAULT,
        help=f"How often to print benchmark progress updates in seconds. Default: {PROGRESS_SECONDS_DEFAULT}",
    )
    parser.add_argument(
        "--application-name-prefix",
        default="crdb-bench",
        help="Prefix to use for PostgreSQL application_name on benchmark sessions. Default: crdb-bench",
    )
    parser.add_argument(
        "--region-label",
        help="Optional label to include in logs and JSON output for this regional runner. "
        "If omitted, the script will try to read the current CockroachDB region from the connection.",
    )
    parser.add_argument(
        "--json-out",
        help="Optional path to write benchmark results as JSON.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=17,
        help="Deterministic seed value folded into generated payloads. Default: 17",
    )
    return parser.parse_args()


def normalize_db_uri(raw_uri: str) -> tuple[str, str | None]:
    if raw_uri.startswith("cockroachdb+psycopg://"):
        return "postgresql://" + raw_uri.removeprefix("cockroachdb+psycopg://"), (
            "Normalized DB_URI from cockroachdb+psycopg:// to postgresql:// for psycopg3."
        )
    if raw_uri.startswith("cockroachdb://"):
        return "postgresql://" + raw_uri.removeprefix("cockroachdb://"), (
            "Normalized DB_URI from cockroachdb:// to postgresql:// for psycopg3."
        )
    if raw_uri.startswith("postgres://"):
        return "postgresql://" + raw_uri.removeprefix("postgres://"), (
            "Normalized DB_URI from postgres:// to postgresql:// for psycopg3."
        )
    return raw_uri, None


def get_db_uri() -> tuple[str, str | None]:
    raw_uri = os.environ.get("DB_URI")
    if not raw_uri:
        raise RuntimeError("DB_URI is not set. Export DB_URI before running this script.")
    return normalize_db_uri(raw_uri)


def connect(db_uri: str, *, application_name: str | None = None) -> psycopg.Connection:
    kwargs: dict[str, object] = {"autocommit": True}
    if application_name:
        kwargs["application_name"] = application_name
    return psycopg.connect(db_uri, **kwargs)


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}")


def format_duration(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:d}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes:d}m {secs:02d}s"
    return f"{secs:d}s"


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = int(round((pct / 100.0) * (len(sorted_values) - 1)))
    index = max(0, min(index, len(sorted_values) - 1))
    return sorted_values[index]


def drop_and_create_tables(conn: psycopg.Connection) -> None:
    log("Dropping benchmark tables if they already exist.")
    with conn.cursor() as cur:
        for table_name in reversed(TABLE_NAMES):
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    log("Creating benchmark tables as LOCALITY REGIONAL BY ROW.")
    with conn.cursor() as cur:
        for statement in DDL_STATEMENTS:
            cur.execute(statement)
    log("Benchmark tables are ready.")


def build_filler_text(base: str, target_length: int, pattern: str) -> str:
    if len(base) >= target_length:
        return base[:target_length]
    repeat_count = ((target_length - len(base)) // len(pattern)) + 2
    return base + (pattern * repeat_count)[: target_length - len(base)]


def sized_payload_text(
    table_name: str,
    sequence_no: int,
    variant: int,
    min_length: int,
    max_length: int,
) -> str:
    span = max_length - min_length + 1
    target_length = min_length + ((sequence_no * 17 + variant * 31) % span)
    prefix = (
        f"{table_name}|txn={sequence_no:09d}|variant={variant}|"
        f"segment={sequence_no % 7}|memo="
    )
    pattern = f"{table_name}-{variant}-{(sequence_no % 97):02d}|"
    return build_filler_text(prefix, target_length, pattern)


def build_iteration_payload(sequence_no: int, seed: int, benchmark_run_id: UUID) -> IterationPayload:
    base_ts = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    txn_id = uuid4()
    tenant_code = f"TENANT-{(seed + sequence_no) % 97:02d}"
    account_ref = f"ACCT-{seed:04d}-{sequence_no:09d}"
    customer_segment = ("consumer", "smb", "enterprise")[sequence_no % 3]
    source_region = ("us-east-1", "us-east-2", "us-west-2")[sequence_no % 3]
    order_ref = f"ORDER-{seed:04d}-{sequence_no:09d}"
    merchant_id = f"MERCHANT-{(seed + sequence_no) % 500:04d}"
    channel_code = ("card", "ach", "wallet")[sequence_no % 3]
    currency_code = ("USD", "EUR", "GBP")[sequence_no % 3]
    source_system = f"source-{(sequence_no % 5) + 1}"
    workflow_name = f"workflow-{(sequence_no % 4) + 1}"
    owner_team = ("fraud", "clearing", "settlement", "ops")[sequence_no % 4]
    status = ("pending", "approved", "settled")[sequence_no % 3]
    event_ts = base_ts + timedelta(milliseconds=sequence_no * 11)
    settled_at = event_ts + timedelta(seconds=(sequence_no % 5) + 1)
    processing_day = date(2026, 1, 1) + timedelta(days=sequence_no % 28)
    submission_day = processing_day - timedelta(days=sequence_no % 2)
    created_at = base_ts + timedelta(milliseconds=(sequence_no * 11) + 3)
    updated_at = created_at + timedelta(seconds=(sequence_no % 7) + 1)
    amount = Decimal(100 + (sequence_no % 31)) + (Decimal(sequence_no % 100) / Decimal("100"))
    tax_amount = Decimal((sequence_no % 17) + 1) + (Decimal(sequence_no % 20) / Decimal("100"))
    fee_amount = Decimal((sequence_no % 9) + 1) + (Decimal(sequence_no % 10) / Decimal("100"))
    risk_score = round(((sequence_no * 37) % 1000) / 10.0, 2)
    retry_count = sequence_no % 4
    batch_number = sequence_no % 32
    approval_code = f"APR-{(seed + sequence_no) % 100000:05d}"
    authorization_ts = event_ts + timedelta(milliseconds=4)
    clearing_ts = event_ts + timedelta(milliseconds=9)
    is_active = (sequence_no % 2) == 0
    is_reversal = (sequence_no % 11) == 0
    priority_code = sequence_no % 5
    score_bucket = sequence_no % 10
    model_version = f"v{(sequence_no % 3) + 1}.{(sequence_no % 5) + 1}"
    status_date = processing_day
    review_count = sequence_no % 6
    escalation_level = sequence_no % 4
    is_final = (sequence_no % 3) == 2
    reserve_amount = Decimal((sequence_no % 23) + 2) + (Decimal(sequence_no % 50) / Decimal("100"))
    case_type = ("review", "monitor", "chargeback", "release")[sequence_no % 4]
    is_manual_review = (sequence_no % 8) == 0
    region_code = ("ue1", "ue2", "uw2")[sequence_no % 3]
    table_a_payload = sized_payload_text(
        "table_a",
        sequence_no,
        1,
        PAYLOAD_TEXT_LENGTHS["table_a"][0],
        PAYLOAD_TEXT_LENGTHS["table_a"][1],
    )
    table_b_payload = sized_payload_text(
        "table_b",
        sequence_no,
        2,
        PAYLOAD_TEXT_LENGTHS["table_b"][0],
        PAYLOAD_TEXT_LENGTHS["table_b"][1],
    )
    table_b_detail = sized_payload_text(
        "table_b_detail",
        sequence_no,
        3,
        PAYLOAD_TEXT_LENGTHS["table_b_detail"][0],
        PAYLOAD_TEXT_LENGTHS["table_b_detail"][1],
    )
    table_c_payload = sized_payload_text(
        "table_c",
        sequence_no,
        4,
        PAYLOAD_TEXT_LENGTHS["table_c"][0],
        PAYLOAD_TEXT_LENGTHS["table_c"][1],
    )
    attributes = json.dumps(
        {
            "iteration": sequence_no,
            "seed": seed,
            "channel": "benchmark",
            "segment": f"segment-{sequence_no % 7}",
            "risk_band": ("low", "medium", "high")[sequence_no % 3],
            "detail": table_b_detail,
        },
        separators=(",", ":"),
    )

    d_params: list[object] = []
    for line_number in range(1, 5):
        quantity = ((sequence_no + line_number) % 9) + 1
        unit_price = Decimal(5 * line_number) + (
            Decimal((sequence_no + line_number) % 100) / Decimal("100")
        )
        extended_price = unit_price * quantity
        discount_amount = Decimal((sequence_no + line_number) % 4) + (
            Decimal((sequence_no + line_number) % 20) / Decimal("100")
        )
        category_code = f"CAT-{((sequence_no + line_number) % 12) + 1:02d}"
        warehouse_code = f"WH-{((sequence_no + line_number) % 7) + 1:02d}"
        tax_code = ("TX-A", "TX-B", "TX-C", "TX-D")[(sequence_no + line_number) % 4]
        shipment_date = processing_day + timedelta(days=line_number)
        promised_date = shipment_date + timedelta(days=(sequence_no + line_number) % 3)
        fulfillment_ts = event_ts + timedelta(minutes=line_number)
        is_backordered = ((sequence_no + line_number) % 10) == 0
        line_status = ("open", "picked", "packed", "shipped")[(sequence_no + line_number) % 4]
        table_d_payload = sized_payload_text(
            "table_d",
            sequence_no,
            line_number,
            PAYLOAD_TEXT_LENGTHS["table_d"][0],
            PAYLOAD_TEXT_LENGTHS["table_d"][1],
        )
        d_params.extend(
            (
                benchmark_run_id,
                txn_id,
                line_number,
                f"SKU-{(seed + sequence_no) % 200:03d}-{line_number}",
                category_code,
                warehouse_code,
                quantity,
                unit_price,
                extended_price,
                discount_amount,
                tax_code,
                shipment_date,
                promised_date,
                fulfillment_ts,
                is_backordered,
                line_status,
                table_d_payload,
            )
        )

    return IterationPayload(
        benchmark_run_id=benchmark_run_id,
        logical_txn_id=txn_id,
        table_a_params=(
            benchmark_run_id,
            txn_id,
            tenant_code,
            account_ref,
            customer_segment,
            source_region,
            amount,
            tax_amount,
            fee_amount,
            event_ts,
            settled_at,
            processing_day,
            priority_code,
            is_active,
            is_reversal,
            score_bucket,
            table_a_payload,
        ),
        table_b_params=(
            benchmark_run_id,
            txn_id,
            order_ref,
            merchant_id,
            channel_code,
            currency_code,
            processing_day,
            submission_day,
            retry_count,
            batch_number,
            approval_code,
            authorization_ts,
            clearing_ts,
            is_manual_review,
            region_code,
            attributes,
            table_b_payload,
        ),
        table_c_params=(
            benchmark_run_id,
            txn_id,
            source_system,
            workflow_name,
            owner_team,
            risk_score,
            model_version,
            status,
            created_at,
            updated_at,
            status_date,
            review_count,
            escalation_level,
            is_final,
            reserve_amount,
            case_type,
            table_c_payload,
        ),
        table_d_params=tuple(d_params),
    )


def execute_non_pipeline_iteration(conn: psycopg.Connection, payload: IterationPayload) -> None:
    with conn.transaction():
        conn.execute(INSERT_A_SQL, payload.table_a_params, prepare=True)
        conn.execute(INSERT_B_SQL, payload.table_b_params, prepare=True)
        conn.execute(INSERT_C_SQL, payload.table_c_params, prepare=True)
        conn.execute(INSERT_D_SQL, payload.table_d_params, prepare=True)


def execute_pipeline_transaction_iteration(conn: psycopg.Connection, payload: IterationPayload) -> None:
    with conn.pipeline():
        with conn.transaction():
            conn.execute(INSERT_A_SQL, payload.table_a_params, prepare=True)
            conn.execute(INSERT_B_SQL, payload.table_b_params, prepare=True)
            conn.execute(INSERT_C_SQL, payload.table_c_params, prepare=True)
            conn.execute(INSERT_D_SQL, payload.table_d_params, prepare=True)


def execute_pipeline_deep_batch(conn: psycopg.Connection, payloads: list[IterationPayload]) -> None:
    with conn.pipeline() as pipeline:
        for payload in payloads:
            conn.execute("BEGIN")
            conn.execute(INSERT_A_SQL, payload.table_a_params, prepare=True)
            conn.execute(INSERT_B_SQL, payload.table_b_params, prepare=True)
            conn.execute(INSERT_C_SQL, payload.table_c_params, prepare=True)
            conn.execute(INSERT_D_SQL, payload.table_d_params, prepare=True)
            conn.execute("COMMIT")
        pipeline.sync()


def is_retryable_transaction_error(exc: BaseException) -> bool:
    sqlstate = getattr(exc, "sqlstate", None)
    message = str(exc).lower()
    return sqlstate == "40001" or "restart transaction" in message


def flush_progress(
    progress_queue: mp.Queue,
    process_id: int,
    completed_delta: int,
    retries_delta: int,
) -> None:
    if completed_delta == 0 and retries_delta == 0:
        return
    progress_queue.put(
        {
            "type": "progress",
            "process_id": process_id,
            "completed_delta": completed_delta,
            "retries_delta": retries_delta,
        }
    )


def detect_connection_gateway_region(conn: psycopg.Connection) -> str:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT gateway_region()")
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
    except Exception as exc:
        log(f"Could not detect gateway_region() for a worker connection. Using 'unknown'. Reason: {exc}")
        return "unknown"

    log("gateway_region() returned no value for a worker connection. Using 'unknown'.")
    return "unknown"


def wait_for_workload_start(config: ProcessConfig) -> None:
    config.workload_start_event.wait()


def worker_iteration_loop(
    conn: psycopg.Connection,
    config: ProcessConfig,
    worker_global_id: int,
    progress_queue: mp.Queue,
    gateway_region: str,
) -> WorkerOutcome:
    latencies_ms: list[float] = []
    retries_total = 0
    completed_total = 0
    progress_completed = 0
    progress_retries = 0
    sequence_no = worker_global_id
    while sequence_no < config.iterations:
        payload = build_iteration_payload(sequence_no, config.seed, config.benchmark_run_id)

        if config.mode == "pipeline" and config.pipeline_style == "deep" and config.pipeline_depth > 1:
            batch_payloads = [payload]
            sequence_cursor = sequence_no + config.total_workers
            while sequence_cursor < config.iterations and len(batch_payloads) < config.pipeline_depth:
                batch_payloads.append(
                    build_iteration_payload(sequence_cursor, config.seed, config.benchmark_run_id)
                )
                sequence_cursor += config.total_workers

            batch_started = perf_counter()
            try:
                execute_pipeline_deep_batch(conn, batch_payloads)
            except psycopg.Error as exc:
                raise RuntimeError(
                    "Deep pipeline batch failed. Safe automatic retry is disabled for multi-transaction "
                    "pipeline batches because successful earlier transactions in the batch may already be committed. "
                    f"Database error: {exc}"
                ) from exc

            batch_elapsed_ms = (perf_counter() - batch_started) * 1000.0
            per_txn_ms = batch_elapsed_ms / len(batch_payloads)
            latencies_ms.extend([per_txn_ms] * len(batch_payloads))
            completed_total += len(batch_payloads)
            progress_completed += len(batch_payloads)
            if progress_completed >= PROGRESS_FLUSH_EVERY:
                flush_progress(progress_queue, config.process_id, progress_completed, progress_retries)
                progress_completed = 0
                progress_retries = 0
            sequence_no = sequence_cursor
            continue

        attempt = 0
        while True:
            txn_started = perf_counter()
            try:
                if config.mode == "non-pipeline":
                    execute_non_pipeline_iteration(conn, payload)
                else:
                    execute_pipeline_transaction_iteration(conn, payload)
                latencies_ms.append((perf_counter() - txn_started) * 1000.0)
                completed_total += 1
                progress_completed += 1
                if progress_completed >= PROGRESS_FLUSH_EVERY:
                    flush_progress(progress_queue, config.process_id, progress_completed, progress_retries)
                    progress_completed = 0
                    progress_retries = 0
                break
            except psycopg.Error as exc:
                if is_retryable_transaction_error(exc) and attempt < config.max_retries:
                    attempt += 1
                    retries_total += 1
                    progress_retries += 1
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    continue
                raise RuntimeError(
                    f"Worker {worker_global_id} failed in {config.mode} mode after {attempt} retries."
                ) from exc

        sequence_no += config.total_workers

    flush_progress(progress_queue, config.process_id, progress_completed, progress_retries)
    return WorkerOutcome(
        worker_id=worker_global_id,
        iterations_completed=completed_total,
        retry_count=retries_total,
        latencies_ms=latencies_ms,
        gateway_region=gateway_region,
    )


def worker_duration_loop(
    conn: psycopg.Connection,
    config: ProcessConfig,
    worker_global_id: int,
    progress_queue: mp.Queue,
    gateway_region: str,
) -> WorkerOutcome:
    assert config.duration_seconds is not None
    latencies_ms: list[float] = []
    retries_total = 0
    completed_total = 0
    progress_completed = 0
    progress_retries = 0
    local_counter = 0
    warmup_deadline_epoch = config.workload_start_epoch.value + config.warmup_seconds
    measurement_deadline_epoch = 0.0
    measurement_started = config.warmup_seconds == 0
    if measurement_started:
        measurement_deadline_epoch = config.measurement_start_epoch.value + config.duration_seconds

    while True:
        now = time.time()
        if measurement_started:
            if now >= measurement_deadline_epoch:
                break
            benchmark_run_id = config.benchmark_run_id
        else:
            if now >= warmup_deadline_epoch:
                progress_queue.put(
                    {
                        "type": "warmup_ready",
                        "process_id": config.process_id,
                        "worker_id": worker_global_id,
                    }
                )
                config.measurement_start_event.wait()
                measurement_started = True
                measurement_deadline_epoch = config.measurement_start_epoch.value + config.duration_seconds
                continue
            benchmark_run_id = config.warmup_benchmark_run_id
            if benchmark_run_id is None:
                raise RuntimeError("Warmup benchmark_run_id is required when warmup_seconds is greater than zero.")

        sequence_no = worker_global_id + (local_counter * config.total_workers)
        local_counter += 1
        payload = build_iteration_payload(sequence_no, config.seed, benchmark_run_id)

        if config.mode == "pipeline" and config.pipeline_style == "deep" and config.pipeline_depth > 1:
            batch_payloads = [payload]
            while len(batch_payloads) < config.pipeline_depth:
                sequence_no = worker_global_id + (local_counter * config.total_workers)
                local_counter += 1
                batch_payloads.append(
                    build_iteration_payload(sequence_no, config.seed, benchmark_run_id)
                )

            batch_started = perf_counter()
            try:
                execute_pipeline_deep_batch(conn, batch_payloads)
            except psycopg.Error as exc:
                raise RuntimeError(
                    "Deep pipeline batch failed. Safe automatic retry is disabled for multi-transaction "
                    "pipeline batches because successful earlier transactions in the batch may already be committed. "
                    f"Database error: {exc}"
                ) from exc

            if measurement_started:
                batch_elapsed_ms = (perf_counter() - batch_started) * 1000.0
                per_txn_ms = batch_elapsed_ms / len(batch_payloads)
                latencies_ms.extend([per_txn_ms] * len(batch_payloads))
                completed_total += len(batch_payloads)
                progress_completed += len(batch_payloads)
                if progress_completed >= PROGRESS_FLUSH_EVERY:
                    flush_progress(progress_queue, config.process_id, progress_completed, progress_retries)
                    progress_completed = 0
                    progress_retries = 0
            continue

        attempt = 0
        while True:
            txn_started = perf_counter()
            try:
                if config.mode == "non-pipeline":
                    execute_non_pipeline_iteration(conn, payload)
                else:
                    execute_pipeline_transaction_iteration(conn, payload)
                if measurement_started:
                    latencies_ms.append((perf_counter() - txn_started) * 1000.0)
                    completed_total += 1
                    progress_completed += 1
                    if progress_completed >= PROGRESS_FLUSH_EVERY:
                        flush_progress(progress_queue, config.process_id, progress_completed, progress_retries)
                        progress_completed = 0
                        progress_retries = 0
                break
            except psycopg.Error as exc:
                if is_retryable_transaction_error(exc) and attempt < config.max_retries:
                    attempt += 1
                    if measurement_started:
                        retries_total += 1
                        progress_retries += 1
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    continue
                raise RuntimeError(
                    f"Worker {worker_global_id} failed in {config.mode} mode after {attempt} retries."
                ) from exc

    flush_progress(progress_queue, config.process_id, progress_completed, progress_retries)
    return WorkerOutcome(
        worker_id=worker_global_id,
        iterations_completed=completed_total,
        retry_count=retries_total,
        latencies_ms=latencies_ms,
        gateway_region=gateway_region,
    )


def process_entry(config: ProcessConfig, progress_queue: mp.Queue) -> None:
    if ConnectionPool is None:
        progress_queue.put(
            {
                "type": "error",
                "process_id": config.process_id,
                "error": (
                    "psycopg_pool is not installed. Install dependencies with "
                    "`python -m pip install -r requirements.txt`."
                ),
            }
        )
        return

    if config.mode == "pipeline":
        capabilities.has_pipeline(check=True)

    worker_count = len(config.worker_global_ids)
    app_name = (
        f"{config.application_name_prefix}:{config.region_label}:{config.mode}:"
        f"proc{config.process_id}"
    )

    try:
        with ConnectionPool(
            conninfo=config.db_uri,
            min_size=worker_count,
            max_size=worker_count,
            kwargs={"autocommit": True, "application_name": app_name},
        ) as pool:
            pool.wait()
            progress_queue.put(
                {
                    "type": "process_ready",
                    "process_id": config.process_id,
                    "workers": worker_count,
                    "application_name": app_name,
                }
            )
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix=f"proc{config.process_id}") as executor:
                futures = []
                for worker_global_id in config.worker_global_ids:
                    futures.append(
                        executor.submit(
                            run_worker_in_process,
                            pool,
                            config,
                            worker_global_id,
                            progress_queue,
                        )
                    )

                outcomes = [future.result() for future in futures]

        total_iterations = sum(outcome.iterations_completed for outcome in outcomes)
        total_retries = sum(outcome.retry_count for outcome in outcomes)
        latencies_ms: list[float] = []
        gateway_regions: list[str] = []
        for outcome in outcomes:
            latencies_ms.extend(outcome.latencies_ms)
            gateway_regions.append(outcome.gateway_region)

        progress_queue.put(
            {
                "type": "result",
                "process_id": config.process_id,
                "iterations_completed": total_iterations,
                "retry_count": total_retries,
                "latencies_ms": latencies_ms,
                "gateway_regions": gateway_regions,
            }
        )
    except Exception as exc:
        progress_queue.put(
            {
                "type": "error",
                "process_id": config.process_id,
                "error": str(exc),
            }
        )


def run_worker_in_process(
    pool: ConnectionPool,
    config: ProcessConfig,
    worker_global_id: int,
    progress_queue: mp.Queue,
) -> WorkerOutcome:
    with pool.connection() as conn:
        gateway_region = detect_connection_gateway_region(conn)
        wait_for_workload_start(config)
        if config.work_mode == "iterations":
            return worker_iteration_loop(conn, config, worker_global_id, progress_queue, gateway_region)
        return worker_duration_loop(conn, config, worker_global_id, progress_queue, gateway_region)


def fetch_table_counts(conn: psycopg.Connection, benchmark_run_id: UUID) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for table_name in TABLE_NAMES:
            cur.execute(
                f"SELECT count(*) FROM {table_name} WHERE benchmark_run_id = %s",
                (benchmark_run_id,),
            )
            counts[table_name] = cur.fetchone()[0]
    return counts


def validate_counts(conn: psycopg.Connection, iterations: int, benchmark_run_id: UUID) -> dict[str, int]:
    counts = fetch_table_counts(conn, benchmark_run_id)
    expected = {
        "table_a": iterations,
        "table_b": iterations,
        "table_c": iterations,
        "table_d": iterations * 4,
    }
    if counts != expected:
        raise RuntimeError(
            "Row-count validation failed for benchmark_run_id "
            f"{benchmark_run_id}. Expected {expected}, found {counts}."
        )
    log(f"Row-count validation passed for benchmark_run_id {benchmark_run_id}.")
    return counts


def print_result(result: BenchmarkResult) -> None:
    print(f"{result.name}:")
    print(f"  mode             : {result.mode}")
    print(f"  benchmark_run_id : {result.benchmark_run_id}")
    print(f"  warmup run id    : {result.warmup_benchmark_run_id}")
    print(f"  work mode        : {result.work_mode}")
    print(f"  workers          : {result.worker_count}")
    print(f"  processes        : {result.process_count}")
    print(f"  pool size total  : {result.pool_size_total}")
    print(f"  retries          : {result.retry_count:,}")
    print(f"  iterations       : {result.iterations_completed:,}")
    print(f"  rows inserted    : {result.rows_inserted:,}")
    print(f"  warmup seconds   : {result.warmup_seconds:.1f}")
    print(f"  duration seconds : {result.requested_duration_seconds}")
    print(f"  total runtime    : {result.elapsed_seconds:.6f} seconds")
    print(f"  txns / second    : {result.txns_per_second:,.2f}")
    print(f"  rows / second    : {result.rows_per_second:,.2f}")
    print(f"  avg latency      : {result.avg_latency_ms:.3f} ms")
    print(f"  p95 latency      : {result.p95_latency_ms:.3f} ms")
    print(f"  p99 latency      : {result.p99_latency_ms:.3f} ms")
    print(f"  pipeline style   : {result.pipeline_style}")
    print(f"  pipeline depth   : {result.pipeline_depth}")
    print(f"  app name prefix  : {result.application_name_prefix}")


def print_counts(label: str, counts: dict[str, int]) -> None:
    print(f"{label} row counts for this benchmark run:")
    for table_name in TABLE_NAMES:
        print(f"  {table_name:<14}: {counts[table_name]:,}")


def print_gateway_connection_counts(label: str, gateway_connection_counts: dict[str, int]) -> None:
    print(f"{label} gateway connections:")
    gateway_header = "Gateway"
    count_header = "No of Connections"
    print(f"  {gateway_header:<30}{count_header}")
    print(f"  {'-' * len(gateway_header):<30}{'-' * len(count_header)}")
    for gateway_region, connection_count in sorted(
        gateway_connection_counts.items(),
        key=lambda item: (-item[1], item[0]),
    ):
        print(f"  {gateway_region:<30}{connection_count}")


def maybe_setup_tables(db_uri: str, application_name: str) -> None:
    log("Preparing schema because --setup-tables was provided.")
    with connect(db_uri, application_name=application_name) as conn:
        drop_and_create_tables(conn)


def detect_region_label(db_uri: str) -> str:
    fallback = socket.gethostname()
    try:
        with connect(db_uri, application_name="crdb-bench-region-detect") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT crdb_internal.locality_value('region')")
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
                cur.execute("SHOW LOCALITY")
                locality_row = cur.fetchone()
                if locality_row and locality_row[0]:
                    return locality_row[0]
    except Exception as exc:
        log(f"Could not auto-detect region from CockroachDB; falling back to hostname '{fallback}'. Reason: {exc}")
        return fallback

    log(f"CockroachDB connection did not expose a region; falling back to hostname '{fallback}'.")
    return fallback


def build_result_payload(
    result: BenchmarkResult,
    counts: dict[str, int],
    region_label: str,
    setup_tables: bool,
    iterations_requested: int,
    duration_seconds: float | None,
    warmup_seconds: float,
    seed: int,
) -> dict[str, object]:
    return {
        "region_label": region_label,
        "mode": result.mode,
        "benchmark_name": result.name,
        "benchmark_run_id": str(result.benchmark_run_id),
        "warmup_benchmark_run_id": (
            str(result.warmup_benchmark_run_id) if result.warmup_benchmark_run_id is not None else None
        ),
        "work_mode": result.work_mode,
        "iterations_requested": iterations_requested,
        "duration_seconds": duration_seconds,
        "warmup_seconds": warmup_seconds,
        "iterations_completed": result.iterations_completed,
        "rows_inserted": result.rows_inserted,
        "elapsed_seconds": result.elapsed_seconds,
        "transactions_per_second": result.txns_per_second,
        "rows_per_second": result.rows_per_second,
        "avg_latency_ms": result.avg_latency_ms,
        "p95_latency_ms": result.p95_latency_ms,
        "p99_latency_ms": result.p99_latency_ms,
        "worker_count": result.worker_count,
        "process_count": result.process_count,
        "pool_size_total": result.pool_size_total,
        "retry_count": result.retry_count,
        "pipeline_style": result.pipeline_style,
        "pipeline_depth": result.pipeline_depth,
        "application_name_prefix": result.application_name_prefix,
        "started_at_utc": result.started_at_utc,
        "finished_at_utc": result.finished_at_utc,
        "setup_tables": setup_tables,
        "seed": seed,
        "table_counts_for_run": counts,
        "gateway_connection_counts": result.gateway_connection_counts,
    }


def write_json_results(json_out: str, payload: dict[str, object]) -> None:
    output_path = Path(json_out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    log(f"Wrote JSON results to {output_path}.")


def determine_process_count(requested_processes: int, total_workers: int) -> int:
    if requested_processes > 0:
        return min(requested_processes, total_workers)
    cpu_count = os.cpu_count() or 1
    return min(total_workers, max(1, min(4, cpu_count)))


def assign_workers(total_workers: int, process_count: int) -> list[ProcessAssignment]:
    assignments: list[ProcessAssignment] = []
    worker_ids = list(range(total_workers))
    chunks: list[list[int]] = [[] for _ in range(process_count)]
    for idx, worker_id in enumerate(worker_ids):
        chunks[idx % process_count].append(worker_id)
    for process_id, chunk in enumerate(chunks, start=1):
        if chunk:
            assignments.append(ProcessAssignment(process_id=process_id, worker_global_ids=chunk))
    return assignments


def run_multiprocess_benchmark(
    db_uri: str,
    benchmark_run_id: UUID,
    warmup_benchmark_run_id: UUID | None,
    mode: str,
    pipeline_style: str,
    pipeline_depth: int,
    iterations: int,
    duration_seconds: float | None,
    warmup_seconds: float,
    workers: int,
    processes: int,
    max_retries: int,
    progress_seconds: float,
    seed: int,
    region_label: str,
    application_name_prefix: str,
) -> BenchmarkResult:
    if ConnectionPool is None:
        raise RuntimeError(
            "psycopg_pool is not installed. Install dependencies with "
            "`python -m pip install -r requirements.txt`."
        )

    if mode == "pipeline":
        capabilities.has_pipeline(check=True)

    process_count = determine_process_count(processes, workers)
    assignments = assign_workers(workers, process_count)
    actual_worker_count = sum(len(assignment.worker_global_ids) for assignment in assignments)
    work_mode = "duration" if duration_seconds is not None else "iterations"

    if work_mode == "iterations":
        log(
            f"Starting {mode} benchmark in iteration mode for {iterations:,} iterations using "
            f"{actual_worker_count} workers across {len(assignments)} processes."
        )
        tracker = ProgressTracker(
            label=f"{mode} benchmark progress",
            progress_seconds=progress_seconds,
            total_iterations=iterations,
        )
    else:
        log(
            f"Starting {mode} benchmark in duration mode for {duration_seconds:.1f} measured seconds "
            f"using {actual_worker_count} workers across {len(assignments)} processes."
        )
        tracker = ProgressTracker(
            label=f"{mode} benchmark progress",
            progress_seconds=progress_seconds,
            duration_seconds=duration_seconds,
        )
        if warmup_seconds > 0:
            log(
                f"Warmup is enabled for an additional {warmup_seconds:.1f} seconds before measured "
                "benchmark accounting begins."
            )

    if mode == "pipeline" and pipeline_style == "deep":
        log(
            f"Deep pipeline mode enabled with pipeline depth {pipeline_depth}. "
            "This aggressively batches multiple transactions per pipeline sync."
        )
    else:
        log("Using generated-on-demand payloads; no large in-memory payload list will be prebuilt.")

    ctx = mp.get_context("spawn")
    progress_queue: mp.Queue = ctx.Queue()
    process_objects: list[mp.Process] = []
    workload_start_event = ctx.Event()
    measurement_start_event = ctx.Event()
    workload_start_epoch = ctx.Value("d", 0.0)
    measurement_start_epoch = ctx.Value("d", 0.0)

    started_at_utc = ""
    measured_started = 0.0

    for assignment in assignments:
        config = ProcessConfig(
            process_id=assignment.process_id,
            worker_global_ids=assignment.worker_global_ids,
            total_workers=actual_worker_count,
            db_uri=db_uri,
            benchmark_run_id=benchmark_run_id,
            warmup_benchmark_run_id=warmup_benchmark_run_id,
            mode=mode,
            pipeline_style=pipeline_style,
            pipeline_depth=pipeline_depth,
            max_retries=max_retries,
            region_label=region_label,
            work_mode=work_mode,
            iterations=iterations,
            duration_seconds=duration_seconds,
            warmup_seconds=warmup_seconds,
            seed=seed,
            application_name_prefix=application_name_prefix,
            workload_start_event=workload_start_event,
            measurement_start_event=measurement_start_event,
            workload_start_epoch=workload_start_epoch,
            measurement_start_epoch=measurement_start_epoch,
        )
        process_obj = ctx.Process(
            target=process_entry,
            args=(config, progress_queue),
            name=f"crdb-bench-proc-{assignment.process_id}",
        )
        process_obj.start()
        process_objects.append(process_obj)

    ready_processes = 0
    results_received = 0
    total_retries = 0
    total_iterations_completed = 0
    all_latencies_ms: list[float] = []
    gateway_connection_counts: Counter[str] = Counter()
    warmup_ready_workers = 0
    workload_started = False
    measurement_started = work_mode == "iterations" or warmup_seconds == 0
    next_progress_log = perf_counter() + progress_seconds

    try:
        while results_received < len(assignments):
            now = perf_counter()
            if measurement_started and measured_started and now >= next_progress_log:
                tracker.log_snapshot()
                next_progress_log = now + progress_seconds
            elif work_mode == "duration" and warmup_seconds > 0 and workload_started and not measurement_started and now >= next_progress_log:
                log(
                    f"Warmup progress: workers ready for measured phase "
                    f"{warmup_ready_workers}/{actual_worker_count}."
                )
                next_progress_log = now + progress_seconds

            try:
                message = progress_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            msg_type = message["type"]
            if msg_type == "process_ready":
                ready_processes += 1
                log(
                    f"Process {message['process_id']} ready with {message['workers']} workers "
                    f"(application_name={message['application_name']}). "
                    f"Ready processes: {ready_processes}/{len(assignments)}"
                )
                if ready_processes == len(assignments) and not workload_started:
                    workload_started = True
                    start_epoch = time.time()
                    workload_start_epoch.value = start_epoch
                    if measurement_started:
                        measurement_start_epoch.value = start_epoch
                        measured_started = perf_counter()
                        started_at_utc = datetime.now(UTC).isoformat()
                        tracker.reset()
                        log("All processes ready. Starting measured benchmark workload now.")
                        measurement_start_event.set()
                    else:
                        log("All processes ready. Starting warmup workload now.")
                    workload_start_event.set()
                continue

            if msg_type == "warmup_ready":
                warmup_ready_workers += 1
                if warmup_ready_workers == actual_worker_count and not measurement_started:
                    measurement_started = True
                    measurement_start_epoch.value = time.time()
                    measured_started = perf_counter()
                    started_at_utc = datetime.now(UTC).isoformat()
                    tracker.reset()
                    log("Warmup complete across all workers. Starting measured benchmark accounting now.")
                    measurement_start_event.set()
                continue

            if msg_type == "progress":
                tracker.update(message["completed_delta"], message["retries_delta"])
                continue

            if msg_type == "result":
                results_received += 1
                total_iterations_completed += message["iterations_completed"]
                total_retries += message["retry_count"]
                all_latencies_ms.extend(message["latencies_ms"])
                gateway_connection_counts.update(message.get("gateway_regions", []))
                continue

            if msg_type == "error":
                raise RuntimeError(f"Process {message['process_id']} failed: {message['error']}")

        elapsed = (perf_counter() - measured_started) if measured_started else 0.0
        tracker.log_snapshot(final=True)
    finally:
        for process_obj in process_objects:
            process_obj.join(timeout=2.0)
            if process_obj.is_alive():
                process_obj.terminate()
                process_obj.join(timeout=2.0)

    all_latencies_ms.sort()
    avg_latency_ms = mean(all_latencies_ms) if all_latencies_ms else 0.0
    p95_latency_ms = percentile(all_latencies_ms, 95.0)
    p99_latency_ms = percentile(all_latencies_ms, 99.0)

    if work_mode == "duration":
        throughput_denominator = duration_seconds or elapsed
    else:
        throughput_denominator = elapsed

    txns_per_second = total_iterations_completed / throughput_denominator if throughput_denominator > 0 else 0.0
    rows_inserted = total_iterations_completed * ROWS_PER_ITERATION
    rows_per_second = rows_inserted / throughput_denominator if throughput_denominator > 0 else 0.0

    log(f"{mode} benchmark finished in {elapsed:.6f} seconds.")
    return BenchmarkResult(
        name="Non-Pipeline Transaction" if mode == "non-pipeline" else "Pipeline Transaction",
        mode=mode,
        benchmark_run_id=benchmark_run_id,
        work_mode=work_mode,
        iterations_completed=total_iterations_completed,
        rows_inserted=rows_inserted,
        elapsed_seconds=elapsed,
        started_at_utc=started_at_utc,
        finished_at_utc=datetime.now(UTC).isoformat(),
        worker_count=actual_worker_count,
        process_count=len(assignments),
        pool_size_total=actual_worker_count,
        retry_count=total_retries,
        txns_per_second=txns_per_second,
        rows_per_second=rows_per_second,
        avg_latency_ms=avg_latency_ms,
        p95_latency_ms=p95_latency_ms,
        p99_latency_ms=p99_latency_ms,
        pipeline_style=pipeline_style,
        pipeline_depth=pipeline_depth if mode == "pipeline" else 1,
        application_name_prefix=application_name_prefix,
        gateway_connection_counts=dict(gateway_connection_counts),
        warmup_seconds=warmup_seconds,
        requested_duration_seconds=duration_seconds,
        warmup_benchmark_run_id=warmup_benchmark_run_id,
    )


def execute_benchmark_mode(
    db_uri: str,
    mode: str,
    pipeline_style: str,
    pipeline_depth: int,
    iterations: int,
    duration_seconds: float | None,
    warmup_seconds: float,
    seed: int,
    setup_tables: bool,
    workers: int,
    processes: int,
    max_retries: int,
    progress_seconds: float,
    region_label: str,
    application_name_prefix: str,
) -> tuple[BenchmarkResult, dict[str, int]]:
    benchmark_run_id = uuid4()
    warmup_benchmark_run_id = uuid4() if duration_seconds is not None and warmup_seconds > 0 else None

    if setup_tables:
        maybe_setup_tables(
            db_uri,
            application_name=f"{application_name_prefix}:{region_label}:setup",
        )

    result = run_multiprocess_benchmark(
        db_uri=db_uri,
        benchmark_run_id=benchmark_run_id,
        warmup_benchmark_run_id=warmup_benchmark_run_id,
        mode=mode,
        pipeline_style=pipeline_style,
        pipeline_depth=pipeline_depth,
        iterations=iterations,
        duration_seconds=duration_seconds,
        warmup_seconds=warmup_seconds,
        workers=workers,
        processes=processes,
        max_retries=max_retries,
        progress_seconds=progress_seconds,
        seed=seed,
        region_label=region_label,
        application_name_prefix=application_name_prefix,
    )

    log(f"Opening a validation connection for benchmark_run_id {benchmark_run_id}.")
    with connect(
        db_uri,
        application_name=f"{application_name_prefix}:{region_label}:validate",
    ) as conn:
        counts = validate_counts(conn, result.iterations_completed, benchmark_run_id)

    print_result(result)
    print_counts(result.name, counts)
    print_gateway_connection_counts(result.name, result.gateway_connection_counts)
    print()
    return result, counts


def print_comparison(non_pipeline_result: BenchmarkResult, pipeline_result: BenchmarkResult) -> None:
    delta = non_pipeline_result.txns_per_second - pipeline_result.txns_per_second
    print("Comparison:")
    print(f"  non-pipeline txns/sec : {non_pipeline_result.txns_per_second:,.2f}")
    print(f"  pipeline txns/sec     : {pipeline_result.txns_per_second:,.2f}")
    print(f"  txns/sec delta        : {delta:,.2f}")


def main() -> int:
    args = parse_args()

    if args.iterations <= 0:
        print("--iterations must be greater than zero.", file=sys.stderr)
        return 2
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        print("--duration-seconds must be greater than zero.", file=sys.stderr)
        return 2
    if args.warmup_seconds < 0:
        print("--warmup-seconds cannot be negative.", file=sys.stderr)
        return 2
    if args.workers <= 0:
        print("--workers must be greater than zero.", file=sys.stderr)
        return 2
    if args.processes < 0:
        print("--processes cannot be negative.", file=sys.stderr)
        return 2
    if args.max_retries < 0:
        print("--max-retries cannot be negative.", file=sys.stderr)
        return 2
    if args.progress_seconds <= 0:
        print("--progress-seconds must be greater than zero.", file=sys.stderr)
        return 2
    if args.pipeline_depth <= 0:
        print("--pipeline-depth must be greater than zero.", file=sys.stderr)
        return 2
    if args.mode == "non-pipeline" and args.pipeline_style != "transaction":
        print("--pipeline-style only applies to pipeline mode.", file=sys.stderr)
        return 2
    if args.warmup_seconds > 0 and args.duration_seconds is None:
        print("--warmup-seconds requires --duration-seconds.", file=sys.stderr)
        return 2
    if not args.setup_only and not args.mode:
        print("Either --setup-only or --mode must be provided.", file=sys.stderr)
        return 2

    try:
        db_uri, normalization_message = get_db_uri()
        if normalization_message:
            log(normalization_message)
            print()

        region_label = args.region_label or detect_region_label(db_uri)
        log(f"Runner region label: {region_label}")
        log(f"Requested worker count: {args.workers}")
        log(
            f"Requested process count: "
            f"{args.processes if args.processes > 0 else 'auto'}"
        )

        if args.setup_only:
            log("Running in setup-only mode.")
            with connect(
                db_uri,
                application_name=f"{args.application_name_prefix}:{region_label}:setup-only",
            ) as conn:
                drop_and_create_tables(conn)
                print("Dropped and recreated benchmark tables:")
                for table_name in TABLE_NAMES:
                    print(f"  {table_name}")
            return 0

        if args.mode == "both" and not args.setup_tables:
            log(
                "Running both modes without --setup-tables. "
                "Each mode will validate only its own benchmark_run_id, but table contents will accumulate."
            )

        if args.mode == "both":
            non_pipeline_result, non_pipeline_counts = execute_benchmark_mode(
                db_uri=db_uri,
                mode="non-pipeline",
                pipeline_style="transaction",
                pipeline_depth=1,
                iterations=args.iterations,
                duration_seconds=args.duration_seconds,
                warmup_seconds=args.warmup_seconds,
                seed=args.seed,
                setup_tables=args.setup_tables,
                workers=args.workers,
                processes=args.processes,
                max_retries=args.max_retries,
                progress_seconds=args.progress_seconds,
                region_label=region_label,
                application_name_prefix=args.application_name_prefix,
            )
            pipeline_result, pipeline_counts = execute_benchmark_mode(
                db_uri=db_uri,
                mode="pipeline",
                pipeline_style=args.pipeline_style,
                pipeline_depth=args.pipeline_depth,
                iterations=args.iterations,
                duration_seconds=args.duration_seconds,
                warmup_seconds=args.warmup_seconds,
                seed=args.seed,
                setup_tables=args.setup_tables,
                workers=args.workers,
                processes=args.processes,
                max_retries=args.max_retries,
                progress_seconds=args.progress_seconds,
                region_label=region_label,
                application_name_prefix=args.application_name_prefix,
            )
            print_comparison(non_pipeline_result, pipeline_result)
            if args.json_out:
                payload = {
                    "region_label": region_label,
                    "mode": "both",
                    "iterations_requested": args.iterations,
                    "duration_seconds": args.duration_seconds,
                    "warmup_seconds": args.warmup_seconds,
                    "seed": args.seed,
                    "setup_tables": args.setup_tables,
                    "workers_requested": args.workers,
                    "processes_requested": args.processes,
                    "results": [
                        build_result_payload(
                            result=non_pipeline_result,
                            counts=non_pipeline_counts,
                            region_label=region_label,
                            setup_tables=args.setup_tables,
                            iterations_requested=args.iterations,
                            duration_seconds=args.duration_seconds,
                            warmup_seconds=args.warmup_seconds,
                            seed=args.seed,
                        ),
                        build_result_payload(
                            result=pipeline_result,
                            counts=pipeline_counts,
                            region_label=region_label,
                            setup_tables=args.setup_tables,
                            iterations_requested=args.iterations,
                            duration_seconds=args.duration_seconds,
                            warmup_seconds=args.warmup_seconds,
                            seed=args.seed,
                        ),
                    ],
                }
                write_json_results(args.json_out, payload)
            return 0

        result, counts = execute_benchmark_mode(
            db_uri=db_uri,
            mode=args.mode,
            pipeline_style=args.pipeline_style if args.mode == "pipeline" else "transaction",
            pipeline_depth=args.pipeline_depth if args.mode == "pipeline" else 1,
            iterations=args.iterations,
            duration_seconds=args.duration_seconds,
            warmup_seconds=args.warmup_seconds,
            seed=args.seed,
            setup_tables=args.setup_tables,
            workers=args.workers,
            processes=args.processes,
            max_retries=args.max_retries,
            progress_seconds=args.progress_seconds,
            region_label=region_label,
            application_name_prefix=args.application_name_prefix,
        )
        if args.json_out:
            payload = build_result_payload(
                result=result,
                counts=counts,
                region_label=region_label,
                setup_tables=args.setup_tables,
                iterations_requested=args.iterations,
                duration_seconds=args.duration_seconds,
                warmup_seconds=args.warmup_seconds,
                seed=args.seed,
            )
            write_json_results(args.json_out, payload)
        return 0
    except Exception as exc:  # pragma: no cover - keeps CLI failures readable
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
