#!/usr/bin/env python3
import argparse
import gzip
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import csv
from typing import Dict, Optional, Iterable, TextIO, List

# ----------------------------
# Regex patterns (tune to your log format)
# ----------------------------

# Example typical lines (CDH/CDP-like):
# 2024-11-14 10:12:34,567 INFO  ... Compiling command(queryId=hive_2024_11_14_10_12_34_123): user=svc_hue; ...
# 2024-11-14 10:12:34,998 INFO  ... Completed compiling command(queryId=hive_2024_11_14_10_12_34_123); compileTime=14523ms
#
# Adjust these if your format differs.

TIMESTAMP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")

COMPILE_START_RE = re.compile(
    r"Compiling command\(queryId=(?P<queryId>[^)]+)\).*?"
    r"user=(?P<user>[^;]+);.*?"
    r"query=(?P<query>.*)$",
    re.IGNORECASE,
)

COMPILE_END_RE = re.compile(
    r"Completed compiling command\(queryId=(?P<queryId>[^)]+)\).*?"
    r"compileTime=(?P<compileTime>\d+)ms",
    re.IGNORECASE,
)

UNION_ALL_RE = re.compile(r"\bUNION\s+ALL\b", re.IGNORECASE)


# ----------------------------
# Data structures
# ----------------------------

@dataclass
class InFlightQuery:
    query_id: str
    user: str
    query: str
    start_ts: Optional[datetime]  # may be None if no timestamp parsed


@dataclass
class QueryRecord:
    query_id: str
    user: str
    compile_time_ms: int
    union_count: int
    slow_compile: bool
    excessive_unions: bool
    start_ts: Optional[str]  # ISO string
    end_ts: Optional[str]    # ISO string
    source_log: str          # which file
    query: str               # (can be truncated)


# ----------------------------
# Utility functions
# ----------------------------

def open_maybe_gzip(path: Path) -> TextIO:
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", errors="ignore")
    return open(path, "r", errors="ignore")


def parse_timestamp(line: str) -> Optional[datetime]:
    m = TIMESTAMP_RE.match(line)
    if not m:
        return None
    ts_str = m.group(1)
    try:
        # Example: 2024-11-14 10:12:34,567
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
    except ValueError:
        return None


# ----------------------------
# Core parser
# ----------------------------

class HiveServer2LogAnalyzer:
    def __init__(
        self,
        compile_threshold_ms: int = 10000,
        union_threshold: int = 3,
        max_query_len: int = 20000,
    ):
        self.compile_threshold_ms = compile_threshold_ms
        self.union_threshold = union_threshold
        self.max_query_len = max_query_len

        # aggregates
        self.per_user_stats = defaultdict(lambda: {
            "total_queries": 0,
            "slow_queries": 0,
            "excessive_union_queries": 0,
            "total_compile_ms": 0,
            "max_compile_ms": 0,
        })
        # hourly stats: key = "YYYY-MM-DD HH"
        self.per_hour_stats = defaultdict(lambda: {
            "total_queries": 0,
            "slow_queries": 0,
            "avg_compile_ms": 0.0,
            "sum_compile_ms": 0,
        })

        self.parse_errors = 0

    def process_logs(self, log_paths: List[Path], csv_writer: csv.DictWriter) -> None:
        inflight: Dict[str, InFlightQuery] = {}

        for path in log_paths:
            with open_maybe_gzip(path) as f:
                for line in f:
                    line_ts = parse_timestamp(line)

                    # START: Compiling command
                    start_match = COMPILE_START_RE.search(line)
                    if start_match:
                        try:
                            query_id = start_match.group("queryId").strip()
                            user = start_match.group("user").strip()
                            query = start_match.group("query").strip()

                            # Avoid unbounded memory for extremely long queries
                            if len(query) > self.max_query_len:
                                query = query[: self.max_query_len] + " --[TRUNCATED]"

                            inflight[query_id] = InFlightQuery(
                                query_id=query_id,
                                user=user,
                                query=query,
                                start_ts=line_ts,
                            )
                        except Exception:
                            self.parse_errors += 1
                        continue

                    # END: Completed compiling command
                    end_match = COMPILE_END_RE.search(line)
                    if end_match:
                        try:
                            query_id = end_match.group("queryId").strip()
                            compile_time_ms = int(end_match.group("compileTime"))

                            infl = inflight.pop(query_id, None)
                            if infl is None:
                                # We saw an end without a start (rotated log or pattern mismatch)
                                # You can choose to log this somewhere.
                                continue

                            union_count = len(UNION_ALL_RE.findall(infl.query))
                            slow = compile_time_ms > self.compile_threshold_ms
                            excessive_unions = union_count > self.union_threshold

                            rec = QueryRecord(
                                query_id=query_id,
                                user=infl.user,
                                compile_time_ms=compile_time_ms,
                                union_count=union_count,
                                slow_compile=slow,
                                excessive_unions=excessive_unions,
                                start_ts=infl.start_ts.isoformat() if infl.start_ts else None,
                                end_ts=line_ts.isoformat() if line_ts else None,
                                source_log=str(path),
                                query=infl.query,
                            )

                            self._emit_record(rec, csv_writer)

                        except Exception:
                            self.parse_errors += 1
                        continue

        # Note: any remaining inflight entries represent queries that never completed
        # (e.g., in-progress at time of log cut). You can optionally export them.

    def _emit_record(self, rec: QueryRecord, csv_writer: csv.DictWriter) -> None:
        # Write to CSV
        csv_writer.writerow(asdict(rec))

        # Update per-user stats
        u = self.per_user_stats[rec.user]
        u["total_queries"] += 1
        u["total_compile_ms"] += rec.compile_time_ms
        if rec.compile_time_ms > u["max_compile_ms"]:
            u["max_compile_ms"] = rec.compile_time_ms
        if rec.slow_compile:
            u["slow_queries"] += 1
        if rec.excessive_unions:
            u["excessive_union_queries"] += 1

        # Update per-hour stats (based on start time prefer, else end time)
        ts = rec.start_ts or rec.end_ts
        if ts:
            dt = datetime.fromisoformat(ts)
            hour_key = dt.strftime("%Y-%m-%d %H")
            h = self.per_hour_stats[hour_key]
            h["total_queries"] += 1
            h["sum_compile_ms"] += rec.compile_time_ms

    def build_summary(self) -> dict:
        # finalize per_hour avg
        for hour_key, h in self.per_hour_stats.items():
            if h["total_queries"] > 0:
                h["avg_compile_ms"] = h["sum_compile_ms"] / h["total_queries"]

        # finalize per_user avg
        for user, u in self.per_user_stats.items():
            if u["total_queries"] > 0:
                u["avg_compile_ms"] = u["total_compile_ms"] / u["total_queries"]
            else:
                u["avg_compile_ms"] = 0.0

        summary = {
            "compile_threshold_ms": self.compile_threshold_ms,
            "union_threshold": self.union_threshold,
            "per_user": self.per_user_stats,
            "per_hour": self.per_hour_stats,
            "parse_errors": self.parse_errors,
        }
        return summary


# ----------------------------
# CLI
# ----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Robust HiveServer2 log analyzer for compile times & UNION ALL heavy queries"
    )
    p.add_argument(
        "--logs",
        nargs="+",
        required=True,
        help="Paths to hiveserver2 log files (plain or .gz)",
    )
    p.add_argument(
        "--compile-threshold-ms",
        type=int,
        default=10000,
        help="Compile time (ms) above which a query is considered slow",
    )
    p.add_argument(
        "--union-threshold",
        type=int,
        default=3,
        help="UNION ALL count above which a query is considered excessive",
    )
    p.add_argument(
        "--max-query-len",
        type=int,
        default=20000,
        help="Max query length to store; longer queries are truncated",
    )
    p.add_argument(
        "--out-queries",
        default="hs2_queries.csv",
        help="Output CSV file with per-query records",
    )
    p.add_argument(
        "--out-summary",
        default="hs2_summary.json",
        help="Output JSON file with aggregates (per user & per hour)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    log_paths = [Path(p) for p in args.logs]

    analyzer = HiveServer2LogAnalyzer(
        compile_threshold_ms=args.compile_threshold_ms,
        union_threshold=args.union_threshold,
        max_query_len=args.max_query_len,
    )

    # Stream output CSV
    fieldnames = [
        "query_id",
        "user",
        "compile_time_ms",
        "union_count",
        "slow_compile",
        "excessive_unions",
        "start_ts",
        "end_ts",
        "source_log",
        "query",
    ]

    with open(args.out_queries, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        analyzer.process_logs(log_paths, writer)

    summary = analyzer.build_summary()
    with open(args.out_summary, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"Wrote per-query CSV to {args.out_queries}")
    print(f"Wrote summary JSON to {args.out_summary}")
    print(f"Parse errors: {analyzer.parse_errors}")


if __name__ == "__main__":
    main()
