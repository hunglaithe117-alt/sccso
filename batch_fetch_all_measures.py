#!/usr/bin/env python3
"""
High‑throughput SonarQube metrics exporter (pipeline style).

What it does
- Discovers projects (or reads from file/args)
- Fetches the full metric key list
- Concurrently exports measures for each project in efficient chunks
- Streams results to CSV (row per project) and optional JSONL (one JSON per line)
- Retries with backoff, supports resume, and progress logging

Quick start examples

1) Crawl all projects and export everything
   python3 batch_fetch_all_measures.py \
       --sonar_host http://localhost:9000 \
       --token YOUR_TOKEN \
       --all_projects \
       --output_dir results \
       --max_workers 8

2) From a list of project keys
   python3 batch_fetch_all_measures.py \
       --sonar_host http://localhost:9000 \
       --token YOUR_TOKEN \
       --project_keys_file project_keys.txt \
       --output_dir results

3) From explicit project keys
   python3 batch_fetch_all_measures.py \
       --sonar_host http://localhost:9000 \
       --token YOUR_TOKEN \
       --project_keys key1 key2 key3 \
       --output_dir results
"""

from __future__ import annotations

import argparse
import csv
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_PAGE_SIZE = 500
DEFAULT_TIMEOUT = 30


# # Metrics to ignore entirely (also ignore any metric that starts with 'new')
# SKIP_METRICS = {
#     "analysis_from_sonarqube_9_4",
#     "comment_lines_data",
#     "branch_coverage",
#     "conditions_to_cover",
#     "duplications_data",
#     "executable_lines_data",
#     "generated_lines",
#     "generated_ncloc",
#     "prioritized_rule_issues",
#     "ncloc_data",
#     "unanalyzed_c",
#     "unanalyzed_cpp",
#     "projects",
#     "public_api",
#     "public_documented_api_density",
#     "public_undocumented_api",
#     "pull_request_fixed_issues",
#     "skipped_tests",
#     "software_quality_maintainability_remediation_effort",
#     "uncovered_conditions",
#     "test_execution_time",
#     "test_errors",
#     "test_failures",
#     "test_success_density",
#     "tests",
# }

ALL_METRIC_KEYS = [
    "bugs",
    "reliability_issues",
    "reliability_rating",
    "software_quality_reliability_remediation_effort",
    "software_quality_reliability_issues",
    "reliability_remediation_effort",
    "software_quality_reliability_rating",
    "vulnerabilities",
    "security_issues",
    "security_rating",
    "security_hotspots",
    "software_quality_security_rating",
    "software_quality_security_issues",
    "software_quality_security_remediation_effort",
    "security_remediation_effort",
    "security_review_rating",
    "security_hotspots_to_review_status",
    "code_smells",
    "sqale_index",
    "sqale_debt_ratio",
    "sqale_rating",
    "maintainability_issues",
    "development_cost",
    "effort_to_reach_maintainability_rating_a",
    "software_quality_maintainability_debt_ratio",
    "software_quality_maintainability_remediation_effort",
    "software_quality_maintainability_rating",
    "effort_to_reach_software_quality_maintainability_rating_a",
    "coverage",
    "line_coverage",
    "lines_to_cover",
    "uncovered_lines",
    "duplicated_lines_density",
    "duplicated_lines",
    "duplicated_blocks",
    "duplicated_files",
    "cognitive_complexity",
    "complexity",
    "ncloc",
    "lines",
    "files",
    "classes",
    "functions",
    "statements",
    "ncloc_language_distribution",
    "comment_lines_density",
    "comment_lines",
    "alert_status",
    "quality_gate_details",
    "software_quality_blocker_issues",
    "critical_violations",
    "violations",
    "software_quality_high_issues",
    "info_violations",
    "software_quality_low_issues",
    "software_quality_maintainability_issues",
    "software_quality_info_issues",
    "minor_violations",
    "major_violations",
    "software_quality_medium_issues",
    "open_issues",
    "last_commit_date",
]


def build_session(
    *,
    token: Optional[str] = None,
    auth: Optional[str] = None,
    retries: int = 3,
    backoff_factor: float = 0.5,
) -> requests.Session:
    """Create a requests Session with retry/backoff and auth.

    - If token is provided, uses Bearer token auth.
    - Else if auth="user:pass" or "token:", uses Basic auth.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    elif auth and ":" in auth:
        username, password = auth.split(":", 1)
        session.auth = (username, password)
    else:
        raise ValueError(
            "Provide --token or --auth formatted as 'username:password' or 'token:'."
        )

    session.headers["Accept"] = "application/json"
    return session


def request_json(
    session: requests.Session, url: str, params: Optional[Dict] = None
) -> Dict:
    """GET JSON with timeout and raise for non-2xx."""
    resp = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    # For 429/5xx, Retry adapter handles backoff; still raise if after retries.
    resp.raise_for_status()
    return resp.json()


def fetch_all_projects(
    session: requests.Session, base_url: str, qualifier: str = "TRK"
) -> List[str]:
    """Fetch all project keys from SonarQube via /api/projects/search (paginated)."""
    projects: List[str] = []
    page = 1
    endpoint = urljoin(base_url, "/api/projects/search")
    while True:
        params = {"p": page, "ps": DEFAULT_PAGE_SIZE, "qualifiers": qualifier}
        data = request_json(session, endpoint, params)
        comps = data.get("components") or []
        if not comps:
            break
        for comp in comps:
            key = comp.get("key")
            if key:
                projects.append(key)
        paging = data.get("paging") or {}
        total = paging.get("total") or 0
        if len(projects) >= total or len(comps) < DEFAULT_PAGE_SIZE:
            break
        page += 1
    return projects


def fetch_metrics(session: requests.Session, base_url: str) -> List[str]:
    """Fetch all metric keys from SonarQube (paginated)."""
    metrics: List[str] = []
    page = 1
    endpoint = urljoin(base_url, "/api/metrics/search")
    while True:
        data = request_json(session, endpoint, {"p": page, "ps": 500})
        batch = data.get("metrics") or []
        if not batch:
            break
        metrics.extend(m.get("key") for m in batch if m.get("key"))
        if len(batch) < 500:
            break
        page += 1
    return metrics


def chunk_list(lst: List[str], size: int) -> Iterable[List[str]]:
    """Split list into chunks of given size."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def fetch_measures_chunk(
    session: requests.Session, base_url: str, project_key: str, metrics: List[str]
) -> List[Dict]:
    """Fetch measures for a given group of metric keys."""
    url = urljoin(base_url, "/api/measures/component")
    params = {"component": project_key, "metricKeys": ",".join(metrics)}
    data = request_json(session, url, params)
    component = data.get("component") or {}
    return component.get("measures") or []


def parse_component_key(component_key: str) -> Tuple[str, str]:
    """
    Parse component key in format {repo}_{commit_sha}
    e.g., '19wu_19wu_011983fcf1ed6a9b6890a8e646b36704c28ad391'
    Returns (repo, commit_sha)
    """
    parts = component_key.split("_")
    if len(parts) >= 2:
        # Find the last part that looks like a commit SHA (40 chars hex)
        for i in range(len(parts) - 1, -1, -1):
            if len(parts[i]) == 40 and all(
                c in "0123456789abcdef" for c in parts[i].lower()
            ):
                repo = "_".join(parts[:i])
                commit = parts[i]
                return repo, commit
    # Fallback: treat everything before last underscore as repo
    if len(parts) >= 2:
        return "_".join(parts[:-1]), parts[-1]
    return component_key, ""


def fetch_all_measures_for_project(
    session: requests.Session,
    base_url: str,
    project_key: str,
    metrics: List[str],
    chunk_size: int,
    per_chunk_delay: float = 0.05,
) -> List[Dict]:
    """Fetch all measures for a single project (chunked to reduce URL size)."""
    all_measures: List[Dict] = []
    for chunk in chunk_list(metrics, max(1, chunk_size)):
        measures = fetch_measures_chunk(session, base_url, project_key, chunk)
        if measures:
            all_measures.extend(measures)
        if per_chunk_delay:
            time.sleep(per_chunk_delay)  # polite delay between chunk calls
    return all_measures


def export_to_json(all_projects_data: List[Dict], output_file: Path) -> None:
    """Export all projects data to JSON."""
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_projects_data, f, indent=2, ensure_ascii=False)


def export_to_csv(all_projects_data: List[Dict], output_file: Path) -> None:
    """
    Legacy batch writer (kept for compatibility when not streaming).
    Exports columns: repo, commit, metric1, metric2, ...
    """
    if not all_projects_data:
        return

    # Collect all unique metric keys across all projects
    all_metric_keys = set()
    for project_data in all_projects_data:
        for measure in project_data.get("measures", []) or []:
            m = measure.get("metric")
            if m:
                all_metric_keys.add(m)

    all_metric_keys = sorted(all_metric_keys)

    # Write CSV
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["repo", "commit"] + all_metric_keys
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for project_data in all_projects_data:
            component_key = project_data.get("component", "")
            repo, commit = parse_component_key(component_key)

            # Build row with metric values
            row = {"repo": repo, "commit": commit}
            for measure in project_data.get("measures", []) or []:
                metric_key = measure.get("metric")
                if not metric_key:
                    continue
                value = measure.get("value", "")
                row[metric_key] = value

            writer.writerow(row)


def is_project_pending(measures: List[Dict]) -> bool:
    """
    Check if a project is pending (not yet analyzed).
    A project is considered pending if it has no measures or all measures are empty.
    """
    if not measures:
        return True
    # Check if all measures have no value
    for measure in measures:
        value = measure.get("value")
        if value is None and measure.get("periods"):
            value = measure["periods"][0].get("value")
        if value is not None and str(value).strip():
            return False
    return True


def measures_to_row(
    component_key: str, metrics: List[str], measures: List[Dict]
) -> Dict[str, str]:
    """Convert measures list to a CSV row {metric -> value} plus repo/commit."""
    repo, commit = parse_component_key(component_key)
    row: Dict[str, str] = {"repo": repo, "commit": commit}
    # Pre-fill to keep consistent columns for streaming
    for m in metrics:
        row[m] = ""
    for measure in measures or []:
        mk = measure.get("metric")
        if not mk:
            continue
        value = measure.get("value")
        # Some metrics only have period values
        if value is None and measure.get("periods"):
            value = measure["periods"][0].get("value")
        row[mk] = "" if value is None else str(value)
    return row


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "High‑throughput SonarQube exporter: crawl or read project keys, "
            "fetch all metric keys, and stream results to CSV/JSONL."
        )
    )
    parser.add_argument(
        "--sonar_host",
        required=True,
        help="SonarQube host URL, e.g. http://localhost:9000",
    )
    # Auth options: prefer token, keep --auth for backward compatibility
    parser.add_argument("--token", help="SonarQube user token (preferred).")
    parser.add_argument("--auth", help="Alternative auth as 'user:pass' or 'token:'")

    # Project sources
    parser.add_argument("--project_keys", nargs="*", help="List of project keys")
    parser.add_argument(
        "--project_keys_file", help="File containing project keys, one per line or CSV"
    )
    parser.add_argument(
        "--all_projects",
        action="store_true",
        help="Fetch all projects from SonarQube automatically",
    )
    parser.add_argument(
        "--qualifier",
        default="TRK",
        help="Component qualifier when crawling (default: TRK)",
    )

    # Output / pipeline controls
    parser.add_argument(
        "--output_dir",
        default="results",
        help="Directory to save outputs (default: results)",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=50,
        help="Number of metrics per API call (default 50)",
    )
    parser.add_argument(
        "--max_workers", type=int, default=8, help="Max concurrent projects (default 8)"
    )
    parser.add_argument(
        "--per_chunk_delay",
        type=float,
        default=0.05,
        help="Delay between metric chunk calls (default 0.05s)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from progress file; skip processed projects",
    )
    parser.add_argument(
        "--jsonl",
        action="store_true",
        help="Also write JSONL lines per project for auditing",
    )

    # Retry tuning
    parser.add_argument(
        "--retries", type=int, default=3, help="HTTP retries (default 3)"
    )
    parser.add_argument(
        "--backoff", type=float, default=0.5, help="Retry backoff factor (default 0.5)"
    )

    args = parser.parse_args()

    base_url = args.sonar_host.rstrip("/")
    session = build_session(
        token=args.token,
        auth=args.auth,
        retries=args.retries,
        backoff_factor=args.backoff,
    )

    # Resolve project keys
    project_keys: List[str] = []
    if args.all_projects:
        print("🔹 Discovering projects via API…")
        project_keys = fetch_all_projects(session, base_url, qualifier=args.qualifier)
        print(f"  → Found {len(project_keys)} projects")
    if args.project_keys:
        project_keys.extend(args.project_keys)
    if args.project_keys_file:
        file_path = Path(args.project_keys_file)
        if not file_path.exists():
            raise FileNotFoundError(f"Project key file does not exist: {file_path}")
        with file_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                s = line.strip()
                if s and not s.startswith("#"):
                    # Support CSV row: take first column
                    if "," in s:
                        s = s.split(",", 1)[0].strip()
                    project_keys.append(s)

    # De-duplicate while preserving order
    seen: Set[str] = set()
    project_keys = [k for k in project_keys if not (k in seen or seen.add(k))]

    if not project_keys:
        print(
            "❌ No project keys provided. Use --project_keys, --project_keys_file, or --all_projects"
        )
        return

    # Prepare output paths and progress
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "all_projects_measures.csv"
    jsonl_path = output_dir / "all_projects_measures.jsonl"
    progress_dir = output_dir / "progress"
    progress_dir.mkdir(exist_ok=True)
    done_file = progress_dir / "processed.txt"

    processed: Set[str] = set()
    if args.resume and done_file.exists():
        processed = {
            line.strip()
            for line in done_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
        before = len(project_keys)
        project_keys = [k for k in project_keys if k not in processed]
        print(
            f"🔁 Resume enabled: skipping {before - len(project_keys)} already processed projects"
        )

    # Use a fixed list of metric keys (user-specified) instead of fetching all metrics
    # This restricts the exporter to only query the metrics needed.
    print(f"  → Using {len(ALL_METRIC_KEYS)} metrics")

    # Initialize CSV with header if new
    csv_lock = threading.Lock()
    jsonl_lock = threading.Lock()
    progress_lock = threading.Lock()

    header = ["repo", "commit"] + ALL_METRIC_KEYS
    new_csv = not csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as csv_handle:
        writer = csv.DictWriter(csv_handle, fieldnames=header)
        if new_csv:
            writer.writeheader()

    total_projects = len(project_keys)
    print(
        f"🚀 Exporting {total_projects} projects with up to {args.max_workers} workers…"
    )

    def process_project(key: str) -> Tuple[str, Dict[str, str], Dict, bool]:
        measures = fetch_all_measures_for_project(
            session,
            base_url,
            key,
            ALL_METRIC_KEYS,
            args.chunk_size,
            args.per_chunk_delay,
        )
        is_pending = is_project_pending(measures)
        row = measures_to_row(key, ALL_METRIC_KEYS, measures)
        project_json = {"component": key, "measures": measures}
        return key, row, project_json, is_pending

    success = 0
    failures = 0
    skipped_pending = 0

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as pool:
        futures = {pool.submit(process_project, key): key for key in project_keys}
        for idx, fut in enumerate(as_completed(futures), start=1):
            key = futures[fut]
            try:
                k, row, project_json, is_pending = fut.result()

                # Skip pending projects (no measures data)
                if is_pending:
                    skipped_pending += 1
                    continue

                # Stream CSV row
                with csv_lock:
                    with csv_path.open("a", newline="", encoding="utf-8") as csv_handle:
                        writer = csv.DictWriter(csv_handle, fieldnames=header)
                        writer.writerow(row)
                # Stream JSONL
                if args.jsonl:
                    with jsonl_lock:
                        with jsonl_path.open("a", encoding="utf-8") as jh:
                            jh.write(
                                json.dumps(project_json, ensure_ascii=False) + "\n"
                            )
                # Update progress
                with progress_lock:
                    with done_file.open("a", encoding="utf-8") as ph:
                        ph.write(k + "\n")
                success += 1
                if idx % 25 == 0 or success <= 5:
                    print(f"  ✅ {success}/{total_projects} done (last: {k})")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"  ❌ Failed {key}: {exc}")

    print("\n🎉 Export complete!")
    print(
        f"  → Success: {success}, Failed: {failures}, Pending (skipped): {skipped_pending}"
    )
    print(f"  → CSV: {csv_path}")
    if args.jsonl:
        print(f"  → JSONL: {jsonl_path}")


if __name__ == "__main__":
    main()
