from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
from collections import Counter
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import aiohttp
import requests

from forglory.parsing import parse_hero, parse_kill_beasts, profile_url_matches


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("forglory.collector")

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class FetchFailure:
    pid: int
    stage: str
    error_type: str
    attempts: int
    http_status: int | None = None
    message: str = ""


@dataclass
class FetchResult:
    pid: int
    data: dict | None
    failure: FetchFailure | None = None
    achievement_failure: FetchFailure | None = None
    diagnostic_html: str | None = None
    diagnostic_url: str | None = None


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


_ENV = load_env_file(ROOT / ".env")


def env_get(key: str, default: str = "") -> str:
    return os.getenv(key, _ENV.get(key, default))


def load_cookie_config() -> tuple[dict[str, str], str]:
    cookies_json = env_get("COOKIES_JSON", "").strip()
    if cookies_json:
        raw = json.loads(cookies_json)
    else:
        cookie_path = Path(env_get("COOKIES_FILE", str(ROOT / "static" / "cfg.json")))
        if not cookie_path.exists():
            raise RuntimeError(
                "Cookies are not configured. Set COOKIES_JSON or COOKIES_FILE."
            )
        raw = json.loads(cookie_path.read_text(encoding="utf-8"))

    cookies = {
        str(item.get("name")): str(item.get("value"))
        for item in raw
        if item.get("name") and item.get("value")
    }
    domain = env_get("WK_DOMAIN", "").strip().lstrip(".")
    if not domain:
        for item in raw:
            if item.get("name") == "wekings_session" and item.get("domain"):
                domain = str(item["domain"]).lstrip(".")
                break
    return cookies, (f"https://{domain}/" if domain else "https://playwekings.mobi/")


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; SM-G973F) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0.0.0 Mobile Safari/537.36"
    )
}

SNAPSHOT_RE = re.compile(
    r"^heroes_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json(?:\.gz)?$"
)


def load_json_any(path: Path) -> dict:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def latest_local_snapshot() -> Path | None:
    candidates: list[tuple[str, Path]] = []
    for path in DATA_DIR.iterdir():
        match = SNAPSHOT_RE.match(path.name)
        if match:
            candidates.append((match.group(1), path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def load_ids_from_db(db_path: Path) -> tuple[list[int], set[int], int | None]:
    """Return all known ids, ids in latest snapshot, and highest probed id."""
    if not db_path.exists():
        return [], set(), None
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "players" in tables and "observations" in tables:
            known = [int(row[0]) for row in conn.execute("SELECT pid FROM players ORDER BY pid")]
            latest = conn.execute("SELECT snapshot_id FROM snapshots ORDER BY ts DESC LIMIT 1").fetchone()
            baseline: set[int] = set()
            if latest:
                baseline = {
                    int(row[0])
                    for row in conn.execute(
                        "SELECT pid FROM observations WHERE snapshot_id=?", (latest[0],)
                    )
                }
            highest_row = conn.execute(
                "SELECT value FROM scan_state WHERE key='highest_probed_id'"
            ).fetchone()
            highest = int(highest_row[0]) if highest_row else None
            return known, baseline, highest

        if "heroes" in tables:
            known = [int(row[0]) for row in conn.execute("SELECT DISTINCT pid FROM heroes ORDER BY pid")]
            latest = conn.execute("SELECT id FROM snapshots ORDER BY ts DESC LIMIT 1").fetchone()
            baseline = {
                int(row[0])
                for row in conn.execute(
                    "SELECT pid FROM heroes WHERE snapshot_id=?", (latest[0],)
                )
            } if latest else set()
            return known, baseline, max(known, default=None)
        return [], set(), None
    finally:
        conn.close()


def load_known_names_from_db(db_path: Path) -> dict[int, str]:
    """Return the last successfully stored name for every known player."""
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if {"players", "observations", "text_values"}.issubset(tables):
            rows = conn.execute(
                """
                SELECT p.pid,n.value
                FROM players p
                JOIN observations o
                  ON o.snapshot_id=p.last_snapshot_id AND o.pid=p.pid
                JOIN text_values n ON n.text_id=o.name_id
                WHERE n.value IS NOT NULL AND trim(n.value)<>''
                """
            ).fetchall()
            return {int(pid): str(name) for pid, name in rows}
        if "heroes" in tables:
            rows = conn.execute(
                """
                SELECT h.pid,h.name
                FROM heroes h
                JOIN (
                    SELECT pid,MAX(snapshot_id) AS snapshot_id
                    FROM heroes
                    GROUP BY pid
                ) latest ON latest.pid=h.pid AND latest.snapshot_id=h.snapshot_id
                WHERE h.name IS NOT NULL AND trim(h.name)<>''
                """
            ).fetchall()
            return {int(pid): str(name) for pid, name in rows}
        return {}
    finally:
        conn.close()


def failure_summary(failures: list[FetchFailure], limit: int = 5) -> list[dict[str, object]]:
    counts = Counter(
        (item.stage, item.error_type, item.http_status, item.message or "")
        for item in failures
    )
    return [
        {
            "stage": stage,
            "error_type": error_type,
            "http_status": status,
            "message": message,
            "count": count,
        }
        for (stage, error_type, status, message), count in counts.most_common(limit)
    ]


def format_failure_summary(failures: list[FetchFailure], limit: int = 3) -> str:
    items = failure_summary(failures, limit)
    if not items:
        return "none"
    return "; ".join(
        f"{item['stage']}:{item['error_type']}:{item['message'] or item['http_status'] or '-'}={item['count']}"
        for item in items
    )


def load_collection_scope(db_path: Path) -> tuple[list[int], set[int], int]:
    known, baseline, highest = load_ids_from_db(db_path)
    if not known:
        latest = latest_local_snapshot()
        if latest:
            data = load_json_any(latest)
            known = sorted(int(pid) for pid in data)
            baseline = set(known)
    if not known:
        start = int(env_get("WK_START_PLAYER_ID", "1"))
        return [], set(), start - 1
    return sorted(set(known)), baseline or set(known), highest or max(known)


def check_site_ready(
    url: str,
    cookies: dict[str, str],
    max_attempts: int = 5,
    delay_seconds: int = 60,
) -> bool:
    for attempt in range(1, max_attempts + 1):
        try:
            LOG.info("Checking site, attempt %s/%s", attempt, max_attempts)
            response = requests.get(url, cookies=cookies, headers=HEADERS, timeout=15)
            response.raise_for_status()
            if "hero/profile" in response.text or "hero/detail" in response.text:
                return True
            LOG.error("Site returned unexpected content")
        except requests.RequestException as exc:
            LOG.error("Site check failed: %s", exc)
        if attempt < max_attempts:
            time.sleep(delay_seconds)
    return False


def _failure(
    pid: int,
    stage: str,
    error_type: str,
    attempts: int,
    status: int | None = None,
    message: str = "",
) -> FetchFailure:
    return FetchFailure(
        pid=pid,
        stage=stage,
        error_type=error_type,
        attempts=attempts,
        http_status=status,
        message=message[:300],
    )


async def _request_text(
    session: aiohttp.ClientSession,
    url: str,
    pid: int,
    stage: str,
    retries: int,
) -> tuple[str | None, str | None, FetchFailure | None]:
    error: FetchFailure | None = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, allow_redirects=True) as response:
                text = await response.text(errors="replace")
                status = int(response.status)
                final_url = str(response.url)
                if status == 200:
                    return text, final_url, None
                if status in {404, 410}:
                    return None, final_url, _failure(pid, stage, "not_found", attempt, status)
                if status in {401, 403}:
                    return None, final_url, _failure(pid, stage, "auth_error", attempt, status)
                if status == 429 or 500 <= status <= 599:
                    if attempt < retries:
                        await asyncio.sleep((2 ** (attempt - 1)) + random.random())
                        continue
                    return None, final_url, _failure(pid, stage, "temporary_http", attempt, status)
                return None, final_url, _failure(pid, stage, "http_error", attempt, status)
        except asyncio.TimeoutError:
            error = _failure(pid, stage, "timeout", attempt)
        except aiohttp.ClientError as exc:
            error = _failure(pid, stage, "network_error", attempt, message=str(exc))
        except Exception as exc:
            error = _failure(pid, stage, "unexpected_error", attempt, message=repr(exc))
        if attempt < retries:
            await asyncio.sleep((2 ** (attempt - 1)) + random.random())
    return None, None, error


async def _limited_request(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    url: str,
    pid: int,
    stage: str,
    retries: int,
) -> tuple[str | None, str | None, FetchFailure | None]:
    async with semaphore:
        return await _request_text(session, url, pid, stage, retries)


async def fetch_hero(
    session: aiohttp.ClientSession,
    hero_id: int,
    semaphore: asyncio.Semaphore,
    domain: str,
    retries: int,
    fallback_name: str | None = None,
    achievement_retries: int = 1,
) -> FetchResult:
    profile_url = f"{domain}hero/detail?player={hero_id}"
    achievement_url = f"{domain}achievements?player={hero_id}"

    profile_task = asyncio.create_task(
        _limited_request(
            semaphore, session, profile_url, hero_id, "profile", retries
        )
    )
    achievement_task = asyncio.create_task(
        _limited_request(
            semaphore,
            session,
            achievement_url,
            hero_id,
            "achievements",
            max(1, achievement_retries),
        )
    )

    profile_text, final_url, failure = await profile_task
    if failure:
        achievement_task.cancel()
        with suppress(asyncio.CancelledError):
            await achievement_task
        return FetchResult(hero_id, None, failure)
    if final_url and not profile_url_matches(final_url, hero_id):
        achievement_task.cancel()
        with suppress(asyncio.CancelledError):
            await achievement_task
        return FetchResult(
            hero_id,
            None,
            _failure(
                hero_id,
                "profile",
                "unexpected_redirect",
                retries,
                message=final_url,
            ),
            diagnostic_html=profile_text,
            diagnostic_url=final_url,
        )
    if not profile_text or "Что-то пошло не так" in profile_text:
        achievement_task.cancel()
        with suppress(asyncio.CancelledError):
            await achievement_task
        return FetchResult(
            hero_id,
            None,
            _failure(hero_id, "profile", "not_found", 1),
            diagnostic_html=profile_text,
            diagnostic_url=final_url,
        )
    try:
        hero_data = parse_hero(
            profile_text,
            hero_id,
            fallback_name=fallback_name,
        )
    except Exception as exc:
        achievement_task.cancel()
        with suppress(asyncio.CancelledError):
            await achievement_task
        return FetchResult(
            hero_id,
            None,
            _failure(
                hero_id,
                "profile",
                "parse_error",
                retries,
                message=str(exc),
            ),
            diagnostic_html=profile_text,
            diagnostic_url=final_url,
        )

    achievement_text, _achievement_final_url, achievement_failure = (
        await achievement_task
    )
    if achievement_text:
        try:
            kills = parse_kill_beasts(achievement_text, hero_id)
            if kills is not None:
                hero_data["Убито зверей"] = kills
            else:
                achievement_failure = _failure(
                    hero_id, "achievements", "achievement_not_found", 1
                )
        except Exception as exc:
            achievement_failure = _failure(
                hero_id, "achievements", "parse_error", 1, message=str(exc)
            )

    return FetchResult(
        hero_id,
        hero_data,
        achievement_failure=achievement_failure,
    )


async def collect(
    ids: Iterable[int],
    cookies: dict[str, str],
    domain: str,
    concurrency: int,
    retries: int,
    known_names: dict[int, str] | None = None,
    achievement_retries: int = 1,
    systemic_failure_sample_size: int = 200,
) -> tuple[dict[int, dict], list[FetchFailure], list[FetchFailure]]:
    ids = list(dict.fromkeys(ids))
    known_names = known_names or {}
    results: dict[int, dict] = {}
    failures: list[FetchFailure] = []
    achievement_failures: list[FetchFailure] = []
    semaphore = asyncio.Semaphore(max(1, concurrency))
    timeout = aiohttp.ClientTimeout(total=30, connect=8, sock_read=18)
    connector = aiohttp.TCPConnector(
        limit=max(concurrency + 10, 30),
        limit_per_host=max(concurrency, 20),
        ttl_dns_cache=600,
        keepalive_timeout=30,
    )
    diagnostics_dir = DATA_DIR / "failed_html"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    for old_file in diagnostics_dir.glob("*.html"):
        old_file.unlink(missing_ok=True)
    diagnostic_count = 0

    async with aiohttp.ClientSession(
        cookies=cookies,
        headers=HEADERS,
        timeout=timeout,
        connector=connector,
    ) as session:
        tasks = [
            asyncio.create_task(
                fetch_hero(
                    session,
                    pid,
                    semaphore,
                    domain,
                    retries,
                    fallback_name=known_names.get(pid),
                    achievement_retries=achievement_retries,
                )
            )
            for pid in ids
        ]
        completed = 0
        aborted = False
        for future in asyncio.as_completed(tasks):
            item = await future
            completed += 1
            if item.data is not None:
                results[item.pid] = item.data
            elif item.failure:
                failures.append(item.failure)
                if item.diagnostic_html and diagnostic_count < 5:
                    diagnostic_count += 1
                    diagnostic_path = diagnostics_dir / (
                        f"{diagnostic_count:02d}_pid_{item.pid}_"
                        f"{item.failure.error_type}.html"
                    )
                    diagnostic_path.write_text(
                        item.diagnostic_html, encoding="utf-8", errors="replace"
                    )
            if item.achievement_failure:
                achievement_failures.append(item.achievement_failure)

            if completed % 1000 == 0 or completed == len(tasks):
                LOG.info(
                    "Progress %s/%s: profiles=%s, failed=%s, "
                    "achievement warnings=%s; reasons=%s",
                    completed,
                    len(tasks),
                    len(results),
                    len(failures),
                    len(achievement_failures),
                    format_failure_summary(failures),
                )

            if (
                systemic_failure_sample_size > 0
                and completed >= systemic_failure_sample_size
                and not results
            ):
                LOG.error(
                    "Systemic collection failure after %s responses; "
                    "aborting remaining requests. Reasons: %s",
                    completed,
                    format_failure_summary(failures, 5),
                )
                aborted = True
                for task in tasks:
                    if not task.done():
                        task.cancel()
                break

        if aborted:
            await asyncio.gather(*tasks, return_exceptions=True)

    return results, failures, achievement_failures


def save_snapshot(
    results: dict[int, dict],
    failures: list[FetchFailure],
    achievement_failures: list[FetchFailure],
    baseline_ids: set[int],
    known_ids: list[int],
    probe_start: int,
    probe_end: int,
) -> tuple[Path, Path]:
    captured = datetime.now(timezone.utc) + timedelta(hours=3)
    timestamp = captured.strftime("%Y-%m-%d_%H-%M-%S")
    snapshot_path = DATA_DIR / f"heroes_{timestamp}.json.gz"
    metadata_path = DATA_DIR / f"heroes_{timestamp}.meta.json"

    sorted_data = {str(pid): results[pid] for pid in sorted(results)}
    with gzip.open(snapshot_path, "wt", encoding="utf-8", compresslevel=6) as handle:
        json.dump(sorted_data, handle, ensure_ascii=False, separators=(",", ":"))

    baseline_success = len(baseline_ids.intersection(results))
    metadata = {
        "schema": 1,
        "snapshot": snapshot_path.name,
        "captured_at": captured.isoformat(),
        "known_ids_count": len(known_ids),
        "baseline_ids_count": len(baseline_ids),
        "baseline_success_count": baseline_success,
        "successful_profiles": len(results),
        "probe_start": probe_start,
        "probe_end": probe_end,
        "highest_probed_id": probe_end,
        "failures": [asdict(item) for item in failures],
        "achievement_failures": [asdict(item) for item in achievement_failures],
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return snapshot_path, metadata_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect all known and newly probed players")
    parser.add_argument("--db-path", default=env_get("DB_PATH", "data/db/ratings.sqlite"))
    parser.add_argument("--concurrency", type=int, default=int(env_get("COLLECT_CONCURRENCY", "40")))
    parser.add_argument("--retries", type=int, default=int(env_get("COLLECT_RETRIES", "2")))
    parser.add_argument(
        "--achievement-retries",
        type=int,
        default=int(env_get("ACHIEVEMENT_RETRIES", "1")),
    )
    parser.add_argument(
        "--systemic-failure-sample-size",
        type=int,
        default=int(env_get("SYSTEMIC_FAILURE_SAMPLE_SIZE", "200")),
    )
    parser.add_argument("--probe-count", type=int, default=int(env_get("NEW_PLAYER_PROBE_COUNT", "300")))
    parser.add_argument(
        "--min-success-ratio",
        type=float,
        default=float(env_get("MIN_BASELINE_SUCCESS_RATIO", "0.995")),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cookies, domain = load_cookie_config()
    if not check_site_ready(
        domain,
        cookies,
        max_attempts=int(env_get("SITE_READY_ATTEMPTS", "5")),
        delay_seconds=int(env_get("SITE_READY_DELAY_SECONDS", "60")),
    ):
        return 1

    db_path = (ROOT / args.db_path).resolve() if not Path(args.db_path).is_absolute() else Path(args.db_path)
    known_ids, baseline_ids, highest_probed = load_collection_scope(db_path)
    known_names = load_known_names_from_db(db_path)
    probe_start = highest_probed + 1
    probe_end = highest_probed + max(0, args.probe_count)
    ids = [*known_ids, *range(probe_start, probe_end + 1)]
    LOG.info(
        "Collection scope: %s known ids, baseline=%s, cached names=%s, "
        "probing %s..%s, concurrency=%s",
        len(known_ids),
        len(baseline_ids),
        len(known_names),
        probe_start,
        probe_end,
        args.concurrency,
    )

    results, failures, achievement_failures = asyncio.run(
        collect(
            ids,
            cookies,
            domain,
            args.concurrency,
            args.retries,
            known_names=known_names,
            achievement_retries=args.achievement_retries,
            systemic_failure_sample_size=args.systemic_failure_sample_size,
        )
    )

    baseline_success = len(baseline_ids.intersection(results))
    ratio = baseline_success / len(baseline_ids) if baseline_ids else 1.0
    if baseline_ids and ratio < args.min_success_ratio:
        LOG.error(
            "Snapshot rejected: baseline success %.2f%% is below %.2f%% (%s/%s)",
            ratio * 100,
            args.min_success_ratio * 100,
            baseline_success,
            len(baseline_ids),
        )
        failure_report = DATA_DIR / "last_failed_collection.json"
        failure_report.write_text(
            json.dumps(
                {
                    "baseline_success_ratio": ratio,
                    "baseline_success": baseline_success,
                    "baseline_total": len(baseline_ids),
                    "failure_summary": failure_summary(failures, 20),
                    "diagnostic_html_directory": str(DATA_DIR / "failed_html"),
                    "failures": [asdict(item) for item in failures],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return 2

    snapshot, metadata = save_snapshot(
        results,
        failures,
        achievement_failures,
        baseline_ids,
        known_ids,
        probe_start,
        probe_end,
    )
    LOG.info("Saved snapshot: %s", snapshot)
    LOG.info("Saved metadata: %s", metadata)
    LOG.info(
        "Collection complete: %s profiles, %s profile failures, %s achievement warnings",
        len(results),
        len(failures),
        len(achievement_failures),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
