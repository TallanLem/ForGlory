from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

import get_data as legacy
from forglory.schema import parse_int


LOG = logging.getLogger("forglory.api_first")

API_NUMERIC_FIELDS: tuple[tuple[str, str], ...] = (
    ("level", "Уровень"),
    ("glory", "Слава"),
    ("wins", "Побед"),
    ("losses", "Поражений"),
    ("dragon_wins", "Побед над Драконом"),
    ("serpent_wins", "Побед над Змеем"),
    ("strength", "Сила"),
    ("defense", "Защита"),
    ("agility", "Ловкость"),
    ("mastery", "Мастерство"),
    ("vitality", "Живучесть"),
    ("silver_looted", "Награбил (серебро)"),
    ("silver_lost", "Потерял (серебро)"),
    ("crystals_looted", "Награбил (кристаллы)"),
    ("crystals_lost", "Потерял (кристаллы)"),
    ("beasts_killed", "Убито зверей"),
)


class ApiCollectionError(RuntimeError):
    """The bulk heroes endpoint did not return a safe usable response."""


@dataclass(frozen=True)
class ApiCollection:
    heroes: dict[int, dict]
    endpoint: str
    attempts_used: int
    meta: dict[str, Any]


def _required_int(value: Any, field: str, row_number: int) -> int:
    number = parse_int(value)
    if number is None:
        raise ApiCollectionError(
            f"API row {row_number}: field {field!r} is missing or is not an integer"
        )
    return number


def _normalise_group(
    value: Any,
    field: str,
    row_number: int,
) -> tuple[str, int]:
    if value is None:
        return "не состоит", 0
    if not isinstance(value, dict):
        raise ApiCollectionError(
            f"API row {row_number}: field {field!r} must be an object or null"
        )

    group_id = _required_int(value.get("id"), f"{field}.id", row_number)
    group_name = " ".join(str(value.get("name") or "").split()).strip()
    if group_id <= 0 or not group_name:
        raise ApiCollectionError(
            f"API row {row_number}: field {field!r} contains an invalid id or name"
        )
    return group_name, group_id


def normalise_api_hero(raw: Any, row_number: int) -> tuple[int, dict]:
    if not isinstance(raw, dict):
        raise ApiCollectionError(f"API row {row_number}: expected an object")

    pid = _required_int(raw.get("id"), "id", row_number)
    if pid <= 0:
        raise ApiCollectionError(f"API row {row_number}: id must be positive")

    nickname = " ".join(str(raw.get("nickname") or "").split()).strip()
    if not nickname:
        raise ApiCollectionError(f"API row {row_number}: nickname is empty")

    hero: dict[str, Any] = {
        "ID": pid,
        "Имя": nickname,
        # The old profile parser always creates this field. The endpoint does
        # not provide it, so preserve the existing snapshot convention.
        "Чат": 0,
    }
    for api_key, snapshot_key in API_NUMERIC_FIELDS:
        hero[snapshot_key] = _required_int(raw.get(api_key), api_key, row_number)

    clan_name, clan_id = _normalise_group(raw.get("clan"), "clan", row_number)
    brotherhood_name, brotherhood_id = _normalise_group(
        raw.get("brotherhood"), "brotherhood", row_number
    )
    hero["Клан"] = clan_name
    hero["clan_id"] = clan_id
    hero["Братство"] = brotherhood_name
    hero["brotherhood_id"] = brotherhood_id
    return pid, hero


def parse_api_payload(
    payload: Any,
    min_player_count: int,
) -> tuple[dict[int, dict], dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ApiCollectionError("API response root must be an object")
    if payload.get("success") is not True:
        raise ApiCollectionError("API response has success != true")

    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ApiCollectionError("API response field 'data' must be a list")
    if len(rows) < max(1, min_player_count):
        raise ApiCollectionError(
            f"API returned only {len(rows)} players; minimum is {max(1, min_player_count)}"
        )

    meta = payload.get("meta")
    meta = dict(meta) if isinstance(meta, dict) else {}
    declared_count = parse_int(meta.get("count"))
    if declared_count is not None and declared_count != len(rows):
        raise ApiCollectionError(
            f"API meta.count={declared_count}, but data contains {len(rows)} rows"
        )

    heroes: dict[int, dict] = {}
    for row_number, raw in enumerate(rows, 1):
        pid, hero = normalise_api_hero(raw, row_number)
        if pid in heroes:
            raise ApiCollectionError(f"API contains duplicate player id {pid}")
        heroes[pid] = hero
    return heroes, meta


def fetch_from_bulk_api(
    domain: str,
    cookies: dict[str, str],
    *,
    attempts: int,
    retry_delay_seconds: float,
    timeout_seconds: float,
    min_player_count: int,
) -> ApiCollection:
    api_path = legacy.env_get("HEROES_API_PATH", "/api/heroes/for-glory").strip()
    if not api_path:
        api_path = "/api/heroes/for-glory"
    endpoint = urljoin(domain, api_path.lstrip("/"))

    attempts = max(1, attempts)
    retry_delay_seconds = max(0.0, retry_delay_seconds)
    timeout_seconds = max(1.0, timeout_seconds)
    last_error: Exception | None = None

    with requests.Session() as session:
        session.headers.update(legacy.HEADERS)
        session.headers.update({"Accept": "application/json"})
        session.cookies.update(cookies)

        for attempt in range(1, attempts + 1):
            try:
                LOG.info("Trying bulk heroes API, attempt %s/%s", attempt, attempts)
                response = session.get(
                    endpoint,
                    timeout=timeout_seconds,
                    allow_redirects=True,
                )
                response.raise_for_status()
                payload = response.json()
                heroes, meta = parse_api_payload(payload, min_player_count)
                LOG.info(
                    "Bulk heroes API succeeded on attempt %s: %s players",
                    attempt,
                    len(heroes),
                )
                return ApiCollection(
                    heroes=heroes,
                    endpoint=endpoint,
                    attempts_used=attempt,
                    meta=meta,
                )
            except (
                requests.RequestException,
                ValueError,
                TypeError,
                ApiCollectionError,
            ) as exc:
                last_error = exc
                LOG.warning(
                    "Bulk heroes API attempt %s/%s failed: %s",
                    attempt,
                    attempts,
                    exc,
                )
                if attempt < attempts:
                    time.sleep(retry_delay_seconds * attempt)

    raise ApiCollectionError(
        f"bulk heroes API failed after {attempts} attempts: {last_error}"
    )


def _database_path(raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else (legacy.ROOT / path).resolve()


def _baseline_ratio(
    results: dict[int, dict],
    baseline_ids: set[int],
) -> tuple[int, float]:
    success = len(baseline_ids.intersection(results))
    ratio = success / len(baseline_ids) if baseline_ids else 1.0
    return success, ratio


def _fallback_scope(
    api_ids: set[int],
    known_ids: list[int],
    highest_probed: int,
    probe_count: int,
) -> tuple[list[int], list[int], int, int]:
    """Return IDs that still require the old parser.

    The bulk API contains only level 5+ characters. Therefore every previously
    known ID absent from the API must still be checked through the old profile
    pages. The normal sequential probe is also retained so new level 1-4
    characters can be discovered before they ever appear in the API.
    """
    missing_known_ids = sorted(set(known_ids).difference(api_ids))
    probe_start = highest_probed + 1
    probe_end = highest_probed + max(0, probe_count)
    probe_ids = list(range(probe_start, probe_end + 1))

    fallback_ids = list(
        dict.fromkeys(
            pid
            for pid in [*missing_known_ids, *probe_ids]
            if pid not in api_ids
        )
    )
    return fallback_ids, missing_known_ids, probe_start, probe_end


def _write_failed_collection(
    results: dict[int, dict],
    failures: list[legacy.FetchFailure],
    baseline_ids: set[int],
    ratio: float,
) -> None:
    baseline_success = len(baseline_ids.intersection(results))
    failure_report = legacy.DATA_DIR / "last_failed_collection.json"
    failure_report.write_text(
        json.dumps(
            {
                "collection_source": "bulk_api+legacy_fallback",
                "baseline_success_ratio": ratio,
                "baseline_success": baseline_success,
                "baseline_total": len(baseline_ids),
                "failure_summary": legacy.failure_summary(failures, 20),
                "diagnostic_html_directory": str(legacy.DATA_DIR / "failed_html"),
                "failures": [asdict(item) for item in failures],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _add_hybrid_metadata(
    metadata_path: Path,
    collection: ApiCollection,
    *,
    fallback_requested: int,
    fallback_successful: int,
    missing_known_count: int,
) -> None:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata.update(
        {
            "collection_source": "bulk_api+legacy_fallback",
            "api_endpoint": collection.endpoint,
            "api_attempts_used": collection.attempts_used,
            "api_player_count": len(collection.heroes),
            "api_meta": collection.meta,
            "legacy_fallback_requested": fallback_requested,
            "legacy_fallback_successful": fallback_successful,
            "api_missing_known_ids_count": missing_known_count,
        }
    )
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> int:
    args = legacy.parse_args()
    cookies, domain = legacy.load_cookie_config()
    db_path = _database_path(args.db_path)
    known_ids, baseline_ids, highest_probed = legacy.load_collection_scope(db_path)
    known_names = legacy.load_known_names_from_db(db_path)

    try:
        collection = fetch_from_bulk_api(
            domain,
            cookies,
            attempts=int(legacy.env_get("HEROES_API_ATTEMPTS", "4")),
            retry_delay_seconds=float(
                legacy.env_get("HEROES_API_RETRY_DELAY_SECONDS", "5")
            ),
            timeout_seconds=float(legacy.env_get("HEROES_API_TIMEOUT_SECONDS", "45")),
            min_player_count=int(legacy.env_get("HEROES_API_MIN_PLAYER_COUNT", "100")),
        )
    except ApiCollectionError as exc:
        LOG.warning(
            "Bulk heroes API is unavailable or unsafe (%s). "
            "Falling back to the complete legacy collector.",
            exc,
        )
        return legacy.main()

    fallback_ids, missing_known_ids, probe_start, probe_end = _fallback_scope(
        set(collection.heroes),
        known_ids,
        highest_probed,
        args.probe_count,
    )
    LOG.info(
        "Hybrid scope: API=%s players; old parser=%s IDs "
        "(%s known IDs absent from API, sequential probe %s..%s)",
        len(collection.heroes),
        len(fallback_ids),
        len(missing_known_ids),
        probe_start,
        probe_end,
    )

    fallback_results: dict[int, dict] = {}
    failures: list[legacy.FetchFailure] = []
    achievement_failures: list[legacy.FetchFailure] = []
    if fallback_ids:
        fallback_results, failures, achievement_failures = asyncio.run(
            legacy.collect(
                fallback_ids,
                cookies,
                domain,
                args.concurrency,
                args.retries,
                known_names=known_names,
                achievement_retries=args.achievement_retries,
                # The API has already proved that the site and cookies work.
                # Missing known IDs can legitimately contain long runs of deleted
                # accounts, so the legacy collector must not abort after a sample
                # with zero successes.
                systemic_failure_sample_size=0,
            )
        )

    # API data is authoritative for level 5+ players. Old-parser results fill
    # API omissions and add newly discovered level 1-4 players.
    results = dict(collection.heroes)
    results.update(fallback_results)

    baseline_success, ratio = _baseline_ratio(results, baseline_ids)
    if baseline_ids and ratio < args.min_success_ratio:
        LOG.error(
            "Hybrid snapshot rejected: baseline success %.2f%% is below %.2f%% (%s/%s)",
            ratio * 100,
            args.min_success_ratio * 100,
            baseline_success,
            len(baseline_ids),
        )
        _write_failed_collection(results, failures, baseline_ids, ratio)
        return 2

    snapshot_path, metadata_path = legacy.save_snapshot(
        results,
        failures,
        achievement_failures,
        baseline_ids=baseline_ids,
        known_ids=known_ids,
        probe_start=probe_start,
        probe_end=probe_end,
    )
    _add_hybrid_metadata(
        metadata_path,
        collection,
        fallback_requested=len(fallback_ids),
        fallback_successful=len(fallback_results),
        missing_known_count=len(missing_known_ids),
    )

    LOG.info("Saved hybrid snapshot: %s", snapshot_path)
    LOG.info("Saved hybrid metadata: %s", metadata_path)
    LOG.info(
        "Hybrid collection complete: total=%s, API=%s, old parser=%s, "
        "profile failures=%s, achievement warnings=%s, baseline=%.2f%% (%s/%s)",
        len(results),
        len(collection.heroes),
        len(fallback_results),
        len(failures),
        len(achievement_failures),
        ratio * 100,
        baseline_success,
        len(baseline_ids),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
