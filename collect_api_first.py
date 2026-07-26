from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests

import get_data as legacy
from forglory.schema import parse_int


LOG = logging.getLogger("forglory.api_only")
SCOPE_MIN_LEVEL = 5

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
    """The bulk heroes endpoint did not return a complete safe response."""


@dataclass(frozen=True)
class ApiCollection:
    heroes: dict[int, dict]
    endpoint: str
    attempts_used: int
    meta: dict[str, Any]


def _read_cookie_records() -> list[dict[str, Any]]:
    """Load exported browser cookies without logging their values."""
    cookies_json = legacy.env_get("COOKIES_JSON", "").strip()
    try:
        if cookies_json:
            raw = json.loads(cookies_json)
        else:
            cookie_path = Path(
                legacy.env_get(
                    "COOKIES_FILE",
                    str(legacy.ROOT / "static" / "cfg.json"),
                )
            )
            if not cookie_path.exists():
                raise ApiCollectionError(
                    "Cookies are not configured. Set COOKIES_JSON or COOKIES_FILE."
                )
            raw = json.loads(cookie_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        raise ApiCollectionError(f"Cannot read cookie configuration: {exc}") from exc

    if not isinstance(raw, list):
        raise ApiCollectionError("Cookie configuration must be a JSON list")
    return [item for item in raw if isinstance(item, dict)]


def load_cookie_config() -> tuple[dict[str, str], str]:
    """Build cookies and the game URL strictly from the session-cookie domain."""
    raw = _read_cookie_records()
    cookies = {
        str(item.get("name")): str(item.get("value"))
        for item in raw
        if item.get("name") and item.get("value")
    }
    if not cookies:
        raise ApiCollectionError("Cookie configuration contains no usable cookies")

    session_cookie = next(
        (
            item
            for item in raw
            if item.get("name") == "wekings_session" and item.get("domain")
        ),
        None,
    )
    if session_cookie is None:
        raise ApiCollectionError(
            "Cookie configuration has no wekings_session cookie with a domain"
        )

    cookie_domain = str(session_cookie["domain"]).strip().lstrip(".")
    if not cookie_domain or "/" in cookie_domain or "://" in cookie_domain:
        raise ApiCollectionError(
            f"Invalid domain in wekings_session cookie: {cookie_domain!r}"
        )
    return cookies, f"https://{cookie_domain}/"


def api_endpoint(domain: str) -> str:
    """Return /heroes/for-glory on the cookie-derived game domain."""
    parsed = urlparse(str(domain or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ApiCollectionError(f"Invalid cookie-derived game domain: {domain!r}")
    base = f"{parsed.scheme}://{parsed.netloc}/"
    return urljoin(base, "heroes/for-glory")


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
        # The endpoint has no chat field; keep the established snapshot schema.
        "Чат": 0,
    }
    for api_key, snapshot_key in API_NUMERIC_FIELDS:
        hero[snapshot_key] = _required_int(raw.get(api_key), api_key, row_number)

    if hero["Уровень"] < SCOPE_MIN_LEVEL:
        raise ApiCollectionError(
            f"API row {row_number}: level {hero['Уровень']} is below {SCOPE_MIN_LEVEL}"
        )

    clan_name, clan_id = _normalise_group(raw.get("clan"), "clan", row_number)
    brotherhood_name, brotherhood_id = _normalise_group(
        raw.get("brotherhood"), "brotherhood", row_number
    )
    hero["Клан"] = clan_name
    hero["clan_id"] = clan_id
    hero["Братство"] = brotherhood_name
    hero["brotherhood_id"] = brotherhood_id
    return pid, hero


def _group_coverage(heroes: dict[int, dict]) -> dict[str, int]:
    clan_ids = {
        int(hero.get("clan_id") or 0)
        for hero in heroes.values()
        if int(hero.get("clan_id") or 0) > 0
    }
    brotherhood_ids = {
        int(hero.get("brotherhood_id") or 0)
        for hero in heroes.values()
        if int(hero.get("brotherhood_id") or 0) > 0
    }
    return {
        "clan_members": sum(1 for hero in heroes.values() if int(hero.get("clan_id") or 0) > 0),
        "clans": len(clan_ids),
        "brotherhood_members": sum(
            1 for hero in heroes.values() if int(hero.get("brotherhood_id") or 0) > 0
        ),
        "brotherhoods": len(brotherhood_ids),
    }



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
    if not isinstance(meta, dict):
        raise ApiCollectionError("API response field 'meta' must be an object")
    meta = dict(meta)

    declared_min_level = parse_int(meta.get("min_level"))
    if declared_min_level != SCOPE_MIN_LEVEL:
        raise ApiCollectionError(
            f"API meta.min_level={declared_min_level!r}; expected {SCOPE_MIN_LEVEL}"
        )

    declared_count = parse_int(meta.get("count"))
    if declared_count != len(rows):
        raise ApiCollectionError(
            f"API meta.count={declared_count!r}, but data contains {len(rows)} rows"
        )

    heroes: dict[int, dict] = {}
    for row_number, raw in enumerate(rows, 1):
        pid, hero = normalise_api_hero(raw, row_number)
        if pid in heroes:
            raise ApiCollectionError(f"API contains duplicate player id {pid}")
        heroes[pid] = hero

    group_coverage = _group_coverage(heroes)
    if group_coverage["clan_members"] == 0 or group_coverage["brotherhood_members"] == 0:
        raise ApiCollectionError(
            "API response contains no usable clan or brotherhood memberships: "
            f"{group_coverage}"
        )
    meta["forglory_group_coverage"] = group_coverage
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
    endpoint = api_endpoint(domain)
    attempts = max(1, attempts)
    retry_delay_seconds = max(0.0, retry_delay_seconds)
    timeout_seconds = max(1.0, timeout_seconds)
    last_error: Exception | None = None

    LOG.info("Bulk heroes endpoint: %s", endpoint)
    with requests.Session() as session:
        session.headers.update(legacy.HEADERS)
        session.headers.update(
            {
                "Accept": "application/json",
                "Referer": domain,
            }
        )
        session.cookies.update(cookies)

        for attempt in range(1, attempts + 1):
            try:
                LOG.info("Trying bulk heroes endpoint, attempt %s/%s", attempt, attempts)
                response = session.get(
                    endpoint,
                    timeout=timeout_seconds,
                    allow_redirects=True,
                )
                response.raise_for_status()
                heroes, meta = parse_api_payload(response.json(), min_player_count)
                coverage = _group_coverage(heroes)
                LOG.info(
                    "Bulk heroes endpoint succeeded on attempt %s: %s players; "
                    "clan members=%s (%s clans), brotherhood members=%s (%s brotherhoods)",
                    attempt,
                    len(heroes),
                    coverage["clan_members"],
                    coverage["clans"],
                    coverage["brotherhood_members"],
                    coverage["brotherhoods"],
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
                status = getattr(getattr(exc, "response", None), "status_code", None)
                LOG.warning(
                    "Bulk heroes endpoint attempt %s/%s failed%s: %s",
                    attempt,
                    attempts,
                    f" with HTTP {status}" if status is not None else "",
                    exc,
                )
                if attempt < attempts:
                    time.sleep(retry_delay_seconds * attempt)

    raise ApiCollectionError(
        f"bulk heroes endpoint {endpoint} failed after {attempts} attempts: {last_error}"
    )


def _database_path(raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else (legacy.ROOT / path).resolve()


def load_previous_level5_ids(db_path: Path) -> tuple[set[int], str | None]:
    """Load only level-5+ players from the latest published snapshot."""
    if not db_path.exists():
        return set(), None

    try:
        connection = sqlite3.connect(db_path)
        try:
            row = connection.execute(
                "SELECT snapshot_id,filename FROM snapshots ORDER BY ts DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return set(), None
            snapshot_id, filename = int(row[0]), str(row[1])
            rows = connection.execute(
                "SELECT pid FROM observations WHERE snapshot_id=? AND level>=?",
                (snapshot_id, SCOPE_MIN_LEVEL),
            ).fetchall()
            return {int(item[0]) for item in rows}, filename
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise ApiCollectionError(f"Cannot read restored SQLite baseline: {exc}") from exc


def _coverage_ratio(
    current_ids: set[int],
    previous_ids: set[int],
) -> tuple[int, float]:
    retained = len(current_ids.intersection(previous_ids))
    ratio = retained / len(previous_ids) if previous_ids else 1.0
    return retained, ratio


def write_failure_report(
    error: str,
    *,
    endpoint: str | None = None,
    current_count: int | None = None,
    previous_count: int | None = None,
    retained_count: int | None = None,
    retained_ratio: float | None = None,
) -> Path:
    legacy.DATA_DIR.mkdir(parents=True, exist_ok=True)
    report_path = legacy.DATA_DIR / "last_failed_collection.json"
    report_path.write_text(
        json.dumps(
            {
                "collection_source": "bulk_api",
                "scope_min_level": SCOPE_MIN_LEVEL,
                "error": error,
                "api_endpoint": endpoint,
                "api_player_count": current_count,
                "previous_level5_count": previous_count,
                "retained_previous_level5_count": retained_count,
                "retained_previous_level5_ratio": retained_ratio,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return report_path


def save_api_snapshot(
    collection: ApiCollection,
    previous_ids: set[int],
    previous_snapshot: str | None,
    retained_count: int,
    retained_ratio: float,
) -> tuple[Path, Path]:
    current_ids = set(collection.heroes)
    highest_id = max(current_ids, default=0)
    snapshot_path, metadata_path = legacy.save_snapshot(
        collection.heroes,
        failures=[],
        achievement_failures=[],
        baseline_ids=previous_ids,
        known_ids=sorted(current_ids),
        probe_start=0,
        probe_end=0,
    )

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata.update(
        {
            "collection_source": "bulk_api",
            "scope_min_level": SCOPE_MIN_LEVEL,
            "api_endpoint": collection.endpoint,
            "api_attempts_used": collection.attempts_used,
            "api_player_count": len(collection.heroes),
            "api_meta": collection.meta,
            "previous_snapshot": previous_snapshot,
            "previous_level5_count": len(previous_ids),
            "retained_previous_level5_count": retained_count,
            "retained_previous_level5_ratio": retained_ratio,
            "probe_start": None,
            "probe_end": None,
            "highest_probed_id": highest_id,
            "legacy_fallback_used": False,
        }
    )
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return snapshot_path, metadata_path


def main() -> int:
    args = legacy.parse_args()
    db_path = _database_path(args.db_path)

    try:
        cookies, domain = load_cookie_config()
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
        previous_ids, previous_snapshot = load_previous_level5_ids(db_path)
    except ApiCollectionError as exc:
        LOG.error("Collection stopped: %s", exc)
        write_failure_report(str(exc))
        return 2

    retained_count, retained_ratio = _coverage_ratio(
        set(collection.heroes),
        previous_ids,
    )
    if previous_ids and retained_ratio < args.min_success_ratio:
        error = (
            "API snapshot rejected: retained previous level-5+ coverage "
            f"{retained_ratio:.2%} is below {args.min_success_ratio:.2%} "
            f"({retained_count}/{len(previous_ids)})"
        )
        LOG.error(error)
        write_failure_report(
            error,
            endpoint=collection.endpoint,
            current_count=len(collection.heroes),
            previous_count=len(previous_ids),
            retained_count=retained_count,
            retained_ratio=retained_ratio,
        )
        return 2

    snapshot_path, metadata_path = save_api_snapshot(
        collection,
        previous_ids,
        previous_snapshot,
        retained_count,
        retained_ratio,
    )
    LOG.info("Saved level-5+ API snapshot: %s", snapshot_path)
    LOG.info("Saved level-5+ API metadata: %s", metadata_path)
    LOG.info(
        "Collection complete: players=%s, retained previous level-5+=%.2f%% (%s/%s)",
        len(collection.heroes),
        retained_ratio * 100,
        retained_count,
        len(previous_ids),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
