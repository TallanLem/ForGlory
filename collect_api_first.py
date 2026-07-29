from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

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
    ("lord_wins", "Побед над Владыкой"),
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

OPTIONAL_API_NUMERIC_FIELDS: tuple[tuple[str, str], ...] = ()

API_VALUE_PARENTS = ("achievements", "stats", "statistics", "counters", "hero")


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


EMPTY_GROUP_NAMES = {"", "не состоит", "none", "null", "нет", "-", "—"}

GROUP_FIELD_ALIASES: dict[str, dict[str, tuple[str, ...]]] = {
    "clan": {
        "containers": (
            "clan", "clan_info", "clan_data", "clanInfo", "clanData", "Клан", "клан",
        ),
        "ids": (
            "clan_id", "clanId", "clan_game_id", "clanGameId", "Клан_id", "клан_id",
        ),
        "names": (
            "clan_name", "clanName", "clan_title", "clanTitle", "Клан", "клан",
        ),
    },
    "brotherhood": {
        "containers": (
            "brotherhood", "brotherhood_info", "brotherhood_data",
            "brotherhoodInfo", "brotherhoodData", "Братство", "братство",
        ),
        "ids": (
            "brotherhood_id", "brotherhoodId", "brotherhood_game_id",
            "brotherhoodGameId", "Братство_id", "братство_id",
        ),
        "names": (
            "brotherhood_name", "brotherhoodName", "brotherhood_title",
            "brotherhoodTitle", "Братство", "братство",
        ),
    },
}

GROUP_PARENT_KEYS = (
    "groups", "group", "memberships", "membership", "associations", "relations",
)


def _clean_group_name(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list, tuple, set, bool)):
        return None
    text = " ".join(str(value).split()).strip()
    if not text or text.casefold() in EMPTY_GROUP_NAMES:
        return None
    return text


def _first_value(scopes: list[dict[str, Any]], keys: tuple[str, ...]) -> Any:
    for scope in scopes:
        for key in keys:
            if key in scope:
                return scope.get(key)
    return None


def _normalise_group_from_row(
    raw: dict[str, Any],
    field: str,
    row_number: int,
) -> tuple[str, int]:
    """Read a group from nested or flat endpoint fields.

    The endpoint uses snake_case for the other player fields. Older code assumed
    ``clan`` and ``brotherhood`` were nested objects, which silently converted
    every player to "не состоит" when the endpoint returned flat
    ``*_id``/``*_name`` fields instead.
    """
    aliases = GROUP_FIELD_ALIASES[field]
    scopes: list[dict[str, Any]] = [raw]
    for parent_key in GROUP_PARENT_KEYS:
        parent = raw.get(parent_key)
        if isinstance(parent, dict):
            scopes.append(parent)

    container = _first_value(scopes, aliases["containers"])
    nested = container if isinstance(container, dict) else None

    group_id = None
    group_name = None
    if nested is not None:
        group_id = parse_int(
            nested.get("id")
            or nested.get(f"{field}_id")
            or nested.get("game_id")
            or nested.get("gameId")
        )
        group_name = _clean_group_name(
            nested.get("name")
            or nested.get(f"{field}_name")
            or nested.get("title")
        )
    elif container is not None:
        if isinstance(container, (int, float)) and not isinstance(container, bool):
            group_id = parse_int(container)
        else:
            group_name = _clean_group_name(container)

    if group_id is None:
        group_id = parse_int(_first_value(scopes, aliases["ids"]))
    if group_name is None:
        group_name = _clean_group_name(_first_value(scopes, aliases["names"]))

    if not group_name and not group_id:
        return "не состоит", 0
    if group_id is None or group_id <= 0 or not group_name:
        return "не состоит", 0
    return group_name, group_id


def _endpoint_group_schema(rows: list[Any]) -> dict[str, Any]:
    all_keys: set[str] = set()
    group_like: set[str] = set()
    sample_shapes: dict[str, str] = {}
    markers = ("clan", "brother", "guild", "клан", "брат")
    for raw in rows[:200]:
        if not isinstance(raw, dict):
            continue
        for key, value in raw.items():
            key_text = str(key)
            all_keys.add(key_text)
            if any(marker in key_text.casefold() for marker in markers):
                group_like.add(key_text)
                if key_text not in sample_shapes:
                    if isinstance(value, dict):
                        sample_shapes[key_text] = "object:" + ",".join(sorted(map(str, value.keys())))
                    else:
                        sample_shapes[key_text] = type(value).__name__
    return {
        "group_like_keys": sorted(group_like),
        "group_like_shapes": sample_shapes,
        "row_keys": sorted(all_keys),
    }

def _api_numeric_value(raw: dict[str, Any], key: str) -> Any:
    """Read a counter from the current or a future nested endpoint shape."""
    if key in raw:
        return raw.get(key)
    for parent_key in API_VALUE_PARENTS:
        parent = raw.get(parent_key)
        if isinstance(parent, dict) and key in parent:
            return parent.get(key)
    return None


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
        hero[snapshot_key] = _required_int(
            _api_numeric_value(raw, api_key),
            api_key,
            row_number,
        )
    for api_key, snapshot_key in OPTIONAL_API_NUMERIC_FIELDS:
        hero[snapshot_key] = parse_int(_api_numeric_value(raw, api_key))

    if hero["Уровень"] < SCOPE_MIN_LEVEL:
        raise ApiCollectionError(
            f"API row {row_number}: level {hero['Уровень']} is below {SCOPE_MIN_LEVEL}"
        )

    clan_name, clan_id = _normalise_group_from_row(raw, "clan", row_number)
    brotherhood_name, brotherhood_id = _normalise_group_from_row(
        raw, "brotherhood", row_number
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

    raw_meta = payload.get("meta")
    meta = dict(raw_meta) if isinstance(raw_meta, dict) else {}

    declared_min_level = parse_int(meta.get("min_level"))
    if declared_min_level is not None and declared_min_level != SCOPE_MIN_LEVEL:
        raise ApiCollectionError(
            f"API meta.min_level={declared_min_level!r}; expected {SCOPE_MIN_LEVEL}"
        )

    declared_count = parse_int(meta.get("count"))
    if declared_count is not None and declared_count != len(rows):
        raise ApiCollectionError(
            f"API meta.count={declared_count!r}, but data contains {len(rows)} rows"
        )
    meta.setdefault("min_level", SCOPE_MIN_LEVEL)
    meta.setdefault("count", len(rows))

    heroes: dict[int, dict] = {}
    for row_number, raw in enumerate(rows, 1):
        pid, hero = normalise_api_hero(raw, row_number)
        if pid in heroes:
            raise ApiCollectionError(f"API contains duplicate player id {pid}")
        heroes[pid] = hero

    group_coverage = _group_coverage(heroes)
    meta["forglory_group_coverage"] = group_coverage
    meta["forglory_endpoint_schema"] = _endpoint_group_schema(rows)
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



@dataclass(frozen=True)
class GroupRoster:
    group_id: int
    name: str
    members: frozenset[int]


def parse_group_roster_html(
    html: str,
    *,
    group_kind: str,
    group_id: int,
) -> GroupRoster | None:
    """Parse one clan/brotherhood warriors page.

    A missing group is rendered by the game as a normal HTTP 200 error page with
    "Что-то пошло не так". That page is the sequential scan terminator.
    """
    if group_kind not in {"clan", "brotherhood"}:
        raise ValueError(f"Unsupported group kind: {group_kind}")

    soup = BeautifulSoup(html or "", "html.parser")
    prefix = "Воины клана " if group_kind == "clan" else "Воины братства "
    header = next(
        (
            tag
            for tag in soup.select("p.group-header")
            if " ".join(tag.get_text(" ", strip=True).split()).startswith(prefix)
        ),
        None,
    )
    if header is None:
        page_text = " ".join(soup.get_text(" ", strip=True).split())
        if "Что-то пошло не так" in page_text:
            return None
        raise ApiCollectionError(
            f"{group_kind} roster {group_id}: group header was not found"
        )

    header_text = " ".join(header.get_text(" ", strip=True).split())
    name = header_text[len(prefix):].strip()
    if not name:
        raise ApiCollectionError(
            f"{group_kind} roster {group_id}: group name is empty"
        )

    members: set[int] = set()
    for link in soup.select('a[href*="hero/detail?player="]'):
        match = re.search(r"(?:[?&])player=(\d+)", str(link.get("href") or ""))
        if match:
            members.add(int(match.group(1)))

    return GroupRoster(group_id=group_id, name=name, members=frozenset(members))


def load_known_group_ids(db_path: Path, group_kind: str) -> list[int]:
    """Load group IDs from the most recent snapshot that still has memberships.

    The latest snapshot may be the broken endpoint-only snapshot with zero group
    fields. Looking for the newest snapshot with at least one positive ID keeps
    the roster scan fast and preserves sparse/deleted historical identifiers.
    """
    if group_kind not in {"clan", "brotherhood"}:
        raise ValueError(f"Unsupported group kind: {group_kind}")
    if not db_path.exists() or db_path.stat().st_size == 0:
        return []

    column = "clan_game_id" if group_kind == "clan" else "brotherhood_game_id"
    try:
        conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True)
        try:
            tables = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
                )
            }
            if "observations" not in tables:
                return []
            row = conn.execute(
                f"SELECT MAX(snapshot_id) FROM observations WHERE {column}>0"
            ).fetchone()
            snapshot_id = int(row[0]) if row and row[0] is not None else None
            if snapshot_id is None:
                return []
            rows = conn.execute(
                f"SELECT DISTINCT {column} FROM observations "
                f"WHERE snapshot_id=? AND {column}>0 ORDER BY {column}",
                (snapshot_id,),
            ).fetchall()
            return [int(item[0]) for item in rows if int(item[0]) > 0]
        finally:
            conn.close()
    except (OSError, sqlite3.Error, ValueError) as exc:
        LOG.warning(
            "Cannot load historical %s IDs from %s: %s",
            group_kind,
            db_path,
            exc,
        )
        return []


def scan_group_rosters(
    domain: str,
    cookies: dict[str, str],
    group_kind: str,
    *,
    timeout_seconds: float,
    attempts: int,
    retry_delay_seconds: float,
    known_group_ids: Iterable[int] = (),
    discovery_window: int = 50,
) -> list[GroupRoster]:
    """Refresh known group IDs and probe only the next small ID window.

    Existing sparse IDs come from the latest successful database snapshot and,
    when available, from the current endpoint response. Every known ID receives
    the configured retries. New groups are searched only from ``max_known + 1``
    through ``max_known + discovery_window``. Individual page failures are
    logged and skipped; roster collection never invalidates player data.
    """
    if group_kind not in {"clan", "brotherhood"}:
        raise ValueError(f"Unsupported group kind: {group_kind}")

    attempts = max(1, int(attempts))
    retry_delay_seconds = max(0.0, float(retry_delay_seconds))
    timeout_seconds = max(1.0, float(timeout_seconds))
    discovery_window = max(0, int(discovery_window))
    known_ids = sorted({int(group_id) for group_id in known_group_ids if int(group_id) > 0})
    rosters_by_id: dict[int, GroupRoster] = {}
    failed_ids: list[int] = []

    with requests.Session() as session:
        session.headers.update(legacy.HEADERS)
        session.headers.update({"Accept": "text/html", "Referer": domain})
        session.cookies.update(cookies)

        def fetch_one(group_id: int, configured_attempts: int) -> GroupRoster | None:
            url = urljoin(domain, f"{group_kind}/warriors?id={group_id}")
            last_error: Exception | None = None
            for attempt in range(1, max(1, configured_attempts) + 1):
                try:
                    response = session.get(
                        url,
                        timeout=timeout_seconds,
                        allow_redirects=True,
                    )
                    if response.status_code in {404, 410}:
                        return None
                    response.raise_for_status()

                    final = urlparse(response.url)
                    if not final.path.rstrip("/").endswith(
                        f"/{group_kind}/warriors"
                    ):
                        raise ApiCollectionError(
                            f"{group_kind} roster {group_id}: "
                            f"unexpected redirect to {response.url}"
                        )

                    roster = parse_group_roster_html(
                        response.text,
                        group_kind=group_kind,
                        group_id=group_id,
                    )
                    if roster is not None:
                        return roster
                    if attempt < max(1, configured_attempts):
                        time.sleep(retry_delay_seconds * attempt)
                except (requests.RequestException, ApiCollectionError) as exc:
                    last_error = exc
                    if attempt < max(1, configured_attempts):
                        time.sleep(retry_delay_seconds * attempt)
                        continue
                    raise ApiCollectionError(
                        f"{group_kind} roster {group_id} failed after "
                        f"{configured_attempts} attempts: {exc}"
                    ) from exc
            if last_error is not None:
                raise ApiCollectionError(
                    f"{group_kind} roster {group_id} failed: {last_error}"
                )
            return None

        if known_ids:
            LOG.info(
                "%s roster scan: refreshing %s known IDs; range=%s..%s",
                group_kind,
                len(known_ids),
                known_ids[0],
                known_ids[-1],
            )
        else:
            LOG.warning(
                "%s roster scan has no historical or endpoint IDs; "
                "probing bootstrap range 1..%s once",
                group_kind,
                discovery_window,
            )

        for index, group_id in enumerate(known_ids, 1):
            try:
                roster = fetch_one(group_id, attempts)
            except Exception as exc:
                failed_ids.append(group_id)
                LOG.warning(
                    "%s known roster id=%s could not be refreshed; skipping: %s",
                    group_kind,
                    group_id,
                    exc,
                )
                continue
            if roster is None:
                LOG.info(
                    "%s known roster id=%s no longer exists; skipping",
                    group_kind,
                    group_id,
                )
                continue
            rosters_by_id[group_id] = roster
            if index % 50 == 0:
                LOG.info(
                    "%s roster refresh progress: checked=%s/%s, valid=%s, members=%s, failed=%s",
                    group_kind,
                    index,
                    len(known_ids),
                    len(rosters_by_id),
                    sum(len(item.members) for item in rosters_by_id.values()),
                    len(failed_ids),
                )

        discovery_start = (known_ids[-1] + 1) if known_ids else 1
        discovery_end = discovery_start + discovery_window - 1
        discovery_ids = range(discovery_start, discovery_end + 1) if discovery_window else ()
        for index, group_id in enumerate(discovery_ids, 1):
            try:
                roster = fetch_one(group_id, 1)
            except Exception as exc:
                failed_ids.append(group_id)
                LOG.warning(
                    "%s roster discovery id=%s failed; skipping without aborting collection: %s",
                    group_kind,
                    group_id,
                    exc,
                )
                continue
            if roster is not None:
                rosters_by_id[group_id] = roster
                LOG.info(
                    "%s roster scan discovered new valid id=%s (%s)",
                    group_kind,
                    group_id,
                    roster.name,
                )
            if index % 50 == 0:
                LOG.info(
                    "%s roster discovery progress: checked=%s/%s, valid_total=%s, members=%s, failed=%s",
                    group_kind,
                    index,
                    discovery_window,
                    len(rosters_by_id),
                    sum(len(item.members) for item in rosters_by_id.values()),
                    len(failed_ids),
                )

    LOG.info(
        "%s roster scan finished: known=%s, discovery=%s..%s, valid=%s, members=%s, failed=%s",
        group_kind,
        len(known_ids),
        discovery_start,
        discovery_end if discovery_window else discovery_start - 1,
        len(rosters_by_id),
        sum(len(item.members) for item in rosters_by_id.values()),
        len(failed_ids),
    )
    return [rosters_by_id[key] for key in sorted(rosters_by_id)]

def apply_group_rosters(
    heroes: dict[int, dict],
    group_kind: str,
    rosters: list[GroupRoster],
) -> dict[str, int]:
    """Overlay roster memberships without making group errors fatal.

    Endpoint memberships are retained as a fallback when the game later starts
    returning them again. Roster pages overwrite only players that were actually
    observed on a valid page. Duplicate membership is logged and the first
    valid assignment wins.
    """
    if group_kind == "clan":
        name_key, id_key = "Клан", "clan_id"
    elif group_kind == "brotherhood":
        name_key, id_key = "Братство", "brotherhood_id"
    else:
        raise ValueError(f"Unsupported group kind: {group_kind}")

    assigned = 0
    duplicate_members: set[int] = set()
    seen_members: set[int] = set()
    for roster in rosters:
        for pid in roster.members:
            if pid in seen_members:
                duplicate_members.add(pid)
                continue
            seen_members.add(pid)
            hero = heroes.get(pid)
            if hero is None:
                continue
            hero[name_key] = roster.name
            hero[id_key] = roster.group_id
            assigned += 1

    if duplicate_members:
        LOG.warning(
            "%s roster pages contain duplicate player memberships; "
            "keeping the first assignment for %s players, first IDs=%s",
            group_kind,
            len(duplicate_members),
            sorted(duplicate_members)[:20],
        )
    return {
        "groups": len(rosters),
        "members_on_pages": len(seen_members),
        "members_assigned": assigned,
        "duplicate_members": len(duplicate_members),
    }


def replace_groups_from_rosters(
    collection: ApiCollection,
    domain: str,
    cookies: dict[str, str],
) -> None:
    """Best-effort group enrichment that can never invalidate player data.

    The endpoint remains the primary source for the level-5+ player snapshot.
    Clan and brotherhood pages are scanned independently. A failure in one kind
    or one page is recorded in metadata and logged, while the endpoint snapshot
    continues to be saved and published.
    """
    endpoint_coverage = _group_coverage(collection.heroes)
    LOG.info(
        "Refreshing clan and brotherhood memberships from warriors pages; "
        "endpoint coverage before enrichment=%s",
        endpoint_coverage,
    )

    scan_options = {
        "timeout_seconds": float(
            legacy.env_get("GROUP_SCAN_TIMEOUT_SECONDS", "30")
        ),
        "attempts": int(legacy.env_get("GROUP_SCAN_ATTEMPTS", "3")),
        "retry_delay_seconds": float(
            legacy.env_get("GROUP_SCAN_RETRY_DELAY_SECONDS", "2")
        ),
        "discovery_window": int(
            legacy.env_get("GROUP_SCAN_DISCOVERY_WINDOW", "50")
        ),
    }
    db_path = Path(legacy.env_get("DB_PATH", "data/db/ratings.sqlite"))
    historical_ids_by_kind = {
        group_kind: load_known_group_ids(db_path, group_kind)
        for group_kind in ("clan", "brotherhood")
    }
    endpoint_ids_by_kind = {
        "clan": sorted(
            {
                int(hero.get("clan_id") or 0)
                for hero in collection.heroes.values()
                if int(hero.get("clan_id") or 0) > 0
            }
        ),
        "brotherhood": sorted(
            {
                int(hero.get("brotherhood_id") or 0)
                for hero in collection.heroes.values()
                if int(hero.get("brotherhood_id") or 0) > 0
            }
        ),
    }
    known_ids_by_kind = {
        group_kind: sorted(
            set(historical_ids_by_kind[group_kind])
            | set(endpoint_ids_by_kind[group_kind])
        )
        for group_kind in ("clan", "brotherhood")
    }
    LOG.info(
        "Group IDs prepared for roster refresh: "
        "historical clans=%s, endpoint clans=%s, total clans=%s; "
        "historical brotherhoods=%s, endpoint brotherhoods=%s, total brotherhoods=%s",
        len(historical_ids_by_kind["clan"]),
        len(endpoint_ids_by_kind["clan"]),
        len(known_ids_by_kind["clan"]),
        len(historical_ids_by_kind["brotherhood"]),
        len(endpoint_ids_by_kind["brotherhood"]),
        len(known_ids_by_kind["brotherhood"]),
    )

    rosters_by_kind: dict[str, list[GroupRoster]] = {
        "clan": [],
        "brotherhood": [],
    }
    scan_errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            group_kind: executor.submit(
                scan_group_rosters,
                domain,
                cookies,
                group_kind,
                known_group_ids=known_ids_by_kind[group_kind],
                **scan_options,
            )
            for group_kind in ("clan", "brotherhood")
        }
        for group_kind, future in futures.items():
            try:
                rosters_by_kind[group_kind] = future.result()
            except Exception as exc:
                scan_errors[group_kind] = str(exc)
                LOG.exception(
                    "%s roster scan failed completely; continuing without it: %s",
                    group_kind,
                    exc,
                )

    scan_meta: dict[str, Any] = {}
    for group_kind in ("clan", "brotherhood"):
        try:
            scan_meta[group_kind] = apply_group_rosters(
                collection.heroes,
                group_kind,
                rosters_by_kind[group_kind],
            )
        except Exception as exc:
            scan_errors[group_kind] = str(exc)
            scan_meta[group_kind] = {
                "groups": 0,
                "members_on_pages": 0,
                "members_assigned": 0,
                "duplicate_members": 0,
            }
            LOG.exception(
                "%s roster application failed; keeping endpoint values: %s",
                group_kind,
                exc,
            )

    coverage = _group_coverage(collection.heroes)
    collection.meta["forglory_endpoint_group_coverage"] = endpoint_coverage
    collection.meta["forglory_group_source"] = "warriors_pages_best_effort"
    collection.meta["forglory_group_roster_scan"] = scan_meta
    collection.meta["forglory_group_coverage"] = coverage
    if scan_errors:
        collection.meta["forglory_group_errors"] = scan_errors
    collection.meta["forglory_group_status"] = (
        "ok"
        if coverage["clan_members"] > 0 and coverage["brotherhood_members"] > 0
        else "partial_or_missing"
    )

    if coverage["clan_members"] == 0 or coverage["brotherhood_members"] == 0:
        LOG.warning(
            "Group enrichment is incomplete, but player snapshot will still be saved: "
            "coverage=%s, scan=%s, errors=%s",
            coverage,
            scan_meta,
            scan_errors,
        )
    else:
        LOG.info(
            "Warriors-page group enrichment completed: coverage=%s, scan=%s",
            coverage,
            scan_meta,
        )


_LORD_WINS_JSON_PATTERNS = (
    re.compile(r'["\']lord_wins["\']\s*[:=]\s*["\']?(\d[\d\s\xa0]*)', re.IGNORECASE),
    re.compile(r'data-(?:achievement-)?(?:key|name|code)=["\']lord_wins["\'][^>]*?(?:data-value|data-count)=["\'](\d[\d\s\xa0]*)', re.IGNORECASE),
    re.compile(r'(?:data-lord-wins|lord-wins)=["\'](\d[\d\s\xa0]*)', re.IGNORECASE),
)


def parse_lord_wins_from_achievements(html: str) -> int | None:
    """Extract the absolute lord-win counter from an achievements page.

    Current pages expose the machine field ``lord_wins`` in embedded state. The
    visible-card fallbacks are deliberately conservative and are used only when
    that machine field is absent.
    """
    if not html:
        return None

    for pattern in _LORD_WINS_JSON_PATTERNS:
        match = pattern.search(html)
        if match:
            value = parse_int(match.group(1))
            if value is not None:
                return value

    soup = BeautifulSoup(html, "html.parser")
    selectors = (
        '[data-achievement-key="lord_wins"]',
        '[data-achievement-name="lord_wins"]',
        '[data-achievement-code="lord_wins"]',
        '[data-lord-wins]',
        '[id*="lord_wins"]',
    )
    for selector in selectors:
        for tag in soup.select(selector):
            for attribute in ("data-value", "data-count", "data-lord-wins", "value"):
                value = parse_int(tag.get(attribute))
                if value is not None:
                    return value
            text_value = parse_int(tag.get_text(" ", strip=True))
            if text_value is not None:
                return text_value

    cards = soup.select(
        "div.flex.flex-col.p-2.leading-5, [data-achievement], "
        ".achievement, [class*='achievement']"
    )
    for card in cards:
        text = " ".join(card.get_text(" ", strip=True).split())
        normalized = text.casefold()
        if "владык" not in normalized and "lord" not in normalized:
            continue

        explicit_patterns = (
            r'(?:побед\w*\s+над\s+владык\w*|lord[_\s-]*wins)\D{0,30}(\d[\d\s\xa0]*)',
            r'(\d[\d\s\xa0]*)\s+(?:побед\w*\s+над\s+)?владык\w*',
        )
        for raw_pattern in explicit_patterns:
            match = re.search(raw_pattern, text, re.IGNORECASE)
            if match:
                value = parse_int(match.group(1))
                if value is not None:
                    return value

        progress = re.search(r'(\d[\d\s\xa0]*)\s+из\s+\d[\d\s\xa0]*', text, re.IGNORECASE)
        if progress:
            value = parse_int(progress.group(1))
            if value is not None:
                return value
    return None


def _previous_lord_wins(db_path: Path) -> dict[int, int]:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return {}
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    try:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(observations)")}
        if "lord_wins" not in columns:
            return {}
        latest = conn.execute(
            "SELECT snapshot_id FROM snapshots ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if latest is None:
            return {}
        return {
            int(pid): int(value)
            for pid, value in conn.execute(
                "SELECT pid,lord_wins FROM observations "
                "WHERE snapshot_id=? AND lord_wins IS NOT NULL",
                (int(latest[0]),),
            )
        }
    finally:
        conn.close()


def _fetch_one_lord_wins(
    pid: int,
    domain: str,
    cookies: dict[str, str],
    retries: int,
    timeout_seconds: float,
) -> tuple[int, int | None, str | None]:
    url = f"{domain}achievements?player={pid}"
    last_error = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            response = requests.get(
                url,
                cookies=cookies,
                headers=legacy.HEADERS,
                timeout=max(5.0, timeout_seconds),
                allow_redirects=True,
            )
            if response.status_code == 200:
                value = parse_lord_wins_from_achievements(response.text)
                if value is not None:
                    return pid, value, None
                last_error = "lord_wins_not_found"
            else:
                last_error = f"http_{response.status_code}"
        except requests.RequestException as exc:
            last_error = type(exc).__name__
        if attempt < max(1, retries):
            time.sleep(min(5.0, 2 ** (attempt - 1)))
    return pid, None, last_error


def enrich_profile_fallback_lord_wins(
    heroes: dict[int, dict],
    *,
    db_path: Path,
    domain: str,
    cookies: dict[str, str],
    concurrency: int,
    retries: int,
) -> dict[str, Any]:
    """Fill lord wins for profile fallback from individual achievement pages."""
    missing = [
        pid for pid, hero in heroes.items()
        if parse_int(hero.get("Побед над Владыкой")) is None
    ]
    if not missing:
        return {
            "requested": 0,
            "fetched": 0,
            "reused_previous": 0,
            "missing": 0,
        }

    previous = _previous_lord_wins(db_path)
    fetched = 0
    reused = 0
    errors: dict[str, int] = {}
    timeout_seconds = float(legacy.env_get("LORD_WINS_TIMEOUT_SECONDS", "30"))
    worker_count = max(1, min(int(concurrency), 64))
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = [
            pool.submit(
                _fetch_one_lord_wins,
                pid,
                domain,
                cookies,
                max(1, retries),
                timeout_seconds,
            )
            for pid in missing
        ]
        for index, future in enumerate(futures, 1):
            pid, value, error = future.result()
            if value is not None:
                heroes[pid]["Побед над Владыкой"] = value
                fetched += 1
            elif pid in previous:
                heroes[pid]["Побед над Владыкой"] = previous[pid]
                reused += 1
            else:
                errors[error or "unknown"] = errors.get(error or "unknown", 0) + 1
            if index % 1000 == 0 or index == len(futures):
                LOG.info(
                    "Lord-wins fallback progress %s/%s: fetched=%s, previous=%s, missing=%s",
                    index,
                    len(futures),
                    fetched,
                    reused,
                    index - fetched - reused,
                )

    still_missing = sum(
        1 for hero in heroes.values()
        if parse_int(hero.get("Побед над Владыкой")) is None
    )
    minimum_ratio = float(legacy.env_get("LORD_WINS_MIN_COVERAGE", "0.995"))
    coverage = (len(heroes) - still_missing) / len(heroes) if heroes else 1.0
    if coverage < minimum_ratio:
        raise ApiCollectionError(
            "Profile fallback lord-wins coverage is too low: "
            f"{coverage:.2%} < {minimum_ratio:.2%}; errors={errors}"
        )
    return {
        "requested": len(missing),
        "fetched": fetched,
        "reused_previous": reused,
        "missing": still_missing,
        "coverage": coverage,
        "errors": errors,
    }


def _save_legacy_fallback_snapshot(
    *,
    args: Any,
    db_path: Path,
    cookies: dict[str, str],
    domain: str,
    reason: str,
) -> tuple[Path, Path]:
    """Run the profile collector while preserving the level-5+ dataset contract."""
    if not legacy.check_site_ready(
        domain,
        cookies,
        max_attempts=int(legacy.env_get("SITE_READY_ATTEMPTS", "5")),
        delay_seconds=int(legacy.env_get("SITE_READY_DELAY_SECONDS", "60")),
    ):
        raise ApiCollectionError("Profile fallback site readiness check failed")

    previous_ids, previous_snapshot = load_previous_level5_ids(db_path)
    known_ids, _legacy_baseline, highest_probed = legacy.load_collection_scope(db_path)
    known_names = legacy.load_known_names_from_db(db_path)

    recheck_window = max(
        0,
        int(legacy.env_get("FALLBACK_RECHECK_WINDOW", "1000")),
    )
    probe_count = max(0, int(args.probe_count))
    probe_start = max(1, highest_probed - recheck_window + 1)
    probe_end = highest_probed + probe_count
    ids = sorted(set(known_ids).union(range(probe_start, probe_end + 1)))

    LOG.warning(
        "Starting profile fallback: reason=%s, known_ids=%s, "
        "recheck=%s..%s, new_probe_end=%s, requests=%s",
        reason,
        len(known_ids),
        probe_start,
        highest_probed,
        probe_end,
        len(ids),
    )
    results, failures, achievement_failures = asyncio.run(
        legacy.collect(
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
    filtered = {
        pid: hero
        for pid, hero in results.items()
        if (parse_int(hero.get("Уровень")) or 0) >= SCOPE_MIN_LEVEL
    }
    lord_wins_meta = enrich_profile_fallback_lord_wins(
        filtered,
        db_path=db_path,
        domain=domain,
        cookies=cookies,
        concurrency=args.concurrency,
        retries=max(args.retries, args.achievement_retries),
    )

    retained_count, retained_ratio = _coverage_ratio(set(filtered), previous_ids)
    if previous_ids and retained_ratio < args.min_success_ratio:
        raise ApiCollectionError(
            "Profile fallback rejected: retained previous level-5+ coverage "
            f"{retained_ratio:.2%} is below {args.min_success_ratio:.2%} "
            f"({retained_count}/{len(previous_ids)})"
        )

    snapshot_path, metadata_path = legacy.save_snapshot(
        filtered,
        failures=failures,
        achievement_failures=achievement_failures,
        baseline_ids=previous_ids,
        known_ids=sorted(set(known_ids).union(filtered)),
        probe_start=probe_start,
        probe_end=probe_end,
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata.update(
        {
            "collection_source": "profile_fallback",
            "scope_min_level": SCOPE_MIN_LEVEL,
            "fallback_reason": reason,
            "previous_snapshot": previous_snapshot,
            "previous_level5_count": len(previous_ids),
            "retained_previous_level5_count": retained_count,
            "retained_previous_level5_ratio": retained_ratio,
            "legacy_fallback_used": True,
            "fallback_recheck_window": recheck_window,
            "lord_wins_source": "achievement_pages",
            "lord_wins_collection": lord_wins_meta,
        }
    )
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    LOG.info(
        "Profile fallback saved: players=%s, retained=%.2f%% (%s/%s), "
        "profile_failures=%s, achievement_warnings=%s",
        len(filtered),
        retained_ratio * 100,
        retained_count,
        len(previous_ids),
        len(failures),
        len(achievement_failures),
    )
    return snapshot_path, metadata_path


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
    except ApiCollectionError as exc:
        LOG.error("Cannot start collection: %s", exc)
        write_failure_report(str(exc))
        return 2

    try:
        previous_ids, previous_snapshot = load_previous_level5_ids(db_path)
    except ApiCollectionError as exc:
        LOG.exception("Cannot load the previous level-5+ baseline: %s", exc)
        write_failure_report(str(exc), endpoint=api_endpoint(domain))
        return 2

    # Only failures of the bulk endpoint itself (including an incomplete or
    # structurally invalid payload) are allowed to activate the expensive
    # profile collector. Missing clan/brotherhood fields are not endpoint
    # failures because those memberships are always collected from roster pages.
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

        retained_count, retained_ratio = _coverage_ratio(
            set(collection.heroes),
            previous_ids,
        )
        if previous_ids and retained_ratio < args.min_success_ratio:
            raise ApiCollectionError(
                "API snapshot rejected: retained previous level-5+ coverage "
                f"{retained_ratio:.2%} is below {args.min_success_ratio:.2%} "
                f"({retained_count}/{len(previous_ids)})"
            )
    except ApiCollectionError as endpoint_error:
        LOG.exception(
            "Bulk endpoint is unavailable or unusable; switching to profile fallback: %s",
            endpoint_error,
        )
        try:
            snapshot_path, metadata_path = _save_legacy_fallback_snapshot(
                args=args,
                db_path=db_path,
                cookies=cookies,
                domain=domain,
                reason=str(endpoint_error),
            )
            LOG.info("Saved profile fallback snapshot: %s", snapshot_path)
            LOG.info("Saved profile fallback metadata: %s", metadata_path)
            return 0
        except Exception as fallback_error:
            error = (
                f"Endpoint collection failed: {endpoint_error}; "
                f"profile fallback failed: {fallback_error}"
            )
            LOG.exception(error)
            write_failure_report(error, endpoint=api_endpoint(domain))
            return 2

    # From this point the endpoint has succeeded. Group enrichment is strictly
    # best-effort: no clan/brotherhood failure may prevent saving player data.
    try:
        replace_groups_from_rosters(collection, domain, cookies)
    except Exception as group_error:
        LOG.exception(
            "Unexpected group enrichment failure; saving endpoint player data anyway: %s",
            group_error,
        )
        collection.meta["forglory_group_status"] = "failed_nonfatal"
        collection.meta["forglory_group_errors"] = {
            "unexpected": str(group_error),
        }
        collection.meta["forglory_group_coverage"] = _group_coverage(
            collection.heroes
        )

    try:
        snapshot_path, metadata_path = save_api_snapshot(
            collection,
            previous_ids,
            previous_snapshot,
            retained_count,
            retained_ratio,
        )
    except Exception as snapshot_error:
        error = f"Bulk endpoint succeeded, but snapshot saving failed: {snapshot_error}"
        LOG.exception(error)
        write_failure_report(
            error,
            endpoint=collection.endpoint,
            current_count=len(collection.heroes),
            previous_count=len(previous_ids),
            retained_count=retained_count,
            retained_ratio=retained_ratio,
        )
        return 2

    LOG.info("Saved level-5+ API snapshot: %s", snapshot_path)
    LOG.info("Saved level-5+ API metadata: %s", metadata_path)
    LOG.info(
        "Collection complete from endpoint with best-effort warriors pages: players=%s, "
        "retained previous level-5+=%.2f%% (%s/%s), groups=%s",
        len(collection.heroes),
        retained_ratio * 100,
        retained_count,
        len(previous_ids),
        _group_coverage(collection.heroes),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
