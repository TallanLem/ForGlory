from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from .schema import parse_int


# Prefer selectors scoped to the actual profile. The generic selector is kept
# only as a fallback for older page layouts.
PROFILE_NAME_SELECTORS = (
    ("[data-player-name]", True),
    ("#profile [data-player-name]", True),
    ("#profile p.text-xl", True),
    ("#profile p.text-center.text-xl", True),
    ("p.text-center.text-xl", False),
)

# These are interface/interstitial labels, not player names. On 2026-07-24 the
# site added "Подтверждение" before the real profile name while reusing the same
# CSS classes as the old name element.
PROFILE_UI_LABELS = {
    "подтверждение",
    "confirmation",
    "подтвердить",
    "confirm",
}


def normalize_profile_text(value: str) -> str:
    return " ".join(value.casefold().split())


def _find_profile_name(soup: BeautifulSoup) -> str | None:
    for selector, trusted in PROFILE_NAME_SELECTORS:
        for tag in soup.select(selector):
            value = tag.get("data-player-name") or tag.get_text(" ", strip=True)
            if not value:
                continue
            candidate = str(value).strip()
            if not candidate:
                continue
            if not trusted and normalize_profile_text(candidate) in PROFILE_UI_LABELS:
                continue
            return candidate
    return None


def parse_hero(html: str, hero_id: int) -> dict:
    """Parse one profile page. Missing optional blocks stay missing, not zeroed."""
    soup = BeautifulSoup(html, "html.parser")
    name = _find_profile_name(soup)
    if not name:
        raise ValueError("profile_name_not_found")

    data: dict[str, object] = {"ID": hero_id, "Имя": name}
    stat_blocks = soup.select("div#stats div.grid.grid-cols-profileStat")
    if not stat_blocks:
        # Tolerate harmless wrapper/class changes while requiring the same label/value structure.
        stat_blocks = soup.select("#stats [data-profile-stat], #stats .profile-stat")

    for block in stat_blocks:
        spans = block.find_all("span", recursive=False)
        if len(spans) < 2:
            spans = block.find_all("span", recursive=True)
        if len(spans) < 2:
            continue

        icon = spans[0].find("img")
        content = spans[1]
        content_text = content.get_text(" ", strip=True)

        if "Клан:" in content_text:
            link = content.find("a", href=re.compile(r"/clan/info\?id=\d+"))
            if link:
                match = re.search(r"id=(\d+)", link.get("href", ""))
                data["Клан"] = link.get_text(" ", strip=True)
                data["clan_id"] = int(match.group(1)) if match else 0
            else:
                data["Клан"] = "не состоит"
                data["clan_id"] = 0
            continue

        if "Братство:" in content_text:
            link = content.find("a", href=re.compile(r"/brotherhood/info\?id=\d+"))
            if link:
                match = re.search(r"id=(\d+)", link.get("href", ""))
                data["Братство"] = link.get_text(" ", strip=True)
                data["brotherhood_id"] = int(match.group(1)) if match else 0
            else:
                data["Братство"] = "не состоит"
                data["brotherhood_id"] = 0
            continue

        if ":" not in content_text:
            continue
        key, raw_value = map(str.strip, content_text.split(":", 1))

        if key in ("Награбил", "Потерял") and icon and icon.get("src"):
            src = str(icon["src"])
            if "silver" in src:
                key += " (серебро)"
            elif "crystal" in src:
                key += " (кристаллы)"

        numeric = parse_int(raw_value)
        data[key] = numeric if numeric is not None else raw_value.strip()

    # A confirmation/interstitial page may contain generic text but no real
    # profile counters. Require the level and at least one additional numeric
    # profile value, while keeping compatibility with partial test/legacy pages.
    numeric_profile_keys = (
        "Слава", "Побед", "Поражений", "Сила", "Защита",
        "Ловкость", "Мастерство", "Живучесть",
    )
    numeric_values = sum(
        isinstance(data.get(key), int) for key in numeric_profile_keys
    )
    if not stat_blocks or data.get("Уровень") is None or numeric_values < 1:
        raise ValueError("profile_stats_not_found")

    # These fields are genuinely zero when the page omits them in the current game layout.
    data.setdefault("Чат", 0)
    return data


def parse_kill_beasts(html: str, _hero_id: int | None = None) -> int | None:
    """Return the beast-kill counter or None when the achievement block is unavailable."""
    soup = BeautifulSoup(html, "html.parser")
    achievements = soup.select("div.flex.flex-col.p-2.leading-5")
    for achievement in achievements:
        name_tag = achievement.select_one("div.font-bold.item-header.pb-1")
        if not name_tag or name_tag.get_text(" ", strip=True) != "Повелитель Зверей":
            continue
        text = achievement.get_text(" ", strip=True)
        status_match = re.search(r"(\d[\d\s\xa0]*)\s+из\s+(\d[\d\s\xa0]*)", text)
        level_match = re.search(r"(?:уровень|ур\.?|lvl)\s*[:№]?\s*(\d+)", text, re.IGNORECASE)
        if not level_match:
            bold_numbers = [parse_int(tag.get_text(" ", strip=True)) for tag in achievement.select("b.font-semibold")]
            level = next((number for number in reversed(bold_numbers) if number is not None), None)
        else:
            level = int(level_match.group(1))
        if not status_match or level is None:
            return None
        current = parse_int(status_match.group(1))
        if current is None:
            return None
        return level * (4 * level - 2) // 2 + current
    return None


def profile_url_matches(final_url: str, expected_hero_id: int) -> bool:
    """Accept canonical redirects only when they still point to the requested profile."""
    parsed = urlparse(final_url)
    if not parsed.path.rstrip("/").endswith("/hero/detail"):
        return False
    values = parse_qs(parsed.query).get("player", [])
    return bool(values and values[0] == str(expected_hero_id))
