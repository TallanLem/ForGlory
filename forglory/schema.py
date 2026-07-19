from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class NumericField:
    json_key: str
    column: str
    aliases: tuple[str, ...] = ()

    @property
    def all_keys(self) -> tuple[str, ...]:
        return (self.json_key, *self.aliases)


NUMERIC_FIELDS: tuple[NumericField, ...] = (
    NumericField("Уровень", "level", ("level", "уровень")),
    NumericField("Слава", "glory", ("glory",)),
    NumericField("Побед", "wins", ("wins",)),
    NumericField("Поражений", "losses", ("losses",)),
    NumericField("Побед над Драконом", "dragon_wins", ("dragon_wins",)),
    NumericField("Побед над Змеем", "snake_wins", ("snake_wins",)),
    NumericField("Убито зверей", "beasts_killed", ("beasts_killed",)),
    NumericField("Сила", "strength", ("strength",)),
    NumericField("Защита", "defense", ("defense",)),
    NumericField("Ловкость", "dexterity", ("dexterity",)),
    NumericField("Мастерство", "mastery", ("mastery",)),
    NumericField("Живучесть", "vitality", ("vitality",)),
    NumericField("Награбил (серебро)", "rob_silver", ("rob_silver",)),
    NumericField("Потерял (серебро)", "lost_silver", ("lost_silver",)),
    NumericField("Награбил (кристаллы)", "rob_crystals", ("rob_crystals",)),
    NumericField("Потерял (кристаллы)", "lost_crystals", ("lost_crystals",)),
    NumericField("Время в походе", "expedition_time"),
    NumericField("Выполнено заданий", "quests_completed"),
    NumericField("Добыто кристаллов", "crystals_mined"),
    NumericField("Друзей", "friends"),
    NumericField("Отправлено подарков", "gifts_sent"),
    NumericField("Поймано рыб", "fish_caught"),
    NumericField("Растений выращено", "plants_grown"),
    NumericField("Убито гоблинов", "goblins_killed"),
    NumericField("Убито драконов", "dragons_killed"),
    NumericField("Убито змеев", "snakes_killed"),
    NumericField("Чат", "chat"),
)

FIELD_BY_JSON_KEY = {field.json_key: field for field in NUMERIC_FIELDS}
FIELD_BY_COLUMN = {field.column: field for field in NUMERIC_FIELDS}

STAT_COLUMNS = ("strength", "defense", "dexterity", "mastery", "vitality")

PARAM_TO_COLUMN: dict[str, str | None] = {
    "Слава": "glory",
    "Побед": "wins",
    "Поражений": "losses",
    "Побед над Драконом": "dragon_wins",
    "Побед над Змеем": "snake_wins",
    "Убито зверей": "beasts_killed",
    "Сила": "strength",
    "Защита": "defense",
    "Ловкость": "dexterity",
    "Мастерство": "mastery",
    "Живучесть": "vitality",
    "Сумма статов": None,
    "Награбил (серебро)": "rob_silver",
    "Потерял (серебро)": "lost_silver",
    "Награбил (кристаллы)": "rob_crystals",
    "Потерял (кристаллы)": "lost_crystals",
}

BEST_PARAMS = tuple(PARAM_TO_COLUMN)


def parse_int(value: Any) -> int | None:
    """Convert game counters to int without turning missing values into zero."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip().replace("\xa0", "").replace(" ", "")
    if not text or text in {"—", "-", "нет", "None", "null"}:
        return None
    text = text.replace(",", ".")
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def pick_numeric(hero: dict[str, Any], field: NumericField) -> int | None:
    for key in field.all_keys:
        if key in hero:
            return parse_int(hero.get(key))
    return None


def pick_text(hero: dict[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = hero.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None
