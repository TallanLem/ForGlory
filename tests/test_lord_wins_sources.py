from __future__ import annotations

import unittest

from collect_api_first import (
    ApiCollectionError,
    normalise_api_hero,
    parse_lord_wins_from_achievements,
)


class LordWinsSourceTests(unittest.TestCase):
    def base_row(self) -> dict:
        return {
            "id": 123,
            "nickname": "Tester",
            "level": 10,
            "glory": 100,
            "wins": 10,
            "losses": 2,
            "dragon_wins": 3,
            "serpent_wins": 4,
            "strength": 5,
            "defense": 6,
            "agility": 7,
            "mastery": 8,
            "vitality": 9,
            "silver_looted": 11,
            "silver_lost": 12,
            "crystals_looted": 13,
            "crystals_lost": 14,
            "beasts_killed": 15,
        }

    def test_nested_endpoint_lord_wins_is_required_and_read(self) -> None:
        row = self.base_row()
        row["achievements"] = {"lord_wins": 140}
        _, hero = normalise_api_hero(row, 1)
        self.assertEqual(hero["Побед над Владыкой"], 140)

    def test_endpoint_without_lord_wins_is_rejected(self) -> None:
        with self.assertRaises(ApiCollectionError):
            normalise_api_hero(self.base_row(), 1)

    def test_achievement_embedded_json(self) -> None:
        html = '<script>window.hero={"lord_wins":4824};</script>'
        self.assertEqual(parse_lord_wins_from_achievements(html), 4824)

    def test_achievement_visible_card(self) -> None:
        html = (
            '<div class="achievement"><div>Побед над Владыкой</div>'
            '<div>140 из 200</div></div>'
        )
        self.assertEqual(parse_lord_wins_from_achievements(html), 140)


if __name__ == "__main__":
    unittest.main()
