from __future__ import annotations

import unittest

from collect_api_first import normalise_api_hero


class EndpointGroupFieldsTests(unittest.TestCase):
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
            "achievements": {"lord_wins": 0},
        }

    def test_current_endpoint_nested_achievements_are_supported(self) -> None:
        row = self.base_row()
        row["achievements"] = {"lord_wins": 321}
        _, hero = normalise_api_hero(row, 1)
        self.assertEqual(hero["Побед над Владыкой"], 321)

    def test_flat_snake_case_groups(self) -> None:
        row = self.base_row()
        row.update(
            clan_id=701,
            clan_name="Клан 701",
            brotherhood_id=801,
            brotherhood_name="Братство 801",
        )
        pid, hero = normalise_api_hero(row, 1)
        self.assertEqual(pid, 123)
        self.assertEqual(hero["Клан"], "Клан 701")
        self.assertEqual(hero["clan_id"], 701)
        self.assertEqual(hero["Братство"], "Братство 801")
        self.assertEqual(hero["brotherhood_id"], 801)

    def test_nested_groups_still_work(self) -> None:
        row = self.base_row()
        row["clan"] = {"id": 702, "name": "Клан 702"}
        row["brotherhood"] = {"id": 802, "name": "Братство 802"}
        _, hero = normalise_api_hero(row, 1)
        self.assertEqual((hero["Клан"], hero["clan_id"]), ("Клан 702", 702))
        self.assertEqual(
            (hero["Братство"], hero["brotherhood_id"]),
            ("Братство 802", 802),
        )

    def test_missing_groups_are_not_memberships(self) -> None:
        _, hero = normalise_api_hero(self.base_row(), 1)
        self.assertEqual((hero["Клан"], hero["clan_id"]), ("не состоит", 0))
        self.assertEqual(
            (hero["Братство"], hero["brotherhood_id"]),
            ("не состоит", 0),
        )


if __name__ == "__main__":
    unittest.main()
