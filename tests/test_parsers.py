from __future__ import annotations

import unittest
from pathlib import Path

from forglory.parsing import parse_hero, parse_kill_beasts, profile_url_matches

FIXTURES = Path(__file__).parent / "fixtures"


class ParserTests(unittest.TestCase):
    def fixture(self, name: str) -> str:
        return (FIXTURES / name).read_text(encoding="utf-8")

    def test_profile_without_clan_or_brotherhood(self) -> None:
        hero = parse_hero(self.fixture("profile_without_groups.html"), 101)
        self.assertEqual(hero["ID"], 101)
        self.assertEqual(hero["Имя"], "Тестовый Игрок")
        self.assertEqual(hero["Слава"], 12345)
        self.assertEqual(hero["clan_id"], 0)
        self.assertEqual(hero["brotherhood_id"], 0)

    def test_profile_without_brotherhood_keeps_clan(self) -> None:
        hero = parse_hero(self.fixture("profile_without_brotherhood.html"), 102)
        self.assertEqual(hero["Клан"], "Север")
        self.assertEqual(hero["clan_id"], 77)
        self.assertEqual(hero["brotherhood_id"], 0)
        self.assertEqual(hero["Награбил (серебро)"], 1500)

    def test_changed_wrapper_uses_conservative_fallback(self) -> None:
        hero = parse_hero(self.fixture("profile_changed_wrapper.html"), 103)
        self.assertEqual(hero["Имя"], "Новая Разметка")
        self.assertEqual(hero["Уровень"], 9)
        self.assertEqual(hero["Сила"], 321)

    def test_confirmation_label_before_real_name_is_ignored(self) -> None:
        html = """
        <html><body>
          <p class="text-center text-xl">Подтверждение</p>
          <div id="profile">
            <p class="text-center text-xl">Настоящий Ник</p>
          </div>
          <div id="stats">
            <div class="grid grid-cols-profileStat"><span></span><span>Уровень: 35</span></div>
            <div class="grid grid-cols-profileStat"><span></span><span>Слава: 42 710</span></div>
          </div>
        </body></html>
        """
        hero = parse_hero(html, 187)
        self.assertEqual(hero["Имя"], "Настоящий Ник")
        self.assertEqual(hero["Уровень"], 35)
        self.assertEqual(hero["Слава"], 42710)

    def test_confirmation_page_without_real_profile_is_rejected(self) -> None:
        html = """
        <html><body>
          <p class="text-center text-xl">Подтверждение</p>
          <div id="stats"></div>
        </body></html>
        """
        with self.assertRaisesRegex(ValueError, "profile_name_not_found|profile_stats_not_found"):
            parse_hero(html, 187)

    def test_broken_achievement_is_optional(self) -> None:
        self.assertIsNone(parse_kill_beasts(self.fixture("achievements_broken.html"), 103))

    def test_valid_achievement(self) -> None:
        # level * (4*level - 2) / 2 + current = 4*14/2 + 3 = 31
        self.assertEqual(parse_kill_beasts(self.fixture("achievements_valid.html"), 103), 31)

    def test_redirect_validation(self) -> None:
        self.assertTrue(profile_url_matches("https://playwekings.mobi/hero/detail?player=17", 17))
        self.assertFalse(profile_url_matches("https://playwekings.mobi/hero/profile", 17))
        self.assertFalse(profile_url_matches("https://playwekings.mobi/login", 17))


if __name__ == "__main__":
    unittest.main()
