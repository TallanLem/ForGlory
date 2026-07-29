from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from collect_api_first import (
    GroupRoster,
    apply_group_rosters,
    load_known_group_ids,
    parse_group_roster_html,
    scan_group_rosters,
)


class GroupRosterScanTests(unittest.TestCase):
    def test_clan_roster_page(self) -> None:
        html = """
        <html><body><main>
          <p class="group-header">Воины клана *ГРЕШНЫЕ*</p>
          <a class="hero-link" href="/hero/detail?player=52">MARSHMALLOW</a>
          <a class="hero-link" href="/hero/detail?player=523">Sindragos</a>
        </main></body></html>
        """
        roster = parse_group_roster_html(html, group_kind="clan", group_id=6)
        self.assertIsNotNone(roster)
        assert roster is not None
        self.assertEqual(roster.name, "*ГРЕШНЫЕ*")
        self.assertEqual(roster.members, frozenset({52, 523}))

    def test_brotherhood_name_may_contain_spaces(self) -> None:
        html = """
        <p class="group-header">Воины братства CHINAZES SAUNTRES</p>
        <a href="https://playwekings.mobi/hero/detail?player=74">drakulessa</a>
        """
        roster = parse_group_roster_html(
            html,
            group_kind="brotherhood",
            group_id=100,
        )
        self.assertIsNotNone(roster)
        assert roster is not None
        self.assertEqual(roster.name, "CHINAZES SAUNTRES")
        self.assertEqual(roster.members, frozenset({74}))

    def test_game_error_page_is_missing_group(self) -> None:
        html = "<html><title>Викинги</title><body>Что-то пошло не так.</body></html>"
        self.assertIsNone(
            parse_group_roster_html(html, group_kind="clan", group_id=500)
        )

    def test_roster_assignment_overlays_without_erasing_endpoint_values(self) -> None:
        heroes = {
            52: {"ID": 52, "Клан": "не состоит", "clan_id": 0},
            523: {"ID": 523, "Клан": "старое", "clan_id": 999},
            9000: {"ID": 9000, "Клан": "Endpoint clan", "clan_id": 777},
        }
        roster = GroupRoster(6, "*ГРЕШНЫЕ*", frozenset({52, 523}))
        result = apply_group_rosters(heroes, "clan", [roster])
        self.assertEqual(result["members_assigned"], 2)
        self.assertEqual((heroes[52]["Клан"], heroes[52]["clan_id"]), ("*ГРЕШНЫЕ*", 6))
        self.assertEqual((heroes[523]["Клан"], heroes[523]["clan_id"]), ("*ГРЕШНЫЕ*", 6))
        self.assertEqual((heroes[9000]["Клан"], heroes[9000]["clan_id"]), ("Endpoint clan", 777))

    def test_duplicate_membership_is_nonfatal(self) -> None:
        heroes = {52: {"ID": 52, "Клан": "не состоит", "clan_id": 0}}
        result = apply_group_rosters(
            heroes,
            "clan",
            [
                GroupRoster(6, "Первый", frozenset({52})),
                GroupRoster(8, "Второй", frozenset({52})),
            ],
        )
        self.assertEqual(result["duplicate_members"], 1)
        self.assertEqual((heroes[52]["Клан"], heroes[52]["clan_id"]), ("Первый", 6))

    def test_known_ids_come_from_last_snapshot_with_positive_groups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ratings.sqlite"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE observations(
                        snapshot_id INTEGER,
                        clan_game_id INTEGER,
                        brotherhood_game_id INTEGER
                    );
                    INSERT INTO observations VALUES(1,6,100);
                    INSERT INTO observations VALUES(1,8,101);
                    INSERT INTO observations VALUES(2,0,0);
                    """
                )
                conn.commit()
            finally:
                conn.close()

            self.assertEqual(load_known_group_ids(db_path, "clan"), [6, 8])
            self.assertEqual(load_known_group_ids(db_path, "brotherhood"), [100, 101])

    def test_only_known_ids_and_next_window_are_scanned(self) -> None:
        pages = {
            6: '<p class="group-header">Воины клана Шестой</p><a href="/hero/detail?player=52">A</a>',
            8: '<p class="group-header">Воины клана Восьмой</p><a href="/hero/detail?player=74">B</a>',
        }
        missing = "<html><body>Что-то пошло не так.</body></html>"

        class Response:
            def __init__(self, text: str, url: str, status_code: int = 200) -> None:
                self.text = text
                self.url = url
                self.status_code = status_code

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    raise requests.HTTPError(f"HTTP {self.status_code}")

        class Session:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def __enter__(self):
                self.headers = {}
                self.cookies = {}
                return self

            def __exit__(self, *_args):
                return False

            def get(self, url: str, **_kwargs):
                self.calls.append(url)
                group_id = int(url.rsplit("=", 1)[1])
                return Response(pages.get(group_id, missing), url)

        session = Session()
        with patch("collect_api_first.requests.Session", return_value=session), patch(
            "collect_api_first.time.sleep", return_value=None
        ):
            rosters = scan_group_rosters(
                "https://playwekings.mobi/",
                {},
                "clan",
                timeout_seconds=1,
                attempts=3,
                retry_delay_seconds=0,
                known_group_ids=[6],
                discovery_window=4,
            )

        self.assertEqual(
            [(item.group_id, item.name) for item in rosters],
            [(6, "Шестой"), (8, "Восьмой")],
        )
        self.assertFalse(any(url.endswith("id=1") for url in session.calls))
        self.assertTrue(any(url.endswith("id=10") for url in session.calls))
        self.assertFalse(any(url.endswith("id=11") for url in session.calls))

    def test_http_500_for_one_known_id_does_not_abort_scan(self) -> None:
        valid = '<p class="group-header">Воины клана Шестой</p><a href="/hero/detail?player=52">A</a>'
        missing = "<html><body>Что-то пошло не так.</body></html>"

        class Response:
            def __init__(self, text: str, url: str, status_code: int = 200) -> None:
                self.text = text
                self.url = url
                self.status_code = status_code

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    raise requests.HTTPError(f"HTTP {self.status_code}")

        class Session:
            def __enter__(self):
                self.headers = {}
                self.cookies = {}
                return self

            def __exit__(self, *_args):
                return False

            def get(self, url: str, **_kwargs):
                group_id = int(url.rsplit("=", 1)[1])
                if group_id == 7:
                    return Response("server error", url, 500)
                if group_id == 6:
                    return Response(valid, url)
                return Response(missing, url)

        with patch("collect_api_first.requests.Session", return_value=Session()), patch(
            "collect_api_first.time.sleep", return_value=None
        ):
            rosters = scan_group_rosters(
                "https://playwekings.mobi/",
                {},
                "clan",
                timeout_seconds=1,
                attempts=3,
                retry_delay_seconds=0,
                known_group_ids=[6, 7],
                discovery_window=3,
            )

        self.assertEqual([(item.group_id, item.name) for item in rosters], [(6, "Шестой")])

    def test_discovery_stops_at_max_known_plus_fifty(self) -> None:
        missing = "<html><body>Что-то пошло не так.</body></html>"

        class Response:
            def __init__(self, url: str) -> None:
                self.text = missing
                self.url = url
                self.status_code = 200

            def raise_for_status(self) -> None:
                return None

        class Session:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def __enter__(self):
                self.headers = {}
                self.cookies = {}
                return self

            def __exit__(self, *_args):
                return False

            def get(self, url: str, **_kwargs):
                self.calls.append(url)
                return Response(url)

        session = Session()
        with patch("collect_api_first.requests.Session", return_value=session), patch(
            "collect_api_first.time.sleep", return_value=None
        ):
            scan_group_rosters(
                "https://playwekings.mobi/",
                {},
                "brotherhood",
                timeout_seconds=1,
                attempts=1,
                retry_delay_seconds=0,
                known_group_ids=[100, 400],
                discovery_window=50,
            )

        requested = {int(url.rsplit("=", 1)[1]) for url in session.calls}
        self.assertIn(100, requested)
        self.assertIn(400, requested)
        self.assertIn(401, requested)
        self.assertIn(450, requested)
        self.assertNotIn(1, requested)
        self.assertNotIn(451, requested)


if __name__ == "__main__":
    unittest.main()
