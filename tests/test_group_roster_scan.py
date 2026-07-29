from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from collect_api_first import (
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

    def test_game_error_page_terminates_scan(self) -> None:
        html = "<html><title>Викинги</title><body>Что-то пошло не так.</body></html>"
        self.assertIsNone(
            parse_group_roster_html(html, group_kind="clan", group_id=500)
        )

    def test_roster_assignment_uses_real_game_id(self) -> None:
        heroes = {
            52: {"ID": 52, "Клан": "не состоит", "clan_id": 0},
            523: {"ID": 523, "Клан": "старое", "clan_id": 999},
            9000: {"ID": 9000, "Клан": "старое", "clan_id": 999},
        }
        roster = parse_group_roster_html(
            """
            <p class="group-header">Воины клана *ГРЕШНЫЕ*</p>
            <a href="/hero/detail?player=52">A</a>
            <a href="/hero/detail?player=523">B</a>
            """,
            group_kind="clan",
            group_id=6,
        )
        assert roster is not None
        result = apply_group_rosters(heroes, "clan", [roster])
        self.assertEqual(result["members_assigned"], 2)
        self.assertEqual((heroes[52]["Клан"], heroes[52]["clan_id"]), ("*ГРЕШНЫЕ*", 6))
        self.assertEqual((heroes[523]["Клан"], heroes[523]["clan_id"]), ("*ГРЕШНЫЕ*", 6))
        self.assertEqual((heroes[9000]["Клан"], heroes[9000]["clan_id"]), ("не состоит", 0))

    def test_scan_retries_game_error_page_before_stopping(self) -> None:
        valid = """
        <p class="group-header">Воины клана Первый</p>
        <a href="/hero/detail?player=52">A</a>
        """
        missing = "<html><body>Что-то пошло не так.</body></html>"

        class Response:
            def __init__(self, text: str, url: str) -> None:
                self.text = text
                self.url = url
                self.status_code = 200

            def raise_for_status(self) -> None:
                return None

        class Session:
            calls: list[str] = []

            def __enter__(self):
                self.headers = {}
                self.cookies = {}
                return self

            def __exit__(self, *_args):
                return False

            def get(self, url: str, **_kwargs):
                self.calls.append(url)
                group_id = int(url.rsplit("=", 1)[1])
                if group_id == 1 and self.calls.count(url) == 1:
                    return Response(missing, url)
                if group_id == 1:
                    return Response(valid, url)
                return Response(missing, url)

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
                maximum_id=10,
            )

        self.assertEqual([(item.group_id, item.name) for item in rosters], [(1, "Первый")])
        self.assertEqual(sum(url.endswith("id=1") for url in session.calls), 2)
        self.assertEqual(sum(url.endswith("id=2") for url in session.calls), 4)

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

    def test_leading_missing_ids_do_not_end_bootstrap_scan(self) -> None:
        valid = """
        <p class="group-header">Воины клана Шестой</p>
        <a href="/hero/detail?player=52">A</a>
        """
        missing = "<html><body>Что-то пошло не так.</body></html>"

        class Response:
            def __init__(self, text: str, url: str) -> None:
                self.text = text
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
                group_id = int(url.rsplit("=", 1)[1])
                return Response(valid if group_id == 6 else missing, url)

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
                maximum_id=20,
                bootstrap_missing_limit=10,
            )

        self.assertEqual([(item.group_id, item.name) for item in rosters], [(6, "Шестой")])
        self.assertEqual(sum(url.endswith("id=1") for url in session.calls), 1)
        self.assertEqual(sum(url.endswith("id=6") for url in session.calls), 1)
        self.assertEqual(sum(url.endswith("id=7") for url in session.calls), 3)

    def test_sparse_historical_ids_are_refreshed_before_tail_discovery(self) -> None:
        pages = {
            6: '<p class="group-header">Воины клана Шестой</p><a href="/hero/detail?player=52">A</a>',
            8: '<p class="group-header">Воины клана Восьмой</p><a href="/hero/detail?player=74">B</a>',
        }
        missing = "<html><body>Что-то пошло не так.</body></html>"

        class Response:
            def __init__(self, text: str, url: str) -> None:
                self.text = text
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
                attempts=2,
                retry_delay_seconds=0,
                maximum_id=20,
                known_group_ids=[6, 8],
            )

        self.assertEqual(
            [(item.group_id, item.name) for item in rosters],
            [(6, "Шестой"), (8, "Восьмой")],
        )
        self.assertFalse(any(url.endswith("id=1") for url in session.calls))
        self.assertEqual(sum(url.endswith("id=9") for url in session.calls), 2)


if __name__ == "__main__":
    unittest.main()
