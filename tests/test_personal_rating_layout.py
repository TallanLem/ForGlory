from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path

import forglory


ROOT = Path(__file__).resolve().parents[1]


class PersonalRatingLayoutTests(unittest.TestCase):
    def test_personal_stats_adds_rank_in_best_growth_rating_fallback(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(
                """
                CREATE TABLE snapshots(
                    snapshot_id INTEGER PRIMARY KEY,
                    ts INTEGER NOT NULL
                );
                CREATE TABLE best_growth(
                    best_for_snapshot_id INTEGER NOT NULL,
                    param TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    diff INTEGER NOT NULL
                );
                INSERT INTO snapshots VALUES(10,1000);
                INSERT INTO best_growth VALUES(10,'Слава',2,20);
                INSERT INTO best_growth VALUES(10,'Слава',7,20);
                INSERT INTO best_growth VALUES(10,'Слава',9,15);
                INSERT INTO best_growth VALUES(10,'Побед',7,4);
                INSERT INTO best_growth VALUES(10,'Побед',3,5);
                """
            )

            def original_query(pid: int, _snap_from: str, _snap_to: str):
                return {
                    "pid": pid,
                    "rows": [
                        {"param": "Слава", "best_diff": 20},
                        {"param": "Побед", "best_diff": 4},
                        {"param": "Поражений", "best_diff": None},
                    ],
                }

            namespace = {
                "query_personal_stats": original_query,
                "get_db": lambda: conn,
            }
            self.assertTrue(forglory._patch_personal_stats_query(namespace))

            result = namespace["query_personal_stats"](7, "from", "to")
            ranks = {row["param"]: row["best_rank"] for row in result["rows"]}
            self.assertEqual(ranks["Слава"], 2)
            self.assertEqual(ranks["Побед"], 2)
            self.assertIsNone(ranks["Поражений"])
        finally:
            conn.close()

    def test_personal_stats_uses_batched_queries(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(
                """
                CREATE TABLE snapshots(
                    snapshot_id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL UNIQUE,
                    ts INTEGER NOT NULL
                );
                CREATE TABLE text_values(
                    text_id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL,
                    norm TEXT NOT NULL
                );
                CREATE TABLE observations(
                    snapshot_id INTEGER NOT NULL,
                    pid INTEGER NOT NULL,
                    name_id INTEGER,
                    level INTEGER,
                    glory INTEGER,
                    wins INTEGER,
                    strength INTEGER,
                    defense INTEGER,
                    dexterity INTEGER,
                    mastery INTEGER,
                    vitality INTEGER,
                    PRIMARY KEY(snapshot_id,pid)
                );
                CREATE TABLE best_growth(
                    best_for_snapshot_id INTEGER NOT NULL,
                    param TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    diff INTEGER NOT NULL,
                    best_snapshot_id INTEGER NOT NULL
                );
                INSERT INTO snapshots VALUES(1,'heroes_2026-08-01_20-00-00.json.gz',100);
                INSERT INTO snapshots VALUES(2,'heroes_2026-08-02_20-00-00.json.gz',200);
                INSERT INTO text_values VALUES(1,'Player One','player one');
                INSERT INTO text_values VALUES(2,'Player Two','player two');
                INSERT INTO text_values VALUES(3,'Player Three','player three');

                INSERT INTO observations VALUES(1,1,1,10,10,2,1,1,1,1,1);
                INSERT INTO observations VALUES(1,2,2,10,20,4,2,2,2,2,2);
                INSERT INTO observations VALUES(1,3,3,10,5,1,3,3,3,3,3);
                INSERT INTO observations VALUES(2,1,1,11,25,7,2,2,2,2,2);
                INSERT INTO observations VALUES(2,2,2,10,30,6,3,3,3,3,3);
                INSERT INTO observations VALUES(2,3,3,10,15,2,4,4,4,4,4);

                INSERT INTO best_growth VALUES(2,'Слава',2,20,2);
                INSERT INTO best_growth VALUES(2,'Слава',1,15,2);
                INSERT INTO best_growth VALUES(2,'Слава',3,10,2);
                INSERT INTO best_growth VALUES(2,'Побед',1,5,2);
                INSERT INTO best_growth VALUES(2,'Побед',2,2,2);
                INSERT INTO best_growth VALUES(2,'Побед',3,1,2);
                INSERT INTO best_growth VALUES(2,'Сумма статов',1,5,2);
                INSERT INTO best_growth VALUES(2,'Сумма статов',2,5,2);
                INSERT INTO best_growth VALUES(2,'Сумма статов',3,5,2);
                """
            )

            def snapshot_info(filename: str):
                row = conn.execute(
                    "SELECT snapshot_id,ts FROM snapshots WHERE filename=?",
                    (filename,),
                ).fetchone()
                return (int(row[0]), int(row[1])) if row else None

            def row_value(row, param: str):
                if param == "Сумма статов":
                    return sum(int(row[key]) for key in (
                        "strength", "defense", "dexterity", "mastery", "vitality"
                    ))
                return int(row[{"Слава": "glory", "Побед": "wins"}[param]])

            def player_value_expr(param: str, alias: str):
                if param == "Сумма статов":
                    return "(" + "+".join(
                        f"{alias}.{key}" for key in (
                            "strength", "defense", "dexterity", "mastery", "vitality"
                        )
                    ) + ")"
                column = {"Слава": "glory", "Побед": "wins"}[param]
                return f"{alias}.{column}"

            def original_query(*_args, **_kwargs):
                raise AssertionError("The original N+1 query must not be called")

            namespace = {
                "query_personal_stats": original_query,
                "get_db": lambda: conn,
                "snapshot_info": snapshot_info,
                "_row_value": row_value,
                "_player_value_expr": player_value_expr,
                "PERSONAL_PARAMS": ["Слава", "Побед", "Сумма статов"],
                "cached_query": lambda function: function,
            }
            self.assertTrue(forglory._patch_personal_stats_query(namespace))

            statements: list[str] = []
            conn.set_trace_callback(
                lambda sql: statements.append(sql)
                if sql.lstrip().upper().startswith(("SELECT", "WITH"))
                else None
            )
            result = namespace["query_personal_stats"](
                1,
                "heroes_2026-08-01_20-00-00.json.gz",
                "heroes_2026-08-02_20-00-00.json.gz",
            )
            conn.set_trace_callback(None)

            self.assertIsNotNone(result)
            self.assertEqual(result["name"], "Player One")
            rows = {row["param"]: row for row in result["rows"]}
            self.assertEqual(rows["Слава"]["overall_rank"], 2)
            self.assertEqual(rows["Слава"]["growth_rank"], 1)
            self.assertEqual(rows["Слава"]["best_rank"], 2)
            self.assertEqual(rows["Сумма статов"]["delta"], 5)
            self.assertLessEqual(len(statements), 6)
        finally:
            conn.close()

    def test_profile_suggestions_are_limited_and_include_level(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(
                """
                CREATE TABLE snapshots(snapshot_id INTEGER PRIMARY KEY,ts INTEGER NOT NULL);
                CREATE TABLE text_values(
                    text_id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL,
                    norm TEXT NOT NULL
                );
                CREATE TABLE observations(
                    snapshot_id INTEGER NOT NULL,
                    pid INTEGER NOT NULL,
                    name_id INTEGER NOT NULL,
                    level INTEGER
                );
                INSERT INTO snapshots VALUES(1,100);
                INSERT INTO snapshots VALUES(2,200);
                INSERT INTO text_values VALUES(1,'Player Alpha','player alpha');
                INSERT INTO text_values VALUES(2,'Player Beta','player beta');
                INSERT INTO text_values VALUES(3,'Player Gamma','player gamma');
                INSERT INTO text_values VALUES(4,'Player Delta','player delta');
                INSERT INTO observations VALUES(2,1,1,11);
                INSERT INTO observations VALUES(2,2,2,12);
                INSERT INTO observations VALUES(2,3,3,13);
                INSERT INTO observations VALUES(2,4,4,14);
                """
            )

            class FakeApp:
                def __init__(self):
                    self.view_functions = {
                        "api_player_suggest_all": lambda: ["legacy"]
                    }

            class FakeRequest:
                args = {"q": "player"}

            flask_app = FakeApp()
            namespace = {
                "app": flask_app,
                "api_player_suggest_all": flask_app.view_functions["api_player_suggest_all"],
                "get_db": lambda: conn,
                "normalize_name": lambda value: " ".join(value.casefold().split()),
                "_db_available": lambda: True,
                "request": FakeRequest(),
                "jsonify": lambda payload: payload,
            }
            self.assertTrue(forglory._patch_player_suggestion_route(namespace))

            payload = flask_app.view_functions["api_player_suggest_all"]()
            self.assertEqual(len(payload), 3)
            self.assertEqual(payload[0], {"name": "Player Alpha", "level": 11})
            self.assertTrue(all("name" in item and "level" in item for item in payload))
        finally:
            conn.close()

    def test_profile_template_uses_compact_controls_and_suggestions(self) -> None:
        template = (ROOT / "templates" / "profile.html").read_text(encoding="utf-8")
        self.assertNotIn("<th>Было</th>", template)
        self.assertNotIn("<th>Стало</th>", template)
        self.assertNotIn("Лучшее за 30 дней", template)
        self.assertNotIn('class="profile-period"', template)
        self.assertNotIn('list="profile-nicknames"', template)
        self.assertNotIn("table-scroll-hint", template)
        self.assertIn("Общее<br><small>(место)</small>", template)
        self.assertIn("Прирост<br><small>(место)</small>", template)
        self.assertIn("Лучшее<br><small>(место)</small>", template)
        self.assertIn('data-label="Общее (место)"', template)
        self.assertIn("row.best_rank", template)
        self.assertIn('id="profile-suggestions"', template)
        self.assertIn("items.slice(0, 3)", template)
        self.assertIn('className = "profile-suggestion-level"', template)
        self.assertIn('id="personal-table-controls"', template)
        self.assertIn('id="personal-scroll-left"', template)
        self.assertIn('id="personal-scroll-right"', template)

    def test_profile_css_places_controls_below_table(self) -> None:
        css = (ROOT / "static" / "profile-fixes.css").read_text(encoding="utf-8")
        self.assertIn(".table-scroll-controls", css)
        self.assertIn(".table-scroll-button", css)
        self.assertIn(".profile-suggestion", css)
        self.assertIn("font-size: 0.76em", css)
        self.assertNotIn("position: absolute;\n  top: 50%", css)


if __name__ == "__main__":
    unittest.main()
