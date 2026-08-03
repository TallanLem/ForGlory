from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from forglory.counter_math import (
    cumulative_delta32,
    sql_cumulative_delta32,
    unwrap_cumulative_counter,
)
from tools.normalize_counter_wraps import normalize_counter_history
from tools.rebuild_best_growth_safe import rebuild_best_growth


RATING_COLUMNS = (
    "glory INTEGER,wins INTEGER,losses INTEGER,"
    "dragon_wins INTEGER,snake_wins INTEGER,lord_wins INTEGER,"
    "beasts_killed INTEGER,strength INTEGER,defense INTEGER,"
    "dexterity INTEGER,mastery INTEGER,vitality INTEGER,"
    "rob_silver INTEGER,lost_silver INTEGER,"
    "rob_crystals INTEGER,lost_crystals INTEGER"
)


class CounterMathTests(unittest.TestCase):
    def test_adjacent_delta_repairs_only_backward_rollover(self) -> None:
        self.assertEqual(
            cumulative_delta32(4_308_602_068, 0),
            4_308_602_068,
        )
        self.assertEqual(cumulative_delta32(0, 4_308_602_068), -13_634_772)

    def test_normal_delta_is_unchanged(self) -> None:
        self.assertEqual(cumulative_delta32(44_427_528, 40_000_000), 4_427_528)
        self.assertEqual(cumulative_delta32(10, 20), -10)
        self.assertIsNone(cumulative_delta32(None, 20))

    def test_sql_delta_matches_python(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            expression = sql_cumulative_delta32("current_value", "previous_value")
            row = conn.execute(
                f"SELECT {expression} FROM "
                "(SELECT ? AS current_value, ? AS previous_value)",
                (0, 4_308_602_068),
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], -13_634_772)

    def test_unwrap_chooses_continuous_representation(self) -> None:
        self.assertEqual(
            unwrap_cumulative_counter(4_308_602_068, 0),
            4_308_602_068,
        )
        self.assertEqual(unwrap_cumulative_counter(0, 4_103_436_672), 4_294_967_296)
        self.assertEqual(
            unwrap_cumulative_counter(4_308_602_068, 4_294_967_296),
            4_308_602_068,
        )


class CounterHistoryNormalizationTests(unittest.TestCase):
    def test_history_repair_makes_all_plain_rating_differences_correct(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                f"""
                CREATE TABLE snapshots(
                    snapshot_id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    ts INTEGER NOT NULL
                );
                CREATE TABLE observations(
                    snapshot_id INTEGER NOT NULL,
                    pid INTEGER NOT NULL,
                    level INTEGER,
                    {RATING_COLUMNS},
                    PRIMARY KEY(snapshot_id,pid)
                );
                """
            )
            conn.executemany(
                "INSERT INTO snapshots(snapshot_id,filename,ts) VALUES(?,?,?)",
                (
                    (1, "before", 1_000_000),
                    (2, "wrapped", 1_086_400),
                    (3, "after", 1_172_800),
                ),
            )
            base = [44, 1, 100, 10, 0, 0, 0, 0, 1, 1, 1, 1, 1]
            columns = (
                "snapshot_id,pid,level,glory,wins,losses,dragon_wins,"
                "snake_wins,lord_wins,beasts_killed,strength,defense,"
                "dexterity,mastery,vitality,rob_silver,lost_silver,"
                "rob_crystals,lost_crystals"
            )
            placeholders = ",".join("?" for _ in range(19))
            conn.executemany(
                f"INSERT INTO observations({columns}) VALUES({placeholders})",
                (
                    (1, 2041, *base, 4_103_436_672, 0, 0, 0),
                    (2, 2041, *base, 0, 0, 0, 0),
                    (3, 2041, *base, 4_308_602_068, 0, 0, 0),
                ),
            )

            changes = normalize_counter_history(conn)
            conn.commit()
            values = conn.execute(
                "SELECT rob_silver FROM observations "
                "WHERE pid=2041 ORDER BY snapshot_id"
            ).fetchall()
            short_delta = conn.execute(
                "SELECT c.rob_silver-p.rob_silver "
                "FROM observations c JOIN observations p ON p.pid=c.pid "
                "WHERE c.snapshot_id=3 AND p.snapshot_id=2 AND c.pid=2041"
            ).fetchone()[0]
            long_delta = conn.execute(
                "SELECT c.rob_silver-p.rob_silver "
                "FROM observations c JOIN observations p ON p.pid=c.pid "
                "WHERE c.snapshot_id=3 AND p.snapshot_id=1 AND c.pid=2041"
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(changes, {"rob_silver": 1})
        self.assertEqual(
            [row[0] for row in values],
            [4_103_436_672, 4_294_967_296, 4_308_602_068],
        )
        self.assertEqual(short_delta, 13_634_772)
        self.assertEqual(long_delta, 205_165_396)


class BestGrowthRebuildTests(unittest.TestCase):
    def test_rebuild_removes_false_four_billion_growth(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ratings.sqlite"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    f"""
                    CREATE TABLE snapshots(
                        snapshot_id INTEGER PRIMARY KEY,
                        filename TEXT NOT NULL,
                        ts INTEGER NOT NULL
                    );
                    CREATE TABLE players(
                        pid INTEGER PRIMARY KEY,
                        visible_from_snapshot_id INTEGER
                    );
                    CREATE TABLE observations(
                        snapshot_id INTEGER NOT NULL,
                        pid INTEGER NOT NULL,
                        level INTEGER,
                        {RATING_COLUMNS},
                        PRIMARY KEY(snapshot_id,pid)
                    );
                    CREATE TABLE best_growth(
                        best_for_snapshot_id INTEGER NOT NULL,
                        param TEXT NOT NULL,
                        pid INTEGER NOT NULL,
                        level INTEGER,
                        diff INTEGER NOT NULL,
                        best_snapshot_id INTEGER NOT NULL,
                        PRIMARY KEY(best_for_snapshot_id,param,pid)
                    );
                    """
                )
                conn.executemany(
                    "INSERT INTO snapshots(snapshot_id,filename,ts) VALUES(?,?,?)",
                    (
                        (1, "before", 1_000_000),
                        (2, "wrapped", 1_086_400),
                        (3, "after", 1_172_800),
                    ),
                )
                conn.executemany(
                    "INSERT INTO players(pid,visible_from_snapshot_id) VALUES(?,?)",
                    ((1, 1), (2, 1)),
                )
                base = [44, 1, 100, 10, 0, 0, 0, 0, 1, 1, 1, 1, 1]
                columns = (
                    "snapshot_id,pid,level,glory,wins,losses,dragon_wins,"
                    "snake_wins,lord_wins,beasts_killed,strength,defense,"
                    "dexterity,mastery,vitality,rob_silver,lost_silver,"
                    "rob_crystals,lost_crystals"
                )
                placeholders = ",".join("?" for _ in range(19))
                conn.executemany(
                    f"INSERT INTO observations({columns}) VALUES({placeholders})",
                    (
                        (1, 1, *base, 4_103_436_672, 0, 0, 0),
                        (2, 1, *base, 0, 0, 0, 0),
                        (3, 1, *base, 4_308_602_068, 0, 0, 0),
                        (1, 2, *base, 10_000_000, 0, 0, 0),
                        (2, 2, *base, 30_000_000, 0, 0, 0),
                        (3, 2, *base, 74_000_000, 0, 0, 0),
                    ),
                )
                normalize_counter_history(conn)
                latest_sid, pairs, candidates = rebuild_best_growth(conn)
                conn.commit()
                rows = conn.execute(
                    "SELECT pid,diff FROM best_growth "
                    "WHERE param='Награбил (серебро)' "
                    "ORDER BY diff DESC,pid"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(latest_sid, 3)
            self.assertEqual(pairs, 2)
            self.assertEqual(candidates, 0)
            self.assertEqual(rows, [(1, 191_530_624), (2, 44_000_000)])
            # The latest daily growth of player 1 is 13,634,772; the former
            # false 4.3-billion value is absent from the rebuilt table.
            self.assertNotIn((1, 4_308_602_068), rows)


if __name__ == "__main__":
    unittest.main()
