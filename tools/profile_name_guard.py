from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any


KNOWN_INTERFACE_NAMES = {
    "подтверждение",
    "confirmation",
    "подтвердить",
    "confirm",
}


def normalize_name(value: Any) -> str:
    return " ".join(str(value or "").casefold().split())


def load_snapshot(path: Path) -> dict[str, dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise RuntimeError("Snapshot root must be an object")
    return data


def validate_snapshot(path: Path) -> int:
    data = load_snapshot(path)
    names = Counter(
        normalize_name(hero.get("Имя") or hero.get("имя") or hero.get("name") or hero.get("nick"))
        for hero in data.values()
        if isinstance(hero, dict)
    )
    names.pop("", None)
    total = len(data)
    if total == 0 or not names:
        raise RuntimeError("Snapshot has no player names")

    name, count = names.most_common(1)[0]
    ratio = count / total
    known_bad_count = sum(names.get(value, 0) for value in KNOWN_INTERFACE_NAMES)

    if known_bad_count >= max(5, int(total * 0.01)):
        raise RuntimeError(
            f"Snapshot rejected: interface label used as player name "
            f"for {known_bad_count}/{total} profiles"
        )
    if total >= 100 and ratio >= 0.50:
        raise RuntimeError(
            f"Snapshot rejected: one name {name!r} appears "
            f"for {count}/{total} profiles ({ratio:.1%})"
        )

    print(
        f"OK: snapshot name distribution; players={total}, "
        f"unique_names={len(names)}, most_common={count} ({ratio:.2%})"
    )
    return 0


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


def repair_database(path: Path, optional: bool = False, recent: int = 30) -> int:
    if not path.exists():
        if optional:
            print(f"Database not found, skipping repair: {path}")
            return 0
        raise FileNotFoundError(path)

    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        if not all(table_exists(conn, name) for name in ("snapshots", "observations", "text_values")):
            raise RuntimeError("Unsupported database schema")

        snapshots = conn.execute(
            "SELECT snapshot_id,filename,ts FROM snapshots ORDER BY ts DESC LIMIT ?",
            (max(1, recent),),
        ).fetchall()
        repaired_total = 0

        for snapshot_id, filename, snapshot_ts in snapshots:
            total = int(
                conn.execute(
                    "SELECT COUNT(*) FROM observations WHERE snapshot_id=?",
                    (snapshot_id,),
                ).fetchone()[0]
            )
            if total < 100:
                continue

            dominant = conn.execute(
                """
                SELECT o.name_id,n.value,n.norm,COUNT(*) AS cnt
                FROM observations o
                LEFT JOIN text_values n ON n.text_id=o.name_id
                WHERE o.snapshot_id=?
                GROUP BY o.name_id,n.value,n.norm
                ORDER BY cnt DESC
                LIMIT 1
                """,
                (snapshot_id,),
            ).fetchone()
            if not dominant:
                continue

            bad_name_id, bad_value, bad_norm, bad_count = dominant
            bad_count = int(bad_count)
            ratio = bad_count / total
            known_interface = normalize_name(bad_norm or bad_value) in KNOWN_INTERFACE_NAMES
            if ratio < 0.90 and not (known_interface and bad_count >= max(5, int(total * 0.01))):
                continue

            previous = conn.execute(
                "SELECT snapshot_id FROM snapshots WHERE ts<? ORDER BY ts DESC",
                (snapshot_ts,),
            ).fetchall()
            if not previous:
                print(f"WARNING: cannot repair first snapshot {filename}")
                continue

            remaining = bad_count
            repaired_snapshot = 0
            for (previous_id,) in previous:
                cursor = conn.execute(
                    """
                    UPDATE observations
                    SET name_id=(
                        SELECT p.name_id
                        FROM observations p
                        WHERE p.snapshot_id=?
                          AND p.pid=observations.pid
                          AND p.name_id IS NOT ?
                    )
                    WHERE snapshot_id=?
                      AND name_id IS ?
                      AND EXISTS(
                          SELECT 1
                          FROM observations p
                          WHERE p.snapshot_id=?
                            AND p.pid=observations.pid
                            AND p.name_id IS NOT ?
                      )
                    """,
                    (
                        previous_id,
                        bad_name_id,
                        snapshot_id,
                        bad_name_id,
                        previous_id,
                        bad_name_id,
                    ),
                )
                changed = max(0, int(cursor.rowcount))
                repaired_snapshot += changed
                remaining -= changed
                if remaining <= 0:
                    break

            if repaired_snapshot:
                repaired_total += repaired_snapshot
                print(
                    f"Repaired {repaired_snapshot}/{bad_count} names in {filename}: "
                    f"{bad_value!r} ({ratio:.1%})"
                )
            if remaining > 0:
                print(
                    f"WARNING: {remaining} names in {filename} could not be restored "
                    f"from earlier snapshots"
                )

        conn.commit()
        check = conn.execute("PRAGMA quick_check").fetchone()[0]
        if check != "ok":
            raise RuntimeError(f"SQLite quick_check failed: {check}")
        print(f"OK: database name repair complete; repaired={repaired_total}")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate collected player names and repair corrupted name snapshots."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate-snapshot")
    validate.add_argument("--snapshot", required=True)

    repair = subparsers.add_parser("repair-db")
    repair.add_argument("--db", required=True)
    repair.add_argument("--optional", action="store_true")
    repair.add_argument("--recent", type=int, default=30)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "validate-snapshot":
            return validate_snapshot(Path(args.snapshot))
        return repair_database(Path(args.db), optional=args.optional, recent=args.recent)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
