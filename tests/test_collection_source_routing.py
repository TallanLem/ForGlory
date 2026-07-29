from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from collect_api_first import (
    ApiCollection,
    ApiCollectionError,
    GroupRoster,
    main,
    replace_groups_from_rosters,
)


class CollectionSourceRoutingTests(unittest.TestCase):
    def _args(self, db_path: Path) -> SimpleNamespace:
        return SimpleNamespace(
            db_path=str(db_path),
            min_success_ratio=0.995,
            probe_count=300,
            concurrency=10,
            retries=2,
            achievement_retries=1,
            systemic_failure_sample_size=200,
        )

    def _collection(self) -> ApiCollection:
        return ApiCollection(
            heroes={
                10: {
                    "ID": 10,
                    "Имя": "Player",
                    "Уровень": 10,
                    "Клан": "Endpoint clan",
                    "clan_id": 999,
                    "Братство": "Endpoint brotherhood",
                    "brotherhood_id": 999,
                }
            },
            endpoint="https://playwekings.mobi/heroes/for-glory",
            attempts_used=1,
            meta={},
        )

    def test_endpoint_success_uses_rosters_and_never_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = self._args(Path(tmp) / "ratings.sqlite")
            collection = self._collection()
            with patch("collect_api_first.legacy.parse_args", return_value=args, create=True), patch(
                "collect_api_first.load_cookie_config",
                return_value=({"wekings_session": "secret"}, "https://playwekings.mobi/"),
            ), patch(
                "collect_api_first.load_previous_level5_ids", return_value=({10}, "previous")
            ), patch(
                "collect_api_first.fetch_from_bulk_api", return_value=collection
            ), patch(
                "collect_api_first.replace_groups_from_rosters"
            ) as roster_scan, patch(
                "collect_api_first.save_api_snapshot",
                return_value=(Path("snapshot.json.gz"), Path("snapshot.meta.json")),
            ), patch(
                "collect_api_first._save_legacy_fallback_snapshot"
            ) as profile_fallback:
                result = main()

            self.assertEqual(result, 0)
            roster_scan.assert_called_once()
            profile_fallback.assert_not_called()

    def test_roster_failure_after_endpoint_success_is_nonfatal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = self._args(Path(tmp) / "ratings.sqlite")
            collection = self._collection()
            with patch("collect_api_first.legacy.parse_args", return_value=args, create=True), patch(
                "collect_api_first.load_cookie_config",
                return_value=({"wekings_session": "secret"}, "https://playwekings.mobi/"),
            ), patch(
                "collect_api_first.load_previous_level5_ids", return_value=({10}, "previous")
            ), patch(
                "collect_api_first.fetch_from_bulk_api", return_value=collection
            ), patch(
                "collect_api_first.replace_groups_from_rosters",
                side_effect=ApiCollectionError("roster failed"),
            ), patch(
                "collect_api_first.save_api_snapshot",
                return_value=(Path("snapshot.json.gz"), Path("snapshot.meta.json")),
            ) as save_snapshot, patch(
                "collect_api_first.write_failure_report"
            ) as failure_report, patch(
                "collect_api_first._save_legacy_fallback_snapshot"
            ) as profile_fallback:
                result = main()

            self.assertEqual(result, 0)
            save_snapshot.assert_called_once()
            failure_report.assert_not_called()
            profile_fallback.assert_not_called()
            self.assertEqual(
                collection.meta["forglory_group_status"],
                "failed_nonfatal",
            )
            self.assertEqual(
                collection.meta["forglory_group_errors"],
                {"unexpected": "roster failed"},
            )

    def test_endpoint_failure_uses_profile_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = self._args(Path(tmp) / "ratings.sqlite")
            with patch("collect_api_first.legacy.parse_args", return_value=args, create=True), patch(
                "collect_api_first.load_cookie_config",
                return_value=({"wekings_session": "secret"}, "https://playwekings.mobi/"),
            ), patch(
                "collect_api_first.load_previous_level5_ids", return_value=({10}, "previous")
            ), patch(
                "collect_api_first.fetch_from_bulk_api",
                side_effect=ApiCollectionError("endpoint unavailable"),
            ), patch(
                "collect_api_first.replace_groups_from_rosters"
            ) as roster_scan, patch(
                "collect_api_first._save_legacy_fallback_snapshot",
                return_value=(Path("snapshot.json.gz"), Path("snapshot.meta.json")),
            ) as profile_fallback:
                result = main()

            self.assertEqual(result, 0)
            roster_scan.assert_not_called()
            profile_fallback.assert_called_once()

    def test_rosters_replace_even_valid_endpoint_group_fields(self) -> None:
        collection = self._collection()
        clan = GroupRoster(6, "Roster clan", frozenset({10}))
        brotherhood = GroupRoster(100, "Roster brotherhood", frozenset({10}))

        def scan(_domain, _cookies, kind, **_kwargs):
            return [clan] if kind == "clan" else [brotherhood]

        with patch("collect_api_first.scan_group_rosters", side_effect=scan):
            replace_groups_from_rosters(
                collection,
                "https://playwekings.mobi/",
                {"wekings_session": "secret"},
            )

        hero = collection.heroes[10]
        self.assertEqual((hero["Клан"], hero["clan_id"]), ("Roster clan", 6))
        self.assertEqual(
            (hero["Братство"], hero["brotherhood_id"]),
            ("Roster brotherhood", 100),
        )
        self.assertEqual(
            collection.meta["forglory_group_source"],
            "warriors_pages_best_effort",
        )


if __name__ == "__main__":
    unittest.main()
