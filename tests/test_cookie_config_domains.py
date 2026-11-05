from __future__ import annotations

import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import collect_api_first as collector


class CookieRoutingTests(unittest.TestCase):
    @staticmethod
    def _mock_env(records, preferred=""):
        payload = json.dumps(records)
        return patch.object(
            collector.legacy,
            "env_get",
            side_effect=lambda key, default="": (
                payload if key == "COOKIES_JSON"
                else preferred if key == "WK_DOMAIN"
                else default
            ),
        )

    def test_versioned_session_cookie_is_accepted(self):
        with self._mock_env([
            {"domain": "wekings.online", "name": "device_id", "value": "dev"},
            {"domain": "wekings.online", "name": "wekings_session_v2_20260729_https", "value": "v2"},
        ]):
            cookies, domain = collector.load_cookie_config()
        self.assertEqual(domain, "https://wekings.online/")
        self.assertEqual(cookies["wekings_session_v2_20260729_https"], "v2")
        self.assertEqual(cookies["device_id"], "dev")

    def test_legacy_session_cookie_still_works(self):
        with self._mock_env([
            {"domain": ".playwekings.mobi", "name": "wekings_session", "value": "old"},
        ]):
            cookies, domain = collector.load_cookie_config()
        self.assertEqual(domain, "https://playwekings.mobi/")
        self.assertEqual(cookies, {"wekings_session": "old"})

    def test_mixed_domain_cookies_are_never_combined(self):
        with self._mock_env([
            {"domain": "playwekings.mobi", "name": "wekings_session", "value": "mobi"},
            {"domain": "playwekings.mobi", "name": "mobi_only", "value": "one"},
            {"domain": "wekings.online", "name": "wekings_session_v2_20260729_https", "value": "online"},
            {"domain": "wekings.online", "name": "online_only", "value": "two"},
        ]):
            candidates = collector._cookie_configs()
        self.assertEqual(
            [domain for _, domain in candidates],
            ["https://wekings.online/", "https://playwekings.mobi/"],
        )
        self.assertNotIn("mobi_only", candidates[0][0])
        self.assertNotIn("online_only", candidates[1][0])

    def test_cookie_wrapper_format(self):
        with self._mock_env({"cookies": [
            {"domain": "wekings.online", "name": "wekings_session_v2", "value": "session"}
        ]}):
            _, domain = collector.load_cookie_config()
        self.assertEqual(domain, "https://wekings.online/")

    def test_preferred_domain_takes_priority(self):
        with self._mock_env([
            {"domain": "wekings.online", "name": "wekings_session_v2", "value": "online"},
            {"domain": "playwekings.mobi", "name": "wekings_session", "value": "mobi"},
        ], preferred="playwekings.mobi"):
            cookies, domain = collector.load_cookie_config()
        self.assertEqual(domain, "https://playwekings.mobi/")
        self.assertEqual(cookies["wekings_session"], "mobi")
        self.assertNotIn("wekings_session_v2", cookies)

    def test_missing_session_is_rejected(self):
        with self._mock_env([
            {"domain": "wekings.online", "name": "device_id", "value": "only_device"}
        ]):
            with self.assertRaisesRegex(collector.ApiCollectionError, "session cookie"):
                collector.load_cookie_config()

    def test_invalid_domain_is_rejected(self):
        with self._mock_env([
            {"domain": "https://evil.site/path", "name": "wekings_session_v2", "value": "x"}
        ]):
            with self.assertRaises(collector.ApiCollectionError):
                collector.load_cookie_config()

    def test_fallback_uses_secondary_domain_before_profile_collection(self):
        mock_args = SimpleNamespace(db_path="none.sqlite", min_success_ratio=0.995)
        result = collector.ApiCollection(
            heroes={10: {"ID": 10, "Имя": "Player", "Уровень": 10}},
            endpoint="https://wekings.online/heroes/for-glory",
            attempts_used=1,
            meta={},
        )
        with (
            patch.object(collector.legacy, "parse_args", return_value=mock_args),
            patch.object(collector, "load_cookie_config", return_value=(
                {"wekings_session": "mobi"}, "https://playwekings.mobi/"
            )),
            patch.object(collector, "_other_cookie_configs", return_value=[
                ({"wekings_session_v2": "online"}, "https://wekings.online/"),
            ]),
            patch.object(collector, "load_previous_level5_ids", return_value=({10}, "previous")),
            patch.object(collector, "fetch_from_bulk_api", side_effect=[
                collector.ApiCollectionError("DNS unavailable"), result,
            ]) as fetch,
            patch.object(collector, "replace_groups_from_rosters") as groups,
            patch.object(collector, "save_api_snapshot", return_value=(
                Path("snapshot.json.gz"), Path("snapshot.meta.json")
            )),
            patch.object(collector, "_save_legacy_fallback_snapshot") as fallback,
        ):
            exit_code = collector.main()
        self.assertEqual(exit_code, 0)
        self.assertEqual(fetch.call_count, 2)
        groups.assert_called_once()
        self.assertEqual(groups.call_args.args[1], "https://wekings.online/")
        self.assertEqual(groups.call_args.args[2]["wekings_session_v2"], "online")
        fallback.assert_not_called()


if __name__ == "__main__":
    unittest.main()
