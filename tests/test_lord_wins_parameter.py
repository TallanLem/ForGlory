from __future__ import annotations

import unittest

import app as app_module
from forglory.schema import PARAM_TO_COLUMN


class LordWinsParameterTests(unittest.TestCase):
    def test_rating_parameter_is_inserted_after_serpent_wins(self) -> None:
        index = app_module.param_options.index("Побед над Змеем")
        self.assertEqual(app_module.param_options[index + 1], "Побед над Владыкой")
        personal_index = app_module.PERSONAL_PARAMS.index("Побед над Змеем")
        self.assertEqual(
            app_module.PERSONAL_PARAMS[personal_index + 1],
            "Побед над Владыкой",
        )
        self.assertEqual(PARAM_TO_COLUMN["Побед над Владыкой"], "lord_wins")


if __name__ == "__main__":
    unittest.main()
