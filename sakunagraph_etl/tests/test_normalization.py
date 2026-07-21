from datetime import date
import unittest

import polars as pl

from sakunagraph_etl.transform.helpers import normalize_datetime, to_int


class NormalizationTests(unittest.TestCase):
    def test_integer_normalization_handles_commas_labels_and_blanks(self) -> None:
        frame = pl.DataFrame({"value": ["1,234 persons", "-", None]})

        normalized = to_int(frame, ["value"])

        self.assertEqual(normalized["value"].to_list(), [1234, None, None])

    def test_date_normalization_forward_fills_report_dates(self) -> None:
        frame = pl.DataFrame({"reported": ["17 July 2026", None]})

        normalized = normalize_datetime(
            frame,
            date_col="reported",
            time_col=None,
            datetime_formats=[],
            date_formats=["%d %B %Y"],
            new_col="reportedAt",
        )

        self.assertEqual(
            normalized["reportedAt"].to_list(),
            [date(2026, 7, 17), date(2026, 7, 17)],
        )


if __name__ == "__main__":
    unittest.main()
