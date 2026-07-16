#!/usr/bin/env python3
"""Behavior tests for the Event Ordering parser.v1 path.

These exercise EventOrderingService through its public parser interface
(parse_event_time_with_parser) so they describe *what* the parser does, not
how. They are the safety net that lets the legacy single-regex builder in the
frontend (generateRegex) be retired.
"""

import os
import sys
import unittest
from datetime import datetime, timezone

# Add backend to path (repo convention — no conftest.py)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from event_ordering_service import EventOrderingService


# Parser config matching the iso_start candidate from
# docs/EVENT_ORDERING_PARSER_SCHEMA_V1.md (JS-style named groups).
ISO_START_PARSER = {
    "datetime": {
        "candidate_patterns": [
            {
                "id": "iso_start",
                "pattern": (
                    r"start:(?<year>\d{4})-(?<month>\d{1,2})-(?<day>\d{1,2})"
                    r"\s+(?<hour>\d{1,2}):(?<minute>\d{2})(?::(?<second>\d{2}))?"
                ),
                "priority": 100,
            }
        ]
    }
}


class TestEventOrderingParserDatetime(unittest.TestCase):
    def setUp(self):
        self.service = EventOrderingService()

    def test_iso_start_datetime_is_extracted_as_utc_from_source_timezone(self):
        """A stream name's embedded start time is parsed to the correct UTC instant.

        The source timezone is Europe/London and the date is in July, so London
        is on BST (UTC+1): 19:30 local must come back as 18:30 UTC.
        """
        stream_name = (
            "UFC 03: UFC FIGHT NIGHT: RED CORNER VS BLUE CORNER "
            "start:2026-07-01 19:30:00"
        )

        result = self.service.parse_event_time_with_parser(
            stream_name, ISO_START_PARSER, timezone_str="Europe/London"
        )

        self.assertIsNotNone(result, "expected a datetime, got None")
        self.assertIsNotNone(result.tzinfo, "expected a timezone-aware datetime")
        self.assertEqual(
            result.astimezone(timezone.utc),
            datetime(2026, 7, 1, 18, 30, 0, tzinfo=timezone.utc),
        )


if __name__ == "__main__":
    unittest.main()
