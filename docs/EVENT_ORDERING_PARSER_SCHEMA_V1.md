# Event Ordering Parser Schema v1

This document defines a backend-first, user-configurable parser schema for event ordering, overflow grouping, and channel renaming.

## Goals

- No hardcoded sport/source formats
- No single giant regex requirement
- Parsing logic lives in backend
- Same parsed output used by ordering, overflow, and naming
- Persisted config per channel

---

## Config Shape (per channel)

```json
{
  "parser": {
    "version": 1,
    "timezone": {
      "source_timezone": "Europe/London",
      "display_timezone": "Europe/Paris",
      "preferred_labels": ["UK", "ET", "US"],
      "default_label": "UK"
    },
    "normalization": {
      "trim": true,
      "collapse_spaces": true,
      "dash_unify": true,
      "separator_unify": true
    },
    "datetime": {
      "required": true,
      "candidate_patterns": [
        {
          "id": "iso_start",
          "pattern": "start:(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2})\\s+(?<hour>\\d{1,2}):(?<minute>\\d{2})(?::(?<second>\\d{2}))?",
          "timezone_label": null,
          "priority": 100
        }
      ],
      "selection_policy": "highest_priority_first_match"
    },
    "fields": {
      "extractors": {
        "league": [
          { "pattern": "^(?<league>[A-Za-z]+)\\b", "priority": 50 }
        ],
        "order": [
          { "pattern": "^[A-Za-z]+\\s*(?<order>\\d+)", "priority": 50 }
        ],
        "event_title": [
          { "pattern": "^[^:]+:\\s*(?<event_title>.*?)(?:\\s+start:|\\s+//|\\||$)", "priority": 90 },
          { "pattern": "^(?<event_title>.*)$", "priority": 1 }
        ],
        "team1": [
          { "pattern": "(?<team1>.*?)\\s+(?:vs\\.?|v|@)\\s+.*", "flags": "i", "priority": 80 }
        ],
        "team2": [
          { "pattern": ".*\\s+(?:vs\\.?|v|@)\\s+(?<team2>.*?)(?:\\s+\\||\\s+//|\\s+start:|$)", "flags": "i", "priority": 80 }
        ]
      },
      "cleanup": {
        "team_trim_tokens": ["PRELIMS", "EARLY PRELIMS"],
        "strip_parentheses": false
      }
    },
    "identity": {
      "fields": ["league", "event_title", "year", "month", "day", "hour", "minute"],
      "exclude_fields": ["order"],
      "fallback_order": [
        ["league", "team1", "team2", "year", "month", "day", "hour", "minute"],
        ["league", "event_title", "year", "month", "day", "hour", "minute"],
        ["league", "event_title"]
      ]
    },
    "naming": {
      "template": "{base_name} | {league} {order} | {event_time}",
      "missing_field_policy": "empty_string"
    },
    "quality": {
      "min_confidence": 0.45,
      "require_datetime_for_ordering": true
    }
  }
}
```

---

## Backend Parsing Contract

For each stream, backend returns parsed artifacts:

```json
{
  "stream_id": 75092,
  "name": "UFC 03: ...",
  "event_time": "2026-03-01T01:55:00",
  "datetime_candidate_id": "iso_start",
  "fields": {
    "league": "UFC",
    "order": "03",
    "event_title": "UFC FIGHT NIGHT: RED CORNER VS BLUE CORNER",
    "team1": "RED CORNER",
    "team2": "BLUE CORNER",
    "year": "2026",
    "month": "03",
    "day": "01",
    "hour": "01",
    "minute": "55",
    "second": "00"
  },
  "identity_key": "league=ufc|event_title=ufc fight night: red corner vs blue corner|year=2026|month=03|day=01|hour=01|minute=55",
  "confidence": 0.88,
  "warnings": []
}
```

---

## How Ordering Uses It

- Upcoming/past split uses parsed `event_time`
- Sort uses `event_time` then optional `order`
- Group backups by `identity_key` + `event_time` bucket

## How Overflow Uses It

- Conflicts detected by same time slot (minute precision)
- Keep first event group; move the rest
- Move all streams sharing the same `identity_key` together

## How Channel Renaming Uses It

- Template variables come from `fields` + computed time/date/timezone
- Works identically on main and overflow channels

---

## Migration Note

Current `pattern` can be deprecated once `parser.version=1` is present for a channel.

Recommended migration sequence:

1. Add parser editor in UI
2. Persist parser object per channel
3. Route preview/order/overflow/rename to parser path
4. Remove legacy single-pattern path
