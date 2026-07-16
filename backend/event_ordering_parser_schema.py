"""Schema + lightweight validation for Event Ordering parser v1."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class ParserTimezone:
    source_timezone: str = ''
    display_timezone: str = ''
    preferred_labels: List[str] = field(default_factory=list)
    default_label: str = ''


@dataclass
class ParserNormalization:
    trim: bool = True
    collapse_spaces: bool = True
    dash_unify: bool = True
    separator_unify: bool = True


@dataclass
class PatternDef:
    id: str = ''
    pattern: str = ''
    timezone_label: Optional[str] = None
    priority: int = 0
    flags: str = ''


@dataclass
class ParserDatetime:
    required: bool = True
    candidate_patterns: List[PatternDef] = field(default_factory=list)
    selection_policy: str = 'highest_priority_first_match'


@dataclass
class ParserFieldConfig:
    extractors: Dict[str, List[PatternDef]] = field(default_factory=dict)
    cleanup: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ParserIdentity:
    fields: List[str] = field(default_factory=list)
    exclude_fields: List[str] = field(default_factory=lambda: ['order'])
    fallback_order: List[List[str]] = field(default_factory=list)


@dataclass
class ParserNaming:
    template: str = '{base_name}'
    missing_field_policy: str = 'empty_string'


@dataclass
class ParserQuality:
    min_confidence: float = 0.45
    require_datetime_for_ordering: bool = True


@dataclass
class ParserConfigV1:
    version: int = 1
    timezone: ParserTimezone = field(default_factory=ParserTimezone)
    normalization: ParserNormalization = field(default_factory=ParserNormalization)
    datetime: ParserDatetime = field(default_factory=ParserDatetime)
    fields: ParserFieldConfig = field(default_factory=ParserFieldConfig)
    identity: ParserIdentity = field(default_factory=ParserIdentity)
    naming: ParserNaming = field(default_factory=ParserNaming)
    quality: ParserQuality = field(default_factory=ParserQuality)


DEFAULT_PARSER_V1 = asdict(ParserConfigV1())


def _load_pattern_defs(raw_list: Any) -> List[PatternDef]:
    if not isinstance(raw_list, list):
        return []
    out: List[PatternDef] = []
    for item in raw_list:
        if isinstance(item, dict):
            out.append(PatternDef(
                id=str(item.get('id', '')),
                pattern=str(item.get('pattern', '')),
                timezone_label=item.get('timezone_label'),
                priority=int(item.get('priority', 0) or 0),
                flags=str(item.get('flags', '')),
            ))
    return out


def validate_parser_v1(raw: Dict[str, Any]) -> Tuple[bool, str]:
    if not isinstance(raw, dict):
        return False, 'parser must be an object'

    if int(raw.get('version', 0) or 0) != 1:
        return False, 'parser.version must be 1'

    dt = raw.get('datetime', {})
    cps = dt.get('candidate_patterns', []) if isinstance(dt, dict) else []
    parsed = _load_pattern_defs(cps)

    if dt.get('required', True) and not parsed:
        return False, 'parser.datetime.candidate_patterns must contain at least one pattern'

    for p in parsed:
        if not p.pattern:
            return False, 'each datetime candidate pattern must include a non-empty pattern'

    identity = raw.get('identity', {})
    if identity and not isinstance(identity.get('fields', []), list):
        return False, 'parser.identity.fields must be an array'

    fields = raw.get('fields', {})
    extractors = fields.get('extractors', {}) if isinstance(fields, dict) else {}
    if extractors and not isinstance(extractors, dict):
        return False, 'parser.fields.extractors must be an object'

    return True, ''


def ensure_channel_parser_defaults(channel_cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(channel_cfg or {})
    parser = out.get('parser')
    if parser is None:
        return out

    merged = asdict(ParserConfigV1())

    def merge(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
        for k, v in src.items():
            if isinstance(v, dict) and isinstance(dst.get(k), dict):
                merge(dst[k], v)
            else:
                dst[k] = v
        return dst

    out['parser'] = merge(merged, parser if isinstance(parser, dict) else {})
    return out
