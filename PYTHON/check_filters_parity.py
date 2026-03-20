#!/usr/bin/env python3
"""
Vérifie la parité des filtres entre:
- HTML/offres.html
- HTML/filtres.html

Usage:
    python PYTHON/check_filters_parity.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OFFRES = ROOT / "HTML" / "offres.html"
FILTRES = ROOT / "HTML" / "filtres.html"


BLOCKS = [
    ("const", "FRENCH_REGIONS_LOOKUP"),
    ("const", "CITY_NORMALIZATION"),
    ("const", "CITY_TO_COUNTRY"),
    ("const", "COUNTRY_NORMALIZATION"),
    ("const", "ITALY_CITY_TO_REGION"),
    ("const", "CITY_TO_PARENT_REGION"),
    ("const", "cityToRegion"),
    ("const", "FAMILY_GROUPS"),
    ("const", "CONTINENT_GROUPS"),
    ("function", "normalizeCountryForFilter"),
    ("function", "normalizeCityForFilter"),
    ("function", "deduplicateRegionLevel"),
    ("function", "getCityRegion"),
]


def _read(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {path}")
    return path.read_text(encoding="utf-8")


def _extract_const(source: str, name: str) -> str | None:
    pattern = re.compile(rf"\bconst\s+{re.escape(name)}\s*=\s*", re.M)
    m = pattern.search(source)
    if not m:
        return None
    start = m.start()
    i = m.end()
    depth = 0
    in_str: str | None = None
    in_line_comment = False
    in_block_comment = False
    escaped = False
    while i < len(source):
        ch = source[i]
        nxt = source[i + 1] if i + 1 < len(source) else ""
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_str:
                in_str = None
        else:
            if ch == "/" and nxt == "/":
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue
            if ch in ("'", '"', "`"):
                in_str = ch
            elif ch in "{[(":
                depth += 1
            elif ch in "}])":
                depth = max(0, depth - 1)
            elif ch == ";" and depth == 0:
                return source[start : i + 1]
        i += 1
    return None


def _extract_function(source: str, name: str) -> str | None:
    pattern = re.compile(rf"\bfunction\s+{re.escape(name)}\s*\(", re.M)
    m = pattern.search(source)
    if not m:
        return None
    start = m.start()
    brace_pos = source.find("{", m.end())
    if brace_pos == -1:
        return None
    i = brace_pos
    depth = 0
    in_str: str | None = None
    in_line_comment = False
    in_block_comment = False
    escaped = False
    while i < len(source):
        ch = source[i]
        nxt = source[i + 1] if i + 1 < len(source) else ""
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_str:
                in_str = None
        else:
            if ch == "/" and nxt == "/":
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue
            if ch in ("'", '"', "`"):
                in_str = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return source[start : i + 1]
        i += 1
    return None


def _normalize(code: str) -> str:
    # Normalisation robuste: ignore commentaires de ligne + espaces insignifiants.
    lines: list[str] = []
    for ln in code.splitlines():
        ln = re.sub(r"//.*$", "", ln).rstrip()
        lines.append(ln)
    while lines and lines[0] == "":
        lines.pop(0)
    while lines and lines[-1] == "":
        lines.pop()
    normalized = "\n".join(lines)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    return normalized


def _extract(source: str, kind: str, name: str) -> str | None:
    if kind == "const":
        return _extract_const(source, name)
    return _extract_function(source, name)


def main() -> int:
    offres = _read(OFFRES)
    filtres = _read(FILTRES)

    missing: list[str] = []
    diffs: list[str] = []

    for kind, name in BLOCKS:
        o = _extract(offres, kind, name)
        f = _extract(filtres, kind, name)
        if o is None or f is None:
            missing.append(f"{kind} {name} (offres={'ok' if o else 'missing'}, filtres={'ok' if f else 'missing'})")
            continue
        if _normalize(o) != _normalize(f):
            diffs.append(f"{kind} {name}")

    # Vérif spécifique sous-filtres pays
    has_regions_offres = "const hasRegions = regionsForCountry.length > 0;" in offres
    has_regions_filtres = "const hasRegions = regionsForCountry.length > 0;" in filtres
    if not (has_regions_offres and has_regions_filtres):
        diffs.append("rule hasRegions (> 0) not aligned")

    if missing:
        print("BLOCS INTROUVABLES:")
        for item in missing:
            print(f"- {item}")
        print()

    if diffs:
        print("PARITE NON CONFORME:")
        for item in diffs:
            print(f"- {item}")
        return 1

    print("PARITE OK: filtres offres/recherche avancee alignes sur les blocs verifies.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

