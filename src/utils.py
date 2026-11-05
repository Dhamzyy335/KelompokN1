"""Helpers for discovering and grouping the NBA/WNBA raw CSV files in dataset/.

File naming convention observed in dataset/:
    {family}_{year}.csv        -> regular season
    {family}_po_{year}.csv     -> playoffs
where {family} itself may contain underscores (e.g. "wnba_cdnnba").
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field

FILENAME_PATTERN = re.compile(r"^(.+?)(_po)?_(\d{4})\.csv$")


@dataclass
class FileInfo:
    family: str
    season: int
    game_type: str  # "regular" or "playoff"
    path: str
    filename: str


def scan_dataset(dataset_dir: str) -> dict[str, list[FileInfo]]:
    """Scan dataset_dir for CSV files and group them by family.

    Returns a dict mapping family name -> list of FileInfo, sorted by
    season then game_type, so callers get a deterministic ordering.
    """
    families: dict[str, list[FileInfo]] = {}

    for filename in sorted(os.listdir(dataset_dir)):
        if not filename.endswith(".csv"):
            continue

        match = FILENAME_PATTERN.match(filename)
        if not match:
            continue

        family, po_flag, year = match.groups()
        info = FileInfo(
            family=family,
            season=int(year),
            game_type="playoff" if po_flag else "regular",
            path=os.path.join(dataset_dir, filename),
            filename=filename,
        )
        families.setdefault(family, []).append(info)

    for file_list in families.values():
        file_list.sort(key=lambda f: (f.season, f.game_type))

    return families


def summarize_families(families: dict[str, list[FileInfo]]) -> list[dict]:
    """Build a printable summary: one row per family with file/year counts."""
    summary = []
    for family, files in sorted(families.items()):
        seasons = sorted({f.season for f in files})
        regular_count = sum(1 for f in files if f.game_type == "regular")
        playoff_count = sum(1 for f in files if f.game_type == "playoff")
        summary.append(
            {
                "family": family,
                "total_files": len(files),
                "regular_files": regular_count,
                "playoff_files": playoff_count,
                "season_min": min(seasons),
                "season_max": max(seasons),
                "seasons": seasons,
            }
        )
    return summary
