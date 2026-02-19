"""Shared utility helpers for normalization, parsing, prompts, and logging."""

from __future__ import annotations

import os
import re
import time
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd

from config import INPUT_EXTS, INPUT_FILE_STEM, POSSIBLE_PLAYER_COLS, POSSIBLE_SQUAD_COLS


def log(message: str) -> None:
    """Write a timestamped log line."""
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {message}")


def find_input_file(folder: Path) -> Path:
    """Find input file by configured stem and known extensions."""
    for ext in INPUT_EXTS:
        candidate = folder / f"{INPUT_FILE_STEM}{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Could not find input file '{INPUT_FILE_STEM}' with extensions {INPUT_EXTS} in {folder}"
    )


def read_input_table(path: Path) -> pd.DataFrame:
    """Read CSV/XLS/XLSX into a dataframe."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported input type: {path.suffix}")


def detect_player_column(df: pd.DataFrame) -> str:
    """Resolve the player column using configured candidates."""
    cols_casefold = {c.casefold(): c for c in df.columns}
    for candidate in POSSIBLE_PLAYER_COLS:
        if candidate.casefold() in cols_casefold:
            return cols_casefold[candidate.casefold()]
    for c in df.columns:
        if df[c].dtype == "object":
            return c
    raise ValueError("Could not detect player column.")


def detect_squad_column(df: pd.DataFrame) -> str:
    """Resolve the squad/club column using configured candidates."""
    cols_casefold = {c.casefold(): c for c in df.columns}
    for candidate in POSSIBLE_SQUAD_COLS:
        if candidate.casefold() in cols_casefold:
            return cols_casefold[candidate.casefold()]
    raise ValueError("Could not detect squad column.")


def normalize_text(value: str) -> str:
    """Normalize text for fuzzy matching (spacing/case/accents)."""
    s = str(value).strip()
    s = re.sub(r"\s+", " ", s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.casefold()


def normalize_player_name(name: str) -> str:
    """Normalize player name string."""
    return normalize_text(name)


def normalize_club_name(name: str) -> str:
    """Normalize club name string."""
    return normalize_text(name)


def parse_market_value_to_int(value) -> Optional[int]:
    """Convert market value text (e.g. '€14.00m') to integer."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))

    s = str(value).strip()
    if s == "" or s.lower() in {"null", "none", "nan"}:
        return None

    s = s.replace("\u20ac", "").replace("$", "").replace("\u00a3", "")
    s = s.replace("â‚¬", "").replace("Â£", "")
    s = s.replace("\u00a0", " ").strip()

    m = re.match(r"^([0-9]+(?:[.,][0-9]+)?)\s*([mMkK])?$", s.replace(" ", ""))
    if m:
        num = m.group(1).replace(",", ".")
        suffix = m.group(2)
        base = float(num)
        if suffix in {"m", "M"}:
            return int(round(base * 1_000_000))
        if suffix in {"k", "K"}:
            return int(round(base * 1_000))
        return int(round(base))

    digits = re.sub(r"[^\d]", "", s)
    if digits.isdigit() and digits:
        return int(digits)
    return None


def score_name(query_name: str, row_name: str) -> int:
    """Score name similarity. Higher is better."""
    if not row_name:
        return 0
    def clean_token(tok: str) -> str:
        # Keep only letters to tolerate encoding/punctuation noise.
        return re.sub(r"[^a-z]", "", tok.casefold())

    q = normalize_player_name(query_name)
    r = normalize_player_name(row_name)
    if q == r:
        return 3
    if q in r or r in q:
        return 2

    q_tokens = set(q.split())
    r_tokens = set(r.split())
    if q_tokens and len(q_tokens.intersection(r_tokens)) >= max(1, min(len(q_tokens), 2)):
        return 1

    q_parts = q.split()
    r_parts = r.split()
    if len(q_parts) >= 2 and len(r_parts) >= 2:
        q_first, r_first = clean_token(q_parts[0]), clean_token(r_parts[0])
        q_last, r_last = clean_token(q_parts[-1]), clean_token(r_parts[-1])
        if not q_first or not r_first or not q_last or not r_last:
            return 0
        min_len = min(len(q_last), len(r_last))
        common_prefix = 0
        for a, b in zip(q_last, r_last):
            if a != b:
                break
            common_prefix += 1
        same_last = (
            q_last == r_last
            or (min_len >= 4 and (q_last.startswith(r_last[:min_len]) or r_last.startswith(q_last[:min_len])))
            or common_prefix >= 5
        )
        # Explicit short-first-name support: "Emi" -> "Emiliano".
        first_prefix = len(q_first) >= 3 and (r_first.startswith(q_first) or q_first.startswith(r_first))
        if same_last and first_prefix:
            return 1
    return 0


def score_squad(query_squad: str, row_clubs: list[str]) -> tuple[int, str]:
    """Score squad similarity and return best matching club label."""
    if not query_squad:
        return 0, row_clubs[0] if row_clubs else ""

    q = normalize_club_name(query_squad)
    best_score = 0
    best_club = row_clubs[0] if row_clubs else ""
    for club in row_clubs:
        c = normalize_club_name(club)
        if not c:
            continue
        if q == c:
            return 3, club
        if q in c or c in q:
            if best_score < 2:
                best_score = 2
                best_club = club
            continue
        q_tokens = set(q.split())
        c_tokens = set(c.split())
        if q_tokens and q_tokens.intersection(c_tokens) and best_score < 1:
            best_score = 1
            best_club = club
    return best_score, best_club


def is_blank(value) -> bool:
    """True when value is empty/NaN/null-like."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    s = str(value).strip().casefold()
    return s in {"", "nan", "none", "null"}


def choose_columns_to_fill(headers: list[str]) -> set[str]:
    """Prompt user for all or selected editable columns."""
    editable = [h for h in headers if h not in {"Player", "Squad"}]
    mode = input("Fill `all` columns or `selected` columns? [all/selected] ").strip().casefold()
    if mode != "selected":
        log("Column mode: all")
        return set(editable)

    print("Selectable columns:")
    print(", ".join(editable))
    selected_input = input("Enter selected columns (comma-separated): ").strip()
    selected = {c.strip() for c in selected_input.split(",") if c.strip()}
    chosen = {c for c in editable if c in selected}
    if not chosen:
        log("No valid selected columns provided. Falling back to all.")
        return set(editable)
    log(f"Column mode: selected -> {sorted(chosen)}")
    return chosen


def ask_worker_count(default_workers: int, total_jobs: int) -> int:
    """Prompt user for worker count with bounds."""
    try:
        raw = input(f"How many workers? [1-{max(1, total_jobs)}] (default {default_workers}) ").strip()
        if not raw:
            return max(1, min(default_workers, total_jobs))
        return max(1, min(int(raw), total_jobs))
    except Exception:
        return max(1, min(default_workers, total_jobs))


def recommended_workers(max_workers: int) -> int:
    """Compute a default worker count based on CPU availability."""
    cpu = os.cpu_count() or 4
    return max(2, min(max_workers, max(2, cpu - 1)))


def ask_enable_backfill() -> bool:
    """Prompt user to enable/disable slow backfill checker."""
    raw = input("Enable slow backfill checker? [y/N] ").strip().casefold()
    return raw in {"y", "yes"}
