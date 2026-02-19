from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd


# ----------------------------
# Config (files in same folder)
# ----------------------------
INPUT_FILE_STEM = "players_data_light-2024_2025"   # your file name without extension
INPUT_EXTS = [".csv", ".xlsx", ".xls"]             # supported input types

# Provide ONE of these (recommended: CSV)
MARKET_VALUES_CSV = "market_values.csv"            # columns: Player, Market Value
MARKET_VALUES_JSON = "market_values.json"          # dict: { "Player Name": 12345678 }

# Output (will be created/overwritten)
OUTPUT_FILE = "players_with_market_values.csv"

# If your player name column is not obvious, add it here
POSSIBLE_PLAYER_COLS = ["Player", "player", "Name", "name", "Player Name", "player_name"]


# ----------------------------
# Helpers
# ----------------------------
def find_input_file(folder: Path) -> Path:
    for ext in INPUT_EXTS:
        candidate = folder / f"{INPUT_FILE_STEM}{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Could not find input file named '{INPUT_FILE_STEM}' with extensions {INPUT_EXTS} in {folder}"
    )


def normalize_player_name(name: str) -> str:
    # Normalization to make matching more robust:
    # - trim
    # - collapse whitespace
    # - casefold for case-insensitive matching
    s = str(name).strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def parse_market_value_to_int(value) -> Optional[int]:
    """
    Accepts:
      - int/float
      - strings like "€12.5m", "€200.00m", "€750k", "12,500,000", "12500000"
    Returns:
      - integer whole number (e.g., 12500000)
      - None if can't parse
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    # Numeric already
    if isinstance(value, (int,)):
        return int(value)
    if isinstance(value, float):
        return int(round(value))

    s = str(value).strip()
    if s == "" or s.lower() in {"null", "none", "nan"}:
        return None

    # Remove currency symbols and spaces
    s = s.replace("€", "").replace("$", "").replace("£", "")
    s = s.replace("\u00a0", " ").strip()  # non-breaking space

    # Common Transfer-style suffixes
    m = re.match(r"^([0-9]+(?:[.,][0-9]+)?)\s*([mMkK])?$", s.replace(" ", ""))
    if m:
        num = m.group(1).replace(",", ".")
        suffix = m.group(2)
        try:
            base = float(num)
        except ValueError:
            return None
        if suffix in {"m", "M"}:
            return int(round(base * 1_000_000))
        if suffix in {"k", "K"}:
            return int(round(base * 1_000))
        return int(round(base))

    # Try thousands separators like "12,500,000" or "12 500 000"
    digits = re.sub(r"[^\d]", "", s)
    if digits.isdigit() and digits != "":
        return int(digits)

    return None


def load_market_value_map(folder: Path) -> Dict[str, Optional[int]]:
    """
    Loads a mapping: normalized_player_name -> market_value_int_or_None
    from market_values.csv or market_values.json (whichever exists).
    """
    csv_path = folder / MARKET_VALUES_CSV
    json_path = folder / MARKET_VALUES_JSON

    mapping: Dict[str, Optional[int]] = {}

    if csv_path.exists():
        df = pd.read_csv(csv_path)
        # Try to find player + value columns
        player_col = None
        value_col = None
        for c in df.columns:
            if c.strip().casefold() == "player":
                player_col = c
            if c.strip().casefold() in {"market value", "market_value", "value"}:
                value_col = c
        if player_col is None:
            raise ValueError(f"{MARKET_VALUES_CSV} must have a 'Player' column.")
        if value_col is None:
            raise ValueError(f"{MARKET_VALUES_CSV} must have a 'Market Value' column (or 'market_value'/'value').")

        for _, row in df.iterrows():
            p = row[player_col]
            v = row[value_col]
            if pd.isna(p):
                continue
            mapping[normalize_player_name(str(p))] = parse_market_value_to_int(v)
        return mapping

    if json_path.exists():
        data = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"{MARKET_VALUES_JSON} must be a JSON object/dict.")
        for p, v in data.items():
            mapping[normalize_player_name(str(p))] = parse_market_value_to_int(v)
        return mapping

    raise FileNotFoundError(
        f"Provide a local mapping file in the same folder: '{MARKET_VALUES_CSV}' or '{MARKET_VALUES_JSON}'."
    )


def detect_player_column(df: pd.DataFrame) -> str:
    cols_casefold = {c.casefold(): c for c in df.columns}
    for candidate in POSSIBLE_PLAYER_COLS:
        if candidate.casefold() in cols_casefold:
            return cols_casefold[candidate.casefold()]
    # fallback: first text-like column
    for c in df.columns:
        if df[c].dtype == "object":
            return c
    raise ValueError(
        f"Could not detect the player name column. Available columns: {list(df.columns)}. "
        f"Add your column name to POSSIBLE_PLAYER_COLS."
    )


def read_input_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported input type: {path.suffix}")


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    folder = Path(__file__).resolve().parent

    input_path = find_input_file(folder)
    df_in = read_input_table(input_path)

    player_col = detect_player_column(df_in)
    df_in[player_col] = df_in[player_col].astype(str)

    mv_map = load_market_value_map(folder)

    # Build output with required columns
    out = pd.DataFrame()
    out["Player"] = df_in[player_col].astype(str)

    def lookup_value(player_name: str) -> Optional[int]:
        key = normalize_player_name(player_name)
        return mv_map.get(key, None)

    out["Market Value"] = out["Player"].map(lookup_value)

    # Save (nulls will appear empty in CSV)
    output_path = folder / OUTPUT_FILE
    out.to_csv(output_path, index=False)

    print(f"Done. Wrote {len(out)} rows to: {output_path}")


if __name__ == "__main__":
    main()
