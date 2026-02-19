"""CSV repository for market values.

Encapsulates file I/O and row-level update semantics.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import OUTPUT_FILE, VERBOSE_ROW_LOG
from utils import is_blank, log


class MarketValuesRepository:
    """Read/write wrapper around market_values.csv."""

    HEADERS = [
        "Player",
        "Squad",
        "Matched Club",
        "Transfermarkt URL",
        "Market Value (raw)",
        "Market Value (int)",
        "Updated At",
        "Status",
    ]

    def __init__(self, folder: Path) -> None:
        self.output_path = folder / OUTPUT_FILE
        self.df = pd.DataFrame(columns=self.HEADERS)

    def initialize_if_missing(self, seed_players: pd.Series, seed_squads: pd.Series) -> None:
        """Create CSV with player/squad seed rows when file does not exist."""
        if self.output_path.exists():
            return
        seed = pd.DataFrame({"Player": seed_players.astype(str), "Squad": seed_squads.astype(str)})
        for col in self.HEADERS:
            if col not in seed.columns:
                seed[col] = ""
        seed.to_csv(self.output_path, index=False)
        log(f"Created {self.output_path.name} from input players")

    def load(self) -> None:
        """Load current CSV contents into memory."""
        try:
            # Keep text-like columns as strings to avoid dtype write warnings.
            self.df = pd.read_csv(self.output_path, dtype=str, keep_default_na=False)
        except pd.errors.EmptyDataError:
            self.df = pd.DataFrame(columns=self.HEADERS)

    def save(self) -> None:
        """Persist in-memory dataframe to CSV."""
        self.df.to_csv(self.output_path, index=False)

    def iter_rows_from(self, start_row: int) -> list[tuple[int, str, str]]:
        """Return processing tuples (index, player, squad) from 1-based start row."""
        rows: list[tuple[int, str, str]] = []
        for idx, row in self.df.iterrows():
            if idx + 1 < start_row:
                continue
            player = str(row.get("Player", ""))
            squad = str(row.get("Squad", ""))
            if not player or player.lower() == "nan":
                continue
            rows.append((idx, player, squad))
        return rows

    def update_row(self, idx: int, row_data: dict, columns_to_fill: set[str]) -> None:
        """Update only allowed columns in a target row."""
        for col, val in row_data.items():
            if col in self.df.columns and col in columns_to_fill:
                self.df.at[idx, col] = val
        if VERBOSE_ROW_LOG:
            log(f"Updated row {idx + 1}: {row_data}")

    def merge_missing_fields(self, idx: int, fetched_row: dict, force_columns: set[str]) -> None:
        """Fill only missing fields on an existing row."""
        current = self.df.iloc[idx].to_dict()
        for col in force_columns:
            if col in fetched_row and is_blank(current.get(col)):
                current[col] = fetched_row.get(col)
        if not is_blank(current.get("Market Value (raw)")):
            current["Status"] = "ok"
        elif not is_blank(current.get("Transfermarkt URL")):
            current["Status"] = "value_not_found"

        for col, val in current.items():
            if col in self.df.columns:
                self.df.at[idx, col] = val
