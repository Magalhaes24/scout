"""Application configuration constants.

This module centralizes runtime defaults so behavior can be tuned without
modifying business logic code.
"""

from __future__ import annotations


# Input dataset configuration.
INPUT_FILE_STEM = "players_data_light-2024_2025"
INPUT_EXTS = [".csv", ".xlsx", ".xls"]

# Output file.
OUTPUT_FILE = "market_values.csv"

# Supported dataset columns.
POSSIBLE_PLAYER_COLS = ["Player", "player", "Name", "name", "Player Name", "player_name"]
POSSIBLE_SQUAD_COLS = ["Squad", "squad", "Team", "team", "Club", "club"]

# Selenium/browser settings.
HEADLESS = True
PAGE_LOAD_TIMEOUT = 12
RESULTS_TIMEOUT = 2
ZEN_BINARY_PATH = ""

# Processing settings.
CHECKPOINT_EVERY = 20
WRITE_EVERY_ROW = False
MAX_WORKERS = 6
PROGRESS_LOG_EVERY = 25

# Backfill settings.
BACKFILL_BEHIND_ROWS = 5
BACKFILL_DELAY_SECONDS = 0.8

# Logging verbosity.
VERBOSE_ROW_LOG = False

# HTTP-first scraping settings.
HTTP_TIMEOUT_SECONDS = 6
HTTP_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

