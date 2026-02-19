# Football Market Value Scraper (Python)

This project enriches a local players dataset with Transfermarkt market values and writes results to `market_values.csv`.

The Python app is structured in modules (OOP style) and uses:
- HTTP-first scraping for speed.
- Selenium browser fallback for rows where HTTP cannot extract a value.
- Parallel workers with safe CSV updates.
- Optional slow backfill checker for missing URL/value cells.

## Project Structure

- `main.py`
  - Entrypoint only.
  - Starts the app from the current folder.

- `app.py`
  - `MarketValueApp` orchestration class.
  - Handles prompts, worker lifecycle, retries, checkpoint writes, and optional backfill.

- `transfermarkt_client.py`
  - `TransfermarktClient` integration layer.
  - Tries HTTP search parsing first.
  - Falls back to Selenium when HTTP misses or does not return `Market Value (raw)`.

- `repository.py`
  - `MarketValuesRepository` persistence layer.
  - Loads/saves `market_values.csv`, updates rows by index, merges missing fields in backfill.

- `utils.py`
  - Shared utilities: logging, normalization, scoring, parsing values, prompts.
  - Includes robust player-name scoring (supports short names like `Emi Buendía` vs `Emiliano Buendía`).

- `config.py`
  - Runtime configuration constants (workers, timeouts, checkpoint frequency, etc.).

## Input and Output

### Input file

The app searches for one of:
- `players_data_light-2024_2025.csv`
- `players_data_light-2024_2025.xlsx`
- `players_data_light-2024_2025.xls`

It detects:
- Player column from: `Player`, `player`, `Name`, `name`, `Player Name`, `player_name`
- Squad column from: `Squad`, `squad`, `Team`, `team`, `Club`, `club`

### Output file

- `market_values.csv`

Main columns:
- `Player`
- `Squad`
- `Matched Club`
- `Transfermarkt URL`
- `Market Value (raw)`
- `Market Value (int)`
- `Updated At`
- `Status`

## How It Works

1. Load input players.
2. Initialize/load `market_values.csv`.
3. Prompt for:
   - Fill mode (`all` or `selected` columns)
   - Start row
   - Backfill enabled/disabled
   - Worker count
4. Process rows in parallel:
   - HTTP search parse first
   - Browser fallback only when needed
5. Retry rows not returned by workers.
6. Save final CSV.
7. Optional backfill pass for rows with missing URL/value.

## Setup

Use your active Python environment (Conda recommended if that is what you run with).

```powershell
python -m pip install pandas selenium
```

Firefox WebDriver is required for fallback mode:
- Install Firefox/Zen (Firefox-based), and ensure geckodriver is available to Selenium.

## Run

```powershell
python main.py
```

You will see prompts:
- `Fill all columns or selected columns`
- `Start from which row number`
- `Enable slow backfill checker`
- `How many workers`

## Performance Notes

- The biggest speed gain is HTTP-first matching (already enabled in code).
- Browser fallback is expensive; used only when HTTP cannot provide a value.
- More workers increase throughput but also CPU/RAM usage.
- If your machine overloads, reduce workers at prompt time.

## Common Issues

- Many rows with `status=value_not_found`
  - This usually means HTTP matched player but no value extracted.
  - Current code falls back to browser in that case.

- Cookie popup blocks browser fallback
  - The client attempts auto-accept in main page and iframes.

- Interrupted run (`Ctrl+C`)
  - The app writes partial progress before stopping.

## Configuration

Edit `config.py` for defaults such as:
- `MAX_WORKERS`
- `CHECKPOINT_EVERY`
- `WRITE_EVERY_ROW`
- `RESULTS_TIMEOUT`
- `HEADLESS`
- `BACKFILL_*`

## Git / Version Control

Typical flow:

```powershell
git add .
git commit -m "Update scraper and docs"
git push
```

