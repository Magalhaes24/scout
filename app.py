"""Application orchestration layer.

This module coordinates:
1. Input loading and column detection.
2. Worker-based scraping.
3. Missing-row retry.
4. Optional slow backfill.
"""

from __future__ import annotations

import threading
import time
from queue import Empty, Queue
from typing import Optional

from config import (
    BACKFILL_BEHIND_ROWS,
    BACKFILL_DELAY_SECONDS,
    CHECKPOINT_EVERY,
    MAX_WORKERS,
    PROGRESS_LOG_EVERY,
    WRITE_EVERY_ROW,
)
from repository import MarketValuesRepository
from transfermarkt_client import TransfermarktClient
from utils import (
    ask_enable_backfill,
    ask_worker_count,
    choose_columns_to_fill,
    detect_player_column,
    detect_squad_column,
    find_input_file,
    is_blank,
    log,
    read_input_table,
    recommended_workers,
)


class MarketValueApp:
    """Main application controller."""

    def __init__(self, folder) -> None:
        self.folder = folder
        self.repo = MarketValuesRepository(folder)
        self.columns_to_fill: set[str] = set()
        self.start_row = 1
        self.enable_backfill = False

    def _prompt_start_row(self) -> int:
        try:
            raw = input("Start from which row number? (1 = first) ").strip()
            row = int(raw) if raw else 1
            return max(1, row)
        except Exception:
            return 1

    def _worker_loop(
        self,
        worker_id: int,
        tasks_queue: Queue,
        results_queue: Queue,
        stop_event: threading.Event,
    ) -> None:
        """Worker loop with HTTP-first client and browser fallback."""
        client = TransfermarktClient()
        try:
            while not stop_event.is_set():
                try:
                    idx, player, squad = tasks_queue.get(timeout=0.3)
                except Empty:
                    break
                try:
                    row_out, source = client.process_player(player, squad)
                except Exception as exc:
                    row_out = {
                        "Player": player,
                        "Squad": squad,
                        "Matched Club": "",
                        "Transfermarkt URL": "",
                        "Market Value (raw)": "",
                        "Market Value (int)": "",
                        "Updated At": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "Status": f"error:{type(exc).__name__}",
                    }
                    source = "error"
                finally:
                    tasks_queue.task_done()
                results_queue.put((idx, row_out, source))
        finally:
            client.close()
            results_queue.put((None, None, f"worker_done:{worker_id}"))

    def _run_parallel(self, rows: list[tuple[int, str, str]]) -> tuple[list[dict], set[int], bool]:
        """Run parallel processing for selected rows."""
        total_jobs = len(rows)
        if total_jobs == 0:
            log("No rows to process from the selected start row.")
            return [], set(), False

        workers = ask_worker_count(recommended_workers(MAX_WORKERS), total_jobs)
        log(f"Parallel workers: {workers}")

        tasks_queue: Queue = Queue()
        for item in rows:
            tasks_queue.put(item)

        result_queue: Queue = Queue()
        stop_event = threading.Event()
        threads: list[threading.Thread] = []
        for worker_id in range(1, workers + 1):
            t = threading.Thread(
                target=self._worker_loop,
                args=(worker_id, tasks_queue, result_queue, stop_event),
                daemon=True,
            )
            t.start()
            threads.append(t)

        processed_idxs: set[int] = set()
        results: list[dict] = []
        processed = 0
        interrupted = False

        try:
            while processed < total_jobs:
                try:
                    idx, row_out, source = result_queue.get(timeout=0.3)
                except Empty:
                    if all(not t.is_alive() for t in threads):
                        break
                    continue
                if idx is None:
                    continue

                self.repo.update_row(idx, row_out, self.columns_to_fill)
                processed_idxs.add(idx)
                results.append(row_out)
                processed += 1

                if row_out.get("Status", "").startswith("error:") or processed % PROGRESS_LOG_EVERY == 0:
                    log(
                        f"Processed {processed}/{total_jobs} | row {idx + 1} "
                        f"| src={source} | status={row_out.get('Status','')} | value={row_out.get('Market Value (raw)','')}"
                    )

                if WRITE_EVERY_ROW:
                    self.repo.save()
                    log(f"Wrote row {idx + 1} to {self.repo.output_path.name}")
                elif CHECKPOINT_EVERY and processed % CHECKPOINT_EVERY == 0:
                    self.repo.save()
                    log(f"Checkpoint write ({processed} rows) to {self.repo.output_path.name}")
        except KeyboardInterrupt:
            interrupted = True
            stop_event.set()
            log("Interrupt received. Stopping workers...")
            self.repo.save()
            log(f"Partial write saved to {self.repo.output_path.name}")

        for t in threads:
            t.join(timeout=1.0)
        log(f"All worker sessions closed ({sum(1 for t in threads if not t.is_alive())}/{len(threads)})")

        return results, processed_idxs, interrupted

    def _retry_missing(self, row_lookup: dict[int, tuple[str, str]], missing_idxs: list[int], results: list[dict]) -> None:
        """Sequential retry for rows not returned by workers."""
        if not missing_idxs:
            return
        log(f"Checker found {len(missing_idxs)} missing rows. Retrying sequentially.")
        client = TransfermarktClient()
        try:
            for idx in missing_idxs:
                player, squad = row_lookup[idx]
                try:
                    row_out, _ = client.process_player(player, squad)
                except Exception as exc:
                    row_out = {
                        "Player": player,
                        "Squad": squad,
                        "Matched Club": "",
                        "Transfermarkt URL": "",
                        "Market Value (raw)": "",
                        "Market Value (int)": "",
                        "Updated At": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "Status": f"error:{type(exc).__name__}",
                    }
                self.repo.update_row(idx, row_out, self.columns_to_fill)
                results.append(row_out)
                if WRITE_EVERY_ROW:
                    self.repo.save()
                    log(f"Wrote row {idx + 1} to {self.repo.output_path.name} (retry)")
        finally:
            client.close()
            log("Retry browser session closed")

    def _run_backfill(self) -> None:
        """Optional slow backfill for empty URL/value rows."""
        if not self.enable_backfill:
            log("Backfill checker disabled")
            return

        start_idx = max(0, self.start_row - 1 - BACKFILL_BEHIND_ROWS)
        log(f"Backfill checker starting at row {start_idx + 1}")

        client = TransfermarktClient()
        updates = 0
        try:
            try:
                for idx in range(start_idx, len(self.repo.df)):
                    row = self.repo.df.iloc[idx]
                    player = str(row.get("Player", ""))
                    squad = str(row.get("Squad", ""))
                    if is_blank(player):
                        continue
                    needs_url = is_blank(row.get("Transfermarkt URL"))
                    needs_value_int = is_blank(row.get("Market Value (int)"))
                    if not (needs_url or needs_value_int):
                        continue

                    log(f"Backfill checking row {idx + 1}: {player} | {squad}")
                    try:
                        fetched, _ = client.process_player(player, squad)
                    except Exception as exc:
                        log(f"Backfill error row {idx + 1}: {type(exc).__name__}")
                        time.sleep(BACKFILL_DELAY_SECONDS)
                        continue

                    force_cols = {
                        "Transfermarkt URL",
                        "Market Value (raw)",
                        "Market Value (int)",
                        "Matched Club",
                        "Updated At",
                        "Status",
                    }
                    self.repo.merge_missing_fields(idx, fetched, force_cols)
                    updates += 1

                    if WRITE_EVERY_ROW:
                        self.repo.save()
                        log(f"Backfill wrote row {idx + 1} to {self.repo.output_path.name}")
                    time.sleep(BACKFILL_DELAY_SECONDS)
            except KeyboardInterrupt:
                log("Interrupt received during backfill.")
                self.repo.save()
                log(f"Partial backfill write saved to {self.repo.output_path.name}")
        finally:
            client.close()
            log("Backfill session closed")

        if updates > 0:
            self.repo.save()
            log(f"Backfill final write: {updates} rows touched")

    def run(self) -> None:
        """Program entrypoint."""
        input_path = find_input_file(self.folder)
        log(f"Input file: {input_path.name}")
        df_in = read_input_table(input_path)
        log(f"Loaded input rows: {len(df_in)}")

        player_col = detect_player_column(df_in)
        squad_col = detect_squad_column(df_in)
        log(f"Detected player column: {player_col}")
        log(f"Detected squad column: {squad_col}")

        self.repo.initialize_if_missing(df_in[player_col], df_in[squad_col])
        self.repo.load()

        self.columns_to_fill = choose_columns_to_fill(self.repo.HEADERS)
        self.start_row = self._prompt_start_row()
        self.enable_backfill = ask_enable_backfill()
        log(f"Starting from row {self.start_row}")

        rows = self.repo.iter_rows_from(self.start_row)
        row_lookup = {idx: (player, squad) for idx, player, squad in rows}
        results, processed_idxs, interrupted = self._run_parallel(rows)

        missing_idxs = [] if interrupted else sorted(set(row_lookup.keys()) - processed_idxs)
        self._retry_missing(row_lookup, missing_idxs, results)

        self.repo.save()
        log(f"Final write to {self.repo.output_path.name}")
        self._run_backfill()
        log(f"Done. Wrote {len(results)} rows to: {self.repo.output_path.name}")

