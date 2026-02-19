"""CLI entrypoint for market value scraping."""

from __future__ import annotations

from pathlib import Path

from app import MarketValueApp


def main() -> None:
    """Launch application from current file directory."""
    folder = Path(__file__).resolve().parent
    MarketValueApp(folder).run()


if __name__ == "__main__":
    main()

