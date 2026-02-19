"""Transfermarkt integration client.

This class provides:
1. HTTP-first search scraping for speed.
2. Selenium browser fallback for misses.
"""

from __future__ import annotations

import re
import urllib.parse
import urllib.request
from html import unescape
from typing import Optional

from config import (
    HEADLESS,
    HTTP_TIMEOUT_SECONDS,
    HTTP_USER_AGENT,
    PAGE_LOAD_TIMEOUT,
    RESULTS_TIMEOUT,
    ZEN_BINARY_PATH,
)
from utils import (
    log,
    parse_market_value_to_int,
    score_name,
    score_squad,
)


class TransfermarktClient:
    """Client that resolves player market value from Transfermarkt search."""

    def __init__(self) -> None:
        self.driver = None
        self.cookies_accepted = False

    def close(self) -> None:
        """Close browser if it was created."""
        if self.driver is not None:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    @staticmethod
    def build_search_url(player_name: str) -> str:
        query = urllib.parse.quote_plus(player_name.strip())
        return f"https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche?query={query}"

    @staticmethod
    def _strip_tags(text: str) -> str:
        return re.sub(r"<[^>]+>", "", text or "")

    def _build_driver(self):
        from selenium import webdriver
        from selenium.webdriver.firefox.options import Options as FirefoxOptions

        options = FirefoxOptions()
        if HEADLESS:
            options.add_argument("-headless")
        if ZEN_BINARY_PATH:
            options.binary_location = ZEN_BINARY_PATH
        options.set_capability("pageLoadStrategy", "eager")
        options.set_preference("permissions.default.image", 2)
        options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", "false")

        driver = webdriver.Firefox(options=options)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        return driver

    def _accept_cookies_if_present(self, wait_seconds: float = 1.0) -> bool:
        """Try click cookie accept button in main page or iframes."""
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        def try_click() -> bool:
            selectors = [
                (By.CSS_SELECTOR, "button.accept-all"),
                (By.XPATH, "//button[@title='Accept & continue' or @aria-label='Accept & continue']"),
                (By.XPATH, "//button[contains(.,'Accept & continue')]"),
                (By.XPATH, "//button[contains(.,'Accept') or contains(.,'I Agree') or contains(.,'Agree')]"),
                (By.ID, "onetrust-accept-btn-handler"),
                (By.CSS_SELECTOR, "button#onetrust-accept-btn-handler"),
            ]
            for by, sel in selectors:
                try:
                    btn = WebDriverWait(self.driver, wait_seconds).until(EC.element_to_be_clickable((by, sel)))
                    btn.click()
                    log("Accepted cookies popup")
                    return True
                except Exception:
                    pass
            return False

        try:
            if try_click():
                return True
            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            for frame in iframes:
                try:
                    self.driver.switch_to.frame(frame)
                    if try_click():
                        self.driver.switch_to.default_content()
                        return True
                except Exception:
                    pass
                finally:
                    try:
                        self.driver.switch_to.default_content()
                    except Exception:
                        pass
        except Exception:
            return False
        return False

    def _ensure_browser(self) -> None:
        """Lazy-create browser session for fallback mode only."""
        if self.driver is not None:
            return
        self.driver = self._build_driver()
        self.driver.get("https://www.transfermarkt.com/")
        self.cookies_accepted = self._accept_cookies_if_present(wait_seconds=1.0)

    def _find_best_http(self, player_name: str, squad: str) -> Optional[tuple[str, str, str]]:
        """Parse search results using HTTP (fast path)."""
        req = urllib.request.Request(
            self.build_search_url(player_name),
            headers={"User-Agent": HTTP_USER_AGENT},
        )
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                html = resp.read().decode("utf-8", errors="ignore")
        except Exception:
            return None

        rows_html = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.I | re.S)
        best = None
        for row_html in rows_html:
            m_player = re.search(
                r'<a[^>]+href="([^"]*?/profil/spieler/\d+[^"]*)"[^>]*>(.*?)</a>',
                row_html,
                flags=re.I | re.S,
            )
            if not m_player:
                continue

            href = unescape(m_player.group(1)).strip()
            row_player = unescape(self._strip_tags(m_player.group(2))).strip()
            if href.startswith("/"):
                href = f"https://www.transfermarkt.com{href}"

            name_score = score_name(player_name, row_player)
            if name_score == 0:
                continue

            club_candidates: list[str] = []
            for m in re.finditer(
                r'<a[^>]+href="[^"]*?/verein/\d+[^"]*"[^>]*>(.*?)</a>',
                row_html,
                flags=re.I | re.S,
            ):
                club_txt = unescape(self._strip_tags(m.group(1))).strip()
                if club_txt:
                    club_candidates.append(club_txt)
            for m in re.finditer(r'<img[^>]+(?:title|alt)="([^"]+)"[^>]*>', row_html, flags=re.I):
                txt = unescape(m.group(1)).strip()
                if txt:
                    club_candidates.append(txt)

            mv_raw = ""
            m_mv = re.search(
                r'<td[^>]*class="[^"]*\brechts\b[^"]*\bhauptlink\b[^"]*"[^>]*>(.*?)</td>',
                row_html,
                flags=re.I | re.S,
            )
            if m_mv:
                mv_raw = unescape(self._strip_tags(m_mv.group(1))).strip()
            if not mv_raw:
                full = unescape(self._strip_tags(row_html))
                full = full.replace("â‚¬", "€").replace("Â£", "£")
                m_mv2 = re.search(r"[€$£]\s*[\d.,]+\s*[mkMK]?", full)
                if m_mv2:
                    mv_raw = m_mv2.group(0).strip()

            squad_score, best_club = score_squad(squad, club_candidates)
            candidate = ((name_score * 10) + (squad_score * 4) + (1 if mv_raw else 0), name_score, squad_score, href, best_club, mv_raw)
            if best is None or candidate[:3] > best[:3]:
                best = candidate

        if best is None:
            return None
        _, _, _, href, best_club, mv_raw = best
        return href, best_club, mv_raw

    def _find_best_browser(self, player_name: str, squad: str) -> Optional[tuple[str, str, str]]:
        """Parse search results using Selenium fallback."""
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        self.driver.get(self.build_search_url(player_name))
        if not self.cookies_accepted:
            self.cookies_accepted = self._accept_cookies_if_present(wait_seconds=1.0) or self.cookies_accepted

        WebDriverWait(self.driver, RESULTS_TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        links = self.driver.find_elements(By.XPATH, "//a[contains(@href,'/profil/spieler/')]")
        if not links:
            return None

        def extract_mv(row_elem) -> str:
            try:
                mv_cells = row_elem.find_elements(By.CSS_SELECTOR, "td.rechts.hauptlink")
                for cell in mv_cells:
                    txt = cell.text.strip()
                    txt = txt.replace("â‚¬", "€").replace("Â£", "£")
                    if re.search(r"[€$£]\s*[\d.,]+\s*[mkMK]?", txt):
                        return txt
            except Exception:
                pass
            return ""

        best = None
        seen_rows = set()
        for link in links:
            href = link.get_attribute("href")
            if not href:
                continue
            try:
                row = link.find_element(By.XPATH, "./ancestor::tr")
            except Exception:
                continue
            if row.id in seen_rows:
                continue
            seen_rows.add(row.id)

            row_player = link.text.strip()
            if not row_player:
                try:
                    same_row_links = row.find_elements(By.XPATH, ".//a[contains(@href,'/profil/spieler/')]")
                    texts = [x.text.strip() for x in same_row_links if x.text and x.text.strip()]
                    if texts:
                        row_player = max(texts, key=len)
                except Exception:
                    pass
            name_score = score_name(player_name, row_player)
            if name_score == 0:
                continue

            club_candidates: list[str] = []
            try:
                for img in row.find_elements(By.XPATH, ".//img"):
                    title = (img.get_attribute("title") or "").strip()
                    alt = (img.get_attribute("alt") or "").strip()
                    if title:
                        club_candidates.append(title)
                    if alt:
                        club_candidates.append(alt)
                for c_link in row.find_elements(By.XPATH, ".//a[contains(@href,'/verein/')]"):
                    txt = c_link.text.strip()
                    if txt:
                        club_candidates.append(txt)
            except Exception:
                pass

            mv_raw = extract_mv(row)
            squad_score, best_club = score_squad(squad, club_candidates)
            candidate = ((name_score * 10) + (squad_score * 4) + (1 if mv_raw else 0), name_score, squad_score, href, best_club, mv_raw)
            if best is None or candidate[:3] > best[:3]:
                best = candidate

        if best is None:
            return None
        _, _, _, href, best_club, mv_raw = best
        return href, best_club, mv_raw

    def process_player(self, player_name: str, squad: str) -> tuple[dict, str]:
        """Return row payload and source ('http' or 'browser')."""
        match = self._find_best_http(player_name, squad)
        source = "http"
        if match is None or not (match[2] and str(match[2]).strip()):
            self._ensure_browser()
            browser_match = self._find_best_browser(player_name, squad)
            if browser_match is not None:
                match = browser_match
                source = "browser"

        if not match:
            return {
                "Player": player_name,
                "Squad": squad,
                "Matched Club": "",
                "Transfermarkt URL": "",
                "Market Value (raw)": "",
                "Market Value (int)": "",
                "Updated At": __import__("time").strftime("%Y-%m-%d %H:%M:%S"),
                "Status": "not_found",
            }, source

        url, matched_club, mv_raw = match
        mv_int = parse_market_value_to_int(mv_raw) if mv_raw else None
        return {
            "Player": player_name,
            "Squad": squad,
            "Matched Club": matched_club,
            "Transfermarkt URL": url,
            "Market Value (raw)": mv_raw,
            "Market Value (int)": mv_int if mv_int is not None else "",
            "Updated At": __import__("time").strftime("%Y-%m-%d %H:%M:%S"),
            "Status": "ok" if mv_raw else "value_not_found",
        }, source
