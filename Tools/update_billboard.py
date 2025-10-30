# Tools/update_billboard.py
# -*- coding: utf-8 -*-
"""
Billboard Hot 100: Wikipedia'dan yıllık #1 şarkıları çekip JSON dosyalarına yazar.
1958..bugün aralığını tarar, her yıl için DataSources/billboard_hot100/<YYYY>.json üretir
ve hepsini DataSources/billboard_hot100/all.json içinde birleştirir.

Kullanım:
  python -u Tools/update_billboard.py
"""
import csv
from __future__ import annotations

import json
import os
import re
import time
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------- Yapılandırma ----------------------------

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT_DIR = os.path.join(REPO_ROOT, "DataSources", "billboard_hot100")
COMBINED_FILE = os.path.join(OUT_DIR, "all.json")

# Wikipedia sayfa kalıbı (yıllık)
WIKI_YEAR_URL = "https://en.wikipedia.org/wiki/List_of_Billboard_Hot_100_number_ones_of_{year}"

HEADERS = {
    # Basit bir tarayıcı benzetimi (403 riskini azaltır)
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.8",
}

REQUEST_TIMEOUT = 20
RETRY_COUNT = 3
RETRY_SLEEP = 2.5

START_YEAR = 1958
THIS_YEAR = datetime.utcnow().year

# ---------------------------- Veri Modeli ----------------------------

@dataclass
class WeekRow:
    year: int
    issue_date: Optional[str]  # ISO 'YYYY-MM-DD' ya da None
    week: Optional[str]        # Bazı yıllarda 'Date' tek hücre olabilir
    song: str
    artist: str
    source: str                # yıl sayfa URL'i
    row_index: int             # tabloda satır sıra

# ---------------------------- Yardımcılar ----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

def ensure_dirs() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

def fetch_html(url: str) -> str:
    """Basit retry ile HTML getirir."""
    last_exc = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.text
            logging.warning("HTTP %s for %s", resp.status_code, url)
        except Exception as e:
            last_exc = e
            logging.warning("Fetch error (%d/%d): %s", attempt, RETRY_COUNT, e)
        time.sleep(RETRY_SLEEP * attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Unable to fetch {url}")

def norm_text(s: Optional[str]) -> str:
    if not s:
        return ""
    # NBSP → space, fazla boşlukları sadeleştir
    s = s.replace("\u00A0", " ").replace("\u200B", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def try_parse_date(cell_text: str, year_hint: int) -> Optional[str]:
    """
    Wikipedia'da tarih çeşitli biçimlerde olabilir (e.g. 'January 7', 'Jan. 7', '2018-01-07').
    Yılsız gelirse year_hint ile tamamlamayı dener.
    """
    t = norm_text(cell_text)
    if not t:
        return None

    # ISO tarihse direkt döndür
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        return t

    # 'Month D' / 'Mon D' / 'Month D, YYYY'
    cleaned = re.sub(r"[.,]", "", t)
    parts = cleaned.split()
    try:
        if len(parts) == 2:
            # Month Day ( yılı yok ) -> year_hint ekle
            dt = datetime.strptime(f"{parts[0]} {parts[1]} {year_hint}", "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
        if len(parts) == 3 and parts[2].isdigit() and len(parts[2]) == 4:
            # Month Day YYYY
            dt = datetime.strptime(f"{parts[0]} {parts[1]} {parts[2]}", "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    # Bazı sayfalarda 'Week of January 7' gibi
    m2 = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})", cleaned, re.I)
    if m2:
        month = m2.group(1)
        day = m2.group(2)
        try:
            dt = datetime.strptime(f"{month} {day} {year_hint}", "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    return None

def header_map_from_table(table: Tag) -> Dict[str, int]:
    """
    TH başlıklarını inceleyip 'date/issue date', 'song', 'artist' sütun indekslerini bulur.
    Esnek: 'Issue date', 'Date', 'Week of', 'Song', 'Single', 'Artist(s)', 'Artist' gibi çeşitleri yakalar.
    """
    mapping: Dict[str, int] = {}
    thead = table.find("thead")
    # Bazı yıllarda header <tbody> ilk satırda da olabilir
    header_row = None
    if thead:
        header_row = thead.find("tr")
    if not header_row:
        header_row = table.find("tr")

    if not header_row:
        return mapping

    ths = header_row.find_all(["th", "td"])
    for idx, th in enumerate(ths):
        txt = norm_text(th.get_text(" "))
        low = txt.lower()
        if any(k in low for k in ["issue date", "date", "week of"]):
            mapping["date"] = idx
        elif any(k in low for k in ["song", "single", "title"]):
            mapping["song"] = idx
        elif any(k in low for k in ["artist(s)", "artist"]):
            mapping["artist"] = idx
    return mapping

def extract_rows_for_year(html: str, year: int, source_url: str) -> List[WeekRow]:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table", class_=lambda c: c and "wikitable" in c)

    rows: List[WeekRow] = []

    for table in tables:
        hmap = header_map_from_table(table)
        if not hmap or "song" not in hmap or "artist" not in hmap:
            continue

        # Tablonun veri satırlarını yürü
        body = table.find("tbody") or table
        row_idx = 0
        for tr in body.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:  # boş/başlık alt satır
                continue

            # şarkı-artist zorunlu
            try:
                song_cell = tds[hmap["song"]]
                artist_cell = tds[hmap["artist"]]
            except Exception:
                continue

            song = norm_text(song_cell.get_text(" "))
            artist = norm_text(artist_cell.get_text(" "))
            if not song or not artist:
                continue

            # tarih opsiyonel (yine de dene)
            issue_iso = None
            if "date" in hmap and hmap["date"] < len(tds):
                issue_iso = try_parse_date(tds[hmap["date"]].get_text(" "), year)

            row = WeekRow(
                year=year,
                issue_date=issue_iso,
                week=issue_iso,  # "week" olarak da aynı değeri taşıyabilir; geriye dönük uyum
                song=song.strip("“”\"' "),
                artist=artist.strip("“”\"' "),
                source=source_url,
                row_index=row_idx
            )
            rows.append(row)
            row_idx += 1

    return rows

def write_year_json(year: int, rows: List[WeekRow]) -> None:
    out_path = os.path.join(OUT_DIR, f"{year}.json")
    data = [asdict(r) for r in rows]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def combine_all_years() -> List[Dict]:
    combined: List[Dict] = []
    for y in range(START_YEAR, THIS_YEAR + 1):
        path = os.path.join(OUT_DIR, f"{y}.json")
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                arr = json.load(f)
                if isinstance(arr, list):
                    combined.extend(arr)
        except Exception as e:
            logging.warning("Could not read %s (%s)", path, e)

    with open(COMBINED_FILE, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    return combined

# ---------------------------- Ana Akış ----------------------------

def scrape_year(year: int) -> int:
    url = WIKI_YEAR_URL.format(year=year)
    try:
        html = fetch_html(url)
        rows = extract_rows_for_year(html, year, url)
        write_year_json(year, rows)
        logging.info("✓ %d: %d row(s) -> billboard_hot100/%d.json", year, len(rows), year)
        # nazik hız: Wikipedia'yı yormayalım
        time.sleep(0.6)
        return len(rows)
    except Exception as e:
        logging.error("✗ %d: %s", year, e)
        # yine de boş dosya yazalım ki birleşik dosyada yıl varlığı belli olsun
        write_year_json(year, [])
        return 0

def main() -> int:
    logging.info("Billboard Hot 100 scraper started.")
    ensure_dirs()

    total_rows = 0
    for y in range(START_YEAR, THIS_YEAR + 1):
        total_rows += scrape_year(y)

    combined_rows = combine_all_years()
    csv_path = write_combined_csv(combined_rows)

    logging.info(
        "Done. Total rows combined: %d -> %s (CSV -> %s)",
        len(combined_rows),
        os.path.relpath(COMBINED_FILE, REPO_ROOT),
        os.path.relpath(csv_path, REPO_ROOT),
    )
    return 0

def write_combined_csv(rows: List[Dict]) -> str:
    csv_path = os.path.join(REPO_ROOT, "DataSources", "billboard_hot100_weekly.csv")
    fieldnames = ["year", "issue_date", "week", "song", "artist", "source", "row_index"]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({
                "year": r.get("year"),
                "issue_date": r.get("issue_date"),
                "week": r.get("week"),
                "song": r.get("song"),
                "artist": r.get("artist"),
                "source": r.get("source"),
                "row_index": r.get("row_index"),
            })
    return csv_path

# ---------------------------- Entrypoint ----------------------------

if __name__ == "__main__":
    import sys
    sys.exit(main())
