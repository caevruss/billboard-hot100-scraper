#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch Billboard Hot 100 weekly #1 songs per year directly from per-year Wikipedia pages:
  https://en.wikipedia.org/wiki/List_of_Billboard_Hot_100_number_ones_of_{YEAR}

Writes one JSON per year into DataSources/billboard_hot100/{YEAR}.json
and a combined DataSources/billboard_hot100/all.json

Run:
  cd Tools
  python update_billboard.py
"""

from __future__ import annotations
import os, re, json, time, datetime, itertools
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

# -----------------------
# Config
# -----------------------
BASE_URL_TEMPLATE = "https://en.wikipedia.org/wiki/List_of_Billboard_Hot_100_number_ones_of_{year}"
DATA_ROOT = Path(__file__).resolve().parents[1] / "DataSources" / "billboard_hot100"
DATA_ROOT.mkdir(parents=True, exist_ok=True)

FIRST_YEAR = 1958
CURRENT_YEAR = datetime.date.today().year

REQUEST_TIMEOUT = 20
# Wikipedia'ya nazik davranalım
PER_REQUEST_SLEEP = 0.8

HEADERS = {
    # Basit bir UA çoğu 403'ü çözer
    "User-Agent": "Hot100-Scraper/1.0 (+github.com/<your-user>/billboard-hot100-scraper)"
}

# -----------------------
# Helpers
# -----------------------

def get(url: str) -> str:
    for attempt in range(3):
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
        if r.status_code == 200:
            return r.text
        # 429/403 gibi durumlarda küçük backoff
        time.sleep(1.5 + attempt)
    r.raise_for_status()
    return ""  # unreachable

def normalize_space(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return s

def parse_date(text: str) -> Optional[str]:
    """
    Wikipedia 'Issue date' hücreleri ör. 'January 6', 'Jan. 6, 2018', 'Jan 6' gibi olabilir.
    Yıl yoksa, çağıran fonksiyon ilgili yıldan tamamlayacak.
    Dönen format ISO: YYYY-MM-DD
    """
    t = normalize_space(text)
    if not t:
        return None

    # Çeşitli kısaltmaları toparlayalım (Jan. -> January vs.)
    MONTHS = {
        'jan': 'January', 'feb': 'February', 'mar': 'March', 'apr': 'April',
        'may': 'May', 'jun': 'June', 'jul': 'July', 'aug': 'August',
        'sep': 'September', 'sept':'September', 'oct': 'October',
        'nov': 'November', 'dec': 'December'
    }

    # 'Jan.', 'Jan' -> 'January'
    t = re.sub(r"\b([A-Za-z]{3,4})\.\b", r"\1", t)
    parts = t.split()
    if not parts:
        return None

    # Ay adını normalleştirelim
    m0 = parts[0].lower()
    if m0 in MONTHS:
        parts[0] = MONTHS[m0]
        t = " ".join(parts)

    # Şu kalıplar sık görülür:
    # "January 6" / "January 6, 2018" / "January 6 (2018)"
    t = re.sub(r"[()]", " ", t)
    t = normalize_space(t)

    # En çok işleyen formatlar
    fmts = [
        "%B %d, %Y",
        "%B %d %Y",
        "%B %d",        # yılı sonra biz ekleyeceğiz
    ]
    for f in fmts:
        try:
            # Yıl yoksa ValueError atar
            dt = datetime.datetime.strptime(t, f)
            # Eğer %Y yoksa da buraya düşmeyecek
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Olmadı, None
    return None

def pick_hot100_tables(soup: BeautifulSoup) -> List[Tag]:
    """
    Yıl sayfasında genelde bir veya birkaç tablo var.
    İstediğimiz tablo, başlıklarda 'Issue date' ile 'Song'/'Title' ve 'Artist(s)' olan tablo.
    """
    tables = []
    for tbl in soup.find_all("table", class_=lambda c: c and "wikitable" in c):
        heads = [normalize_space(th.get_text(" ")) for th in tbl.find_all("th")]
        heads_text = " | ".join(heads).lower()
        if "issue date" in heads_text and ("song" in heads_text or "title" in heads_text):
            tables.append(tbl)
    return tables

def parse_year_page(html: str, year: int) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    tables = pick_hot100_tables(soup)
    rows: List[Dict[str, Any]] = []

    for tbl in tables:
        # satırlar
        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            # Başlık indeksleri her tabloda farklı olabildiği için, esnek çıkaralım:
            # 'Issue date' -> ilk/ikinci hücrelerde olur çoğunlukla
            # 'Song/Title'  -> link veya italik olabilir
            # 'Artist(s)'   -> bazen 'Artist' veya 'Artist(s)'
            ths = [normalize_space(th.get_text(" ")) for th in tbl.find_all("th")]
            th_lower = [h.lower() for h in ths]
            # Kabaca indeksler:
            def find_idx(keys: List[str]) -> Optional[int]:
                for k in keys:
                    for i,h in enumerate(th_lower):
                        if k in h:
                            return i
                return None

            idx_date = find_idx(["issue date", "date"])
            idx_song = find_idx(["song", "title"])
            idx_artist = find_idx(["artist", "artist(s)"])

            # Yedek plan: İlk üç hücreyi sırayla deneyelim
            if idx_date is None:  idx_date  = 0
            if idx_song is None:  idx_song  = 1 if len(tds) > 1 else 0
            if idx_artist is None: idx_artist = 2 if len(tds) > 2 else 1

            cells = [normalize_space(td.get_text(" ")) for td in tds]
            try:
                raw_date  = cells[idx_date]
                raw_song  = cells[idx_song]
                raw_artist= cells[idx_artist]
            except IndexError:
                continue

            # Şarkı adını link/italikten almayı deneyelim (daha temiz)
            song_cell = tds[min(idx_song, len(tds)-1)]
            song_text = song_cell.get_text(" ").strip()
            # italik varsa onu tercih et
            i_tag = song_cell.find("i")
            if i_tag:
                song_text = i_tag.get_text(" ").strip()

            # Tarihi ISO'ya çevir (yıl yoksa bu yıl ile tamamlayacağız)
            iso = parse_date(raw_date)
            if iso is None:
                # Ay-gün parse edilememiş olabilir; boş geç
                continue

            # Yıl yoksa ekle
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", iso):
                dt = datetime.datetime.strptime(iso, "%Y-%m-%d").date()
                iso_date = dt
            else:
                # normalde buraya gelmeyiz
                continue

            # Bazı sayfalarda tablo yıl sonuna sarkabilir; cross-year durumunda
            # çok küçük oynamalar olabilir. Genelde Issue date yıl ile uyumludur.
            entry = {
                "issue_date": iso_date.isoformat(),
                "year": iso_date.year,
                "song": song_text,
                "artist": raw_artist,
                "source": "wikipedia"
            }
            rows.append(entry)

    # Yinelenenleri (bazı sayfalarda aynı haftanın birden çok tablo satırı olabiliyor) temizle
    dedup = {}
    for r in rows:
        key = (r["issue_date"], normalize_space(r["song"]).lower(), normalize_space(r["artist"]).lower())
        if key not in dedup:
            dedup[key] = r
    rows = list(dedup.values())

    # Sadece istenen yıl aralığı
    rows = [r for r in rows if r["year"] >= FIRST_YEAR and r["year"] <= CURRENT_YEAR]
    rows.sort(key=lambda r: r["issue_date"])
    return rows

def save_year(year: int, rows: List[Dict[str, Any]]):
    out = DATA_ROOT / f"{year}.json"
    out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  ✔ {year}: {len(rows)} rows -> {out.relative_to(DATA_ROOT.parent)}")

def main():
    years = list(range(FIRST_YEAR, CURRENT_YEAR + 1))
    print(f"Fetching Hot 100 #1 per-year pages ({years[0]}–{years[-1]}) …")

    all_rows: List[Dict[str, Any]] = []

    for y in years:
        url = BASE_URL_TEMPLATE.format(year=y)
        try:
            html = get(url)
        except requests.HTTPError as e:
            print(f"  ✖ {y}: HTTP {e.response.status_code} — skipping")
            continue
        rows = parse_year_page(html, y)
        save_year(y, rows)
        all_rows.extend(rows)
        time.sleep(PER_REQUEST_SLEEP)

    # Birleşik dosya
    all_rows.sort(key=lambda r: (r["issue_date"], r["song"].lower()))
    combined = DATA_ROOT / "all.json"
    combined.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nDone. Total rows: {len(all_rows)} -> {combined.relative_to(DATA_ROOT.parent)}")

if __name__ == "__main__":
    main()
