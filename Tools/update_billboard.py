from __future__ import annotations

import re
import json
import time
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from datetime import date

# ---- Ayarlar ----
BASE_URL = "https://en.wikipedia.org"
INDEX_URL = f"{BASE_URL}/wiki/Lists_of_Billboard_number-one_singles"
OUT_DIR = Path("DataSources")
JSON_OUT = OUT_DIR / "billboard_hot100_by_issue_date.json"
CSV_OUT  = OUT_DIR / "billboard_hot100_by_issue_date.csv"

# 1958 Hot 100 başlangıcı
MIN_YEAR = 1958
MAX_YEAR = date.today().year  # istersen sabitleyebilirsin

# Tarayıcı taklidi + retry
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/"
    })
    return s

# Metin temizleme (köşeli kaynak dipnotlarını ve boşlukları temizler)
CITE_RE = re.compile(r"\[\d+\]")
WS_RE = re.compile(r"\s+")
def clean_text(t: str) -> str:
    t = (t or "").strip()
    t = CITE_RE.sub("", t)
    t = WS_RE.sub(" ", t)
    return t.strip().strip("“”\"'")

def fetch_html(s: requests.Session, url: str) -> str:
    # basit retry
    for i in range(4):
        r = s.get(url, timeout=20)
        if r.status_code == 200 and r.text:
            return r.text
        # ufak gecikmeyle tekrar dene
        time.sleep(0.8 + i * 0.6)
    r.raise_for_status()
    return r.text

def extract_hot100_year_links(index_html: str) -> List[Tuple[int,str]]:
    """
    Ana sayfadaki "Hot 100 era" bölümünden 1958..MAX_YEAR linklerini bulur.
    Dönüş: [(yıl, /wiki/Billboard_Hot_100_number_ones_of_1990), ...]
    """
    soup = BeautifulSoup(index_html, "lxml")
    # "Hot 100 era" başlığının altındaki yıl linkleri
    header = soup.find(id=re.compile(r"hot_100_era", re.I))
    if not header:
        # bazı temalarda id olmayabiliyor; ikinci yöntem: başlık yazısıyla ara
        header = soup.find(lambda t: t.name in ("h2","h3") and "Hot 100 era" in t.get_text())
        if not header:
            raise RuntimeError("Couldn't locate 'Hot 100 era' section on index page.")

    ul = header.find_next("ul")
    if not ul:
        # bazen yıllar birkaç <ul> bloğu halinde alt alta olur; önce bir kapsayıcı arayalım
        container = header.find_next()
        years = []
        # güvenli bir şekilde, header'dan sonra gelen kardeşlerinde birkaç <ul> tarayalım
        for _ in range(12):
            if container and container.name == "ul":
                for a in container.select("a[href^='/wiki/']"):
                    text = a.get_text(strip=True)
                    if text.isdigit():
                        y = int(text)
                        if y >= MIN_YEAR and y <= MAX_YEAR:
                            years.append((y, a.get("href")))
            container = container.find_next_sibling()
        if years:
            return years
        raise RuntimeError("Couldn't find year list under 'Hot 100 era'.")
    # Basit tek <ul> halinde ise:
    years = []
    for a in ul.select("a[href^='/wiki/']"):
        text = a.get_text(strip=True)
        if text.isdigit():
            y = int(text)
            if y >= MIN_YEAR and y <= MAX_YEAR:
                years.append((y, a.get("href")))
    return years

def parse_year_table(html: str, year: int) -> List[Dict]:
    """
    O yılın sayfasındaki tablo(lar)dan Issue date / Song / Artist(s) çek.
    En güvenlisi: pandas.read_html ile "Issue" başlıklı tabloyu almak.
    """
    # Pandas'a string verelim ki header’ları rahat yakalasın
    tables = pd.read_html(html)  # lxml yüklü olduğu için hızlıdır
    rows: List[Dict] = []

    # Issue date / Song kolonları olabilen tabloları dolaş
    for df in tables:
        cols = [c.lower() for c in df.columns.astype(str).tolist()]
        # Sık görülen başlık varyantları:
        # "Issue date", "Issue Date", "Date", "Song", "Single", "Artist(s)", "Artist"
        if not any("issue" in c and "date" in c for c in cols) and not "date" in cols:
            continue
        if not any("song" in c or "single" in c for c in cols):
            continue

        # Kolon adlarını normalize et
        colmap = {}
        for c in df.columns:
            cl = str(c).strip().lower()
            if "issue" in cl and "date" in cl:
                colmap["issue"] = c
            elif cl == "date":  # bazı yıllarda sadece "Date" var
                colmap["issue"] = c
            elif "song" in cl or "single" in cl:
                colmap["song"] = c
            elif "artist" in cl:
                colmap["artist"] = c

        if "issue" not in colmap or "song" not in colmap:
            continue

        for _, r in df.iterrows():
            try:
                issue_raw = clean_text(str(r[colmap["issue"]]))
                if not issue_raw or issue_raw.lower() in ("nan","—","-"):
                    continue
                # Issue date bazı yıllarda "January 5" gibi yıl belirtilmeden verilir -> yıl ekle
                issue_text = f"{issue_raw} {year}" if re.search(r"\d{4}", issue_raw) is None else issue_raw
                issue_dt = dateparser.parse(issue_text, fuzzy=True).date()
            except Exception:
                continue

            song = clean_text(str(r[colmap["song"]]))
            if not song:
                continue
            artist = clean_text(str(r[colmap["artist"]])) if "artist" in colmap else ""

            rows.append({
                "issue_date": issue_dt.isoformat(),
                "song": song,
                "artist": artist,
                "year": year,
            })

    return rows

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sess = make_session()

    print("🔎 Fetching index:", INDEX_URL)
    index_html = fetch_html(sess, INDEX_URL)
    year_links = extract_hot100_year_links(index_html)
    # Tekrarlayan/karmaşık listeleri temizle, sıralı tut
    seen = set()
    filtered = []
    for y, href in year_links:
        if (y, href) not in seen:
            filtered.append((y, href))
            seen.add((y, href))
    filtered.sort(key=lambda t: t[0])

    print(f"📅 Years found: {filtered[0][0]}–{filtered[-1][0]}  (total {len(filtered)})")

    all_rows: List[Dict] = []
    for y, href in filtered:
        url = href if href.startswith("http") else BASE_URL + href
        print(f"  → {y}: {url}")
        html = fetch_html(sess, url)
        rows = parse_year_table(html, y)
        print(f"     parsed {len(rows)} rows")
        all_rows.extend(rows)
        time.sleep(0.5)  # nazik olalım

    # Issue date -> song/artist sözlüğü
    by_date: Dict[str, Dict] = {}
    for r in all_rows:
        by_date[r["issue_date"]] = {"song": r["song"], "artist": r["artist"]}

    # Kaydet
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(by_date, f, ensure_ascii=False, indent=2)

    pd.DataFrame(all_rows).sort_values("issue_date").to_csv(CSV_OUT, index=False, encoding="utf-8")

    print(f"\n✅ Saved {len(by_date)} entries")
    print(f"   JSON: {JSON_OUT}")
    print(f"   CSV : {CSV_OUT}")

if __name__ == "__main__":
    main()
