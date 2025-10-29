def pick_hot100_tables(soup: BeautifulSoup) -> List[Tag]:
    """
    Yıl sayfasındaki 'wikitable' tablolardan, başlıklarında tarih + şarkı + artist geçenleri seç.
    Başlık adları yıllara göre değişebildiği için geniş eşleştirme kullan.
    """
    wanted = []
    for tbl in soup.find_all("table", class_=lambda c: c and "wikitable" in c):
        # En mantıklı başlık satırı: thead içindeki th'ler ya da ilk tr'nin th'leri
        header_tr = None
        thead = tbl.find("thead")
        if thead:
            header_tr = thead.find("tr")
        if header_tr is None:
            # fallback: ilk th içeren satır
            for tr in tbl.find_all("tr"):
                if tr.find("th"):
                    header_tr = tr
                    break
        if header_tr is None:
            continue

        heads = [normalize_space(th.get_text(" ")) for th in header_tr.find_all("th")]
        hl = " | ".join(h.lower() for h in heads)

        has_date   = any(k in hl for k in ["issue date", "chart date", "week ending", "week date", "date"])
        has_song   = any(k in hl for k in ["song", "single", "title"])
        has_artist = any(k in hl for k in ["artist", "artist(s)"])

        if has_date and has_song and has_artist:
            wanted.append(tbl)

    return wanted


def parse_year_page(html: str, year: int) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    tables = pick_hot100_tables(soup)
    rows: List[Dict[str, Any]] = []

    if not tables:
        return rows

    # header anahtarlarını normalize edecek yardımcı
    def header_key(s: str) -> str:
        s = normalize_space(s).lower()
        s = s.replace("single", "song")       # single -> song
        s = s.replace("title", "song")        # title  -> song
        s = s.replace("artist(s)", "artist")
        s = s.replace("chart date", "date")
        s = s.replace("issue date", "date")
        s = s.replace("week ending", "date")
        s = s.replace("week date", "date")
        return s

    for tbl in tables:
        # başlık indekslerini çıkar
        header_tr = None
        thead = tbl.find("thead")
        if thead:
            header_tr = thead.find("tr")
        if header_tr is None:
            for tr in tbl.find_all("tr"):
                if tr.find("th"):
                    header_tr = tr
                    break
        if header_tr is None:
            continue

        headers = [header_key(th.get_text(" ")) for th in header_tr.find_all("th")]
        # indeks adayları
        def find_col(keys: List[str]) -> Optional[int]:
            for i, h in enumerate(headers):
                for k in keys:
                    if k in h:
                        return i
            return None

        idx_date   = find_col(["date"])
        idx_song   = find_col(["song"])
        idx_artist = find_col(["artist"])

        # satırları dolaş
        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            # başlığın altında olmayan satırlar olabilir (altbaşlıklar vs.)
            cells_txt = [normalize_space(td.get_text(" ")) for td in tds]

            # indeksler eksikse kaba fallback
            _id = idx_date if idx_date is not None and idx_date < len(tds) else 0
            _is = idx_song if idx_song is not None and idx_song < len(tds) else min(1, len(tds)-1)
            _ia = idx_artist if idx_artist is not None and idx_artist < len(tds) else min(2, len(tds)-1)

            raw_date   = cells_txt[_id]
            raw_artist = cells_txt[_ia]

            # şarkı adını hücreden daha temiz al
            song_cell = tds[_is]
            song_text = normalize_space(song_cell.get_text(" "))
            # italik içinde ad varsa onu tercih et
            it = song_cell.find("i")
            if it:
                st = normalize_space(it.get_text(" "))
                if st:
                    song_text = st

            if not song_text:
                continue

            # Tarihi ISO'ya çevir; yıl yoksa 'year' ekle
            iso = parse_date(raw_date)
            if iso is None:
                # bazen 'Jan 6' gibi yalnızca ay-gün gelir: year'i eklemeyi dene
                date_with_year = f"{raw_date} {year}"
                iso = parse_date(date_with_year)
            if iso is None:
                continue

            # YYYY-MM-DD'ye dönmüş olmalı
            try:
                dt = datetime.datetime.strptime(iso, "%Y-%m-%d").date()
            except ValueError:
                continue

            rows.append({
                "issue_date": dt.isoformat(),
                "year": dt.year,
                "song": song_text,
                "artist": raw_artist,
                "source": "wikipedia"
            })

    # dedup + yıl filtresi + sıralama
    dedup = {}
    for r in rows:
        key = (r["issue_date"], r["song"].lower(), r["artist"].lower())
        if key not in dedup:
            dedup[key] = r
    rows = [r for r in dedup.values() if FIRST_YEAR <= r["year"] <= CURRENT_YEAR]
    rows.sort(key=lambda r: r["issue_date"])
    return rows
