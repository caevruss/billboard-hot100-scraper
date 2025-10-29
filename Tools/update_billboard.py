import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

OUTPUT = "DataSources/billboard_hot100_weekly.csv"

URL = "https://en.wikipedia.org/wiki/Lists_of_Billboard_number-one_singles"

def fetch_table():
    print("🔍 Fetching Billboard table from Wikipedia...")
    r = requests.get(URL, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    tables = soup.find_all("table", {"class": "wikitable"})
    print(f"Found {len(tables)} tables.")

    all_rows = []
    for table in tables:
        df = pd.read_html(str(table))[0]
        for _, row in df.iterrows():
            if "Date" in df.columns[0]:
                start = str(row[df.columns[0]]).split("–")[0].strip()
                song  = str(row.get("Song", row.get("Title", ""))).strip("“”\"'")
                artist = str(row.get("Artist(s)", row.get("Artist", "")))
                if song and artist:
                    all_rows.append({
                        "start": start,
                        "title": song,
                        "artist": artist
                    })
    print(f"Parsed {len(all_rows)} entries.")
    return all_rows

def save_csv(rows):
    df = pd.DataFrame(rows)
    df.drop_duplicates(subset=["start"], inplace=True)
    df.to_csv(OUTPUT, index=False)
    print(f"✅ Saved {len(df)} rows to {OUTPUT}")

if __name__ == "__main__":
    try:
        data = fetch_table()
        if data:
            save_csv(data)
        else:
            print("⚠️ No data fetched.")
    except Exception as e:
        print("❌ Error:", e)

