#!/usr/bin/env python3
"""
import.py

Usage:
    python import.py anime.csv [anime.db]

Features:
- Anime table has internal id (primary key) AND mal_id column.
- Skips Demographic, Popularity, and Source columns.
- Always inserts genres into Genres and links via AnimeGenres.
- Robust parsing of genre lists.
"""

import sqlite3
import csv
import ast
import re
import sys
from pathlib import Path
from typing import List, Optional

csv.field_size_limit(sys.maxsize)


# ---------- Parsing helpers ----------

def parse_genres(raw: str) -> List[str]:
    if not raw:
        return []

    # Remove line breaks and strip whitespace
    s = str(raw).replace("\n", " ").strip()
    if not s or s == "[]":
        return []

    # Remove wrapping double quotes
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()

    # Replace single quotes with double quotes for JSON-like parsing
    s_json = s.replace("'", '"')

    # Try parsing as JSON array
    import json
    try:
        val = json.loads(s_json)
        if isinstance(val, list):
            return [str(x).strip() for x in val if str(x).strip()]
    except:
        pass

    # Fallback: regex to capture words
    found = re.findall(r"[A-Za-z0-9\s\-]+", s)
    return [f.strip() for f in found if f.strip()]



def parse_episodes(raw: str) -> Optional[int]:
    if not raw:
        return None
    try:
        return int(float(raw))
    except:
        m = re.search(r"\d+", str(raw))
        return int(m.group()) if m else None


def parse_score(raw: str) -> Optional[float]:
    if not raw:
        return None
    try:
        return float(raw)
    except:
        return None


# ---------- Schema ----------

def create_schema(cur):
    cur.executescript("""
    PRAGMA foreign_keys = ON;

    DROP TABLE IF EXISTS AnimeGenres;
    DROP TABLE IF EXISTS Genres;
    DROP TABLE IF EXISTS Anime;

    CREATE TABLE Anime (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mal_id INTEGER,
        image TEXT,
        title TEXT,
        release_date TEXT,
        synopsis TEXT,
        score REAL,
        episodes INTEGER,
        studio TEXT,
        theme TEXT
    );

    CREATE TABLE Genres (
        genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    );

    CREATE TABLE AnimeGenres (
        anime_id INTEGER,
        genre_id INTEGER,
        PRIMARY KEY (anime_id, genre_id),
        FOREIGN KEY (anime_id) REFERENCES Anime(id),
        FOREIGN KEY (genre_id) REFERENCES Genres(genre_id)
    );
    """)


# ---------- Import ----------

def import_csv_to_db(csv_path: Path, db_path: Path):
    print("CSV:", csv_path)
    print("DB :", db_path)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    create_schema(cur)
    conn.commit()

    insert_anime_sql = """
    INSERT INTO Anime
    (mal_id, image, title, release_date, synopsis, score, episodes, studio, theme)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    insert_genre_sql = "INSERT OR IGNORE INTO Genres (name) VALUES (?)"
    select_genre_sql = "SELECT genre_id FROM Genres WHERE name = ?"
    insert_link_sql = "INSERT OR IGNORE INTO AnimeGenres (anime_id, genre_id) VALUES (?, ?)"

    inserted = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        # normalize headers
        field_map = {h.strip().lower(): h for h in reader.fieldnames if h}

        print("\nDetected CSV columns:")
        for k in field_map:
            print(" -", k)

        def g(row, *possible_keys):
            for key in possible_keys:
                k = field_map.get(key.lower())
                if k:
                    return (row.get(k) or "")
            return ""

        for i, row in enumerate(reader, start=1):
            raw_mid = g(row, "mal_id", "mal id", "id").strip()
            if not raw_mid:
                continue

            try:
                mal_id = int(float(raw_mid))
            except:
                continue

            try:
                # Insert Anime
                cur.execute(insert_anime_sql, (
                    mal_id,
                    g(row, "image"),
                    g(row, "title", "name"),
                    g(row, "release", "release_date", "aired"),
                    g(row, "synopsis", "description"),
                    parse_score(g(row, "score", "rating")),
                    parse_episodes(g(row, "episodes", "eps")),
                    g(row, "studio", "studios"),
                    g(row, "theme", "themes"),
                ))

                anime_id = cur.lastrowid

                # ---------- GENRES ----------
                raw_genre = g(row, "genre", "genres")
                genres_list = parse_genres(raw_genre)
                for genre in genres_list:
                    cur.execute(insert_genre_sql, (genre,))
                    cur.execute(select_genre_sql, (genre,))
                    gid = cur.fetchone()
                    if gid:
                        cur.execute(insert_link_sql, (anime_id, gid[0]))

                inserted += 1

                if inserted % 500 == 0:
                    conn.commit()
                    print("Inserted", inserted)

            except Exception as e:
                print(f"Row {i} skipped:", e)

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM Anime")
    print("\n✅ Import complete")
    print("Rows in Anime:", cur.fetchone()[0])
    conn.close()


# ---------- Main ----------

def main(argv):
    if len(argv) < 2:
        print("Usage: python import.py anime.csv [anime.db]")
        return

    csv_path = Path(argv[1])
    db_path = Path(argv[2]) if len(argv) > 2 else csv_path.with_name("anime.db")

    import_csv_to_db(csv_path, db_path)


if __name__ == "__main__":
    main(sys.argv)
