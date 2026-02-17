#!/usr/bin/env python3
"""
Add an `image` column to a CSV of anime by scraping MyAnimeList pages using MAL_ID.

Usage:
    python add_images_to_csv.py input.csv

Output:
    input_with_images.csv
"""

import sys
import time
import random
import csv
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# Optional: use tqdm if available for a nice progress bar
try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x, **kw: x  # fallback: identity


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AnimeImageFetcher/1.0; +https://example.local/)",
    # Add referer to look more like a browser
    "Referer": "https://myanimelist.net/",
}


def get_image_url_from_html(html: str) -> Optional[str]:
    """Parse HTML and return the best guess for cover image URL or None."""
    soup = BeautifulSoup(html, "html.parser")

    # 1) Prefer the Open Graph image
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return og["content"].strip()

    # 2) Look for <img itemprop="image"> or <img ... src=...>
    img = soup.find("img", attrs={"itemprop": "image"})
    if img and img.get("data-src"):
        return img["data-src"].strip()
    if img and img.get("src"):
        return img["src"].strip()

    # 3) fallback: first image inside #content or .leftside, etc.
    # (MAL layout may change; this is a best-effort fallback)
    for selector in ["#content img", ".leftside img", ".pic img"]:
        el = soup.select_one(selector)
        if el and el.get("src"):
            return el["src"].strip()

    return None


def fetch_image_url(mal_id, session=None):
    if not mal_id:
        return None

    try:
        url = f"https://api.jikan.moe/v4/anime/{mal_id}"
        r = requests.get(url, timeout=10)

        if r.status_code != 200:
            return None

        data = r.json()

        return data["data"]["images"]["jpg"]["image_url"]

    except Exception as e:
        print("Error:", e)
        return None


def insert_column_after(headers: list, after_col: str, new_col: str) -> list:
    """Return new headers list with new_col inserted after after_col."""
    if after_col not in headers:
        # put it at the front if MAL_ID missing
        return [new_col] + headers
    idx = headers.index(after_col)
    return headers[: idx + 1] + [new_col] + headers[idx + 1 :]


def main(infile: str):
    in_path = Path(infile)
    if not in_path.exists():
        print("Input file not found:", infile)
        return

    out_path = in_path.with_name(in_path.stem + in_path.suffix)

    # Read original CSV headers (don't infer types yet)
    with in_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            print("Input CSV is empty")
            return

    # Prepare output headers with 'image' inserted after 'MAL_ID'
    out_headers = insert_column_after(headers, "MAL_ID", "image")

    # We'll stream read and write to avoid huge memory usage.
    # If output exists, we'll read it to avoid refetching already-fetched images.
    already_done = {}
    if out_path.exists():
        print(f"Found existing output {out_path}. Loading existing image values to resume...")
        with out_path.open("r", encoding="utf-8", newline="") as f_out:
            r = csv.DictReader(f_out)
            for row in r:
                mid = row.get("MAL_ID")
                if mid:
                    already_done[mid] = row.get("image")

    session = requests.Session()

    rows_processed = 0
    save_every = 50  # flush to disk every N rows

    # We'll read input rows and write to a temp file, and replace final on completion.
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")

    with in_path.open("r", encoding="utf-8-sig", newline="") as f_in, temp_path.open("w", encoding="utf-8", newline="") as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=out_headers, extrasaction="ignore")
        writer.writeheader()

        for row in tqdm(list(reader), desc="Rows", unit="row"):
            rows_processed += 1
            mal_id = str(row.get("MAL_ID", "")).strip()

            # If we already have this MAL_ID in existing out file, reuse it
            image_url = None
            if mal_id in already_done:
                image_url = already_done[mal_id]

            if not image_url:
                image_url = fetch_image_url(mal_id, session)

                # polite pause to avoid hammering the site
                time.sleep(random.uniform(1.0, 1.6))

            # attach image to row
            new_row = {}
            for col in out_headers:
                if col == "image":
                    new_row["image"] = image_url or ""
                else:
                    new_row[col] = row.get(col, "")

            writer.writerow(new_row)

            if rows_processed % save_every == 0:
                f_out.flush()
                print(f"Saved progress: {rows_processed} rows")

    # replace final output
    temp_path.replace(out_path)
    print(f"All done. Output saved to: {out_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python add_images_to_csv.py input.csv")
        sys.exit(1)
    main(sys.argv[1])
