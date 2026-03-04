import os
import re
import json
import time
from typing import List, Dict, Set, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import discord

BASE = "https://discordhome.com"
TAG_DIR_URL = "https://discordhome.com/discord-tags"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}
SLEEP_SECONDS = float(os.getenv("SCRAPE_SLEEP", "1.0"))
MAX_TAG_PAGES = int(os.getenv("MAX_TAG_PAGES", "50"))     # safety default
MAX_PAGES_PER_TAG = int(os.getenv("MAX_PAGES_PER_TAG", "2"))  # safety default

INVITE_OK = ("discord.gg/", "discord.com/invite/")

GUILD_TAG_RE = re.compile(r"Guild tag\s+([A-Za-z0-9]{2,12})", re.IGNORECASE)

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def resolve_join_to_invite(join_url: str) -> Optional[str]:
    try:
        r = requests.get(join_url, headers=HEADERS, timeout=30, allow_redirects=True)
        final = r.url
        if any(x in final for x in INVITE_OK):
            return final
        return None
    except Exception:
        return None

def collect_tag_pages() -> List[str]:
    soup = get_soup(TAG_DIR_URL)

    # “View servers” buttons link to /servers/tag/...
    tag_pages: Set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/servers/tag/" in href:
            tag_pages.add(urljoin(BASE, href))

    tag_pages_list = sorted(tag_pages)
    return tag_pages_list

def scrape_tag_page(tag_page_url: str, max_pages: int) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    next_url = tag_page_url

    for _ in range(max_pages):
        soup = get_soup(next_url)

        # Find all “Join server” links (these are /join/<id>)
        join_links = soup.find_all("a", string=re.compile(r"Join server", re.IGNORECASE))
        for a in join_links:
            join_href = a.get("href")
            if not join_href:
                continue
            join_full = urljoin(BASE, join_href)

            # Get text in the card and extract “Guild tag XYZ”
            card = a.find_parent()
            card_text = card.get_text(" ", strip=True) if card else ""
            m = GUILD_TAG_RE.search(card_text)
            if not m:
                continue

            tag = m.group(1).strip().upper()
            invite = resolve_join_to_invite(join_full)
            if invite:
                out.append({"tag": tag, "invite": invite})

            time.sleep(SLEEP_SECONDS)

        # Pagination: try to follow “Next”
        next_a = soup.find("a", string=re.compile(r"Next", re.IGNORECASE))
        if next_a and next_a.get("href"):
            next_url = urljoin(BASE, next_a["href"])
        else:
            break

    return out

async def validate_invites(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN is not set.")

    client = discord.Client(intents=discord.Intents.none())
    await client.login(token)

    valid: List[Dict[str, str]] = []
    seen_invites: Set[str] = set()

    for rec in records:
        inv = rec["invite"]
        if inv in seen_invites:
            continue
        seen_invites.add(inv)

        code = inv.rstrip("/").split("/")[-1]
        try:
            _ = await client.fetch_invite(code, with_counts=False)
            valid.append(rec)
        except Exception:
            pass

    await client.close()
    return valid

def write_tags_json(records: List[Dict[str, str]], path: str = "tags.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(records)} records to {path}")

def main():
    tag_pages = collect_tag_pages()
    print(f"Found {len(tag_pages)} tag pages in directory.")

    tag_pages = tag_pages[:MAX_TAG_PAGES]
    print(f"Scraping first {len(tag_pages)} tag pages (MAX_TAG_PAGES={MAX_TAG_PAGES}).")

    all_records: List[Dict[str, str]] = []
    for url in tag_pages:
        print("Scraping:", url)
        all_records.extend(scrape_tag_page(url, max_pages=MAX_PAGES_PER_TAG))

    print(f"Collected {len(all_records)} (tag, invite) pairs before validation.")

    import asyncio
    valid_records = asyncio.run(validate_invites(all_records))
    print(f"Validated {len(valid_records)} invites.")

    write_tags_json(valid_records)

if __name__ == "__main__":
    main()
