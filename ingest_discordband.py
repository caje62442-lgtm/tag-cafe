import os
import re
import json
import time
from typing import List, Dict, Set, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Optional invite validation (recommended so you don't store dead invites)
# If you don't want validation, set VALIDATE_INVITES=0 in env.
VALIDATE_INVITES = os.getenv("VALIDATE_INVITES", "1").strip() not in ("0", "false", "False")

# --------------------------
# Config (env-tunable)
# --------------------------
BASE = "https://discord.band"
START_URL = BASE + "/tags/4-characters"

MAX_PAGES = int(os.getenv("MAX_PAGES", "20"))                 # how many pages to crawl
SLEEP_SECONDS = float(os.getenv("SCRAPE_SLEEP", "1.0"))       # rate limit
OUT_FILE = os.getenv("OUT_FILE", "tags.json")

# If you only want simple ASCII tags like PAWS / MEOW, keep this = 1
# If you want unicode tags too, set ONLY_ASCII_TAGS=0
ONLY_ASCII_TAGS = os.getenv("ONLY_ASCII_TAGS", "1").strip() not in ("0", "false", "False")

# If you want to keep duplicates (same tag pointing to multiple servers), keep as 1 (recommended for your pager)
KEEP_DUPLICATE_TAGS = os.getenv("KEEP_DUPLICATE_TAGS", "1").strip() not in ("0", "false", "False")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

INVITE_RE = re.compile(
    r"(https?://(?:www\.)?(?:discord\.gg|discord\.com/invite)/[A-Za-z0-9-]+)",
    re.IGNORECASE,
)

ASCII_TAG_RE = re.compile(r"^[A-Za-z0-9]{4}$")

# --------------------------
# HTTP + parsing helpers
# --------------------------
def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def page_url(page: int) -> str:
    # discord.band uses /tags/4-characters/page/<n>
    if page <= 1:
        return START_URL
    return f"{START_URL}/page/{page}"

def extract_tag_and_invites_from_page(url: str) -> List[Tuple[str, str]]:
    soup = get_soup(url)

    # Strategy:
    # - Pull all invite links (discord.gg / discord.com/invite)
    # - For each invite link, find the nearest preceding tag label in the listing block.
    #
    # The HTML can change; this is intentionally defensive:
    # - We look at the parent container text and choose the best 4-char token.

    pairs: List[Tuple[str, str]] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        m = INVITE_RE.search(href) or INVITE_RE.search(a.get_text(" ", strip=True))
        if not m:
            continue

        invite = m.group(1)

        # Grab a chunk of surrounding text to find the displayed tag.
        # The tag itself is usually rendered near the invite button inside the same card.
        container = a
        # Walk up a few levels to get a meaningful “card” text.
        for _ in range(4):
            if container.parent:
                container = container.parent
            else:
                break

        blob = container.get_text(" ", strip=True)

        # Find candidate 4-char tokens in the blob
        candidates = []
        for token in re.findall(r"\S+", blob):
            token_clean = token.strip().strip("[](){}<>!@#$%^&*_=+|;:'\",.?/\\")
            if len(token_clean) == 4:
                candidates.append(token_clean)

        # Prefer exact ASCII 4-char tags (PAWS, MEOW, etc.)
        tag = None
        if candidates:
            # Try to find the first that matches ASCII_TAG_RE
            for c in candidates:
                if ASCII_TAG_RE.match(c):
                    tag = c
                    break
            # Otherwise take the first 4-char candidate (for unicode tags)
            if tag is None:
                tag = candidates[0]

        if tag is None:
            continue

        if ONLY_ASCII_TAGS and not ASCII_TAG_RE.match(tag):
            continue

        pairs.append((tag.upper(), invite))

    return pairs

# --------------------------
# Optional invite validation
# --------------------------
async def validate_invites(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    import discord

    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("DISCORD_TOKEN is not set (needed for VALIDATE_INVITES=1).")

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
            # with_counts=False = faster and avoids storing member counts
            await client.fetch_invite(code, with_counts=False)
            valid.append(rec)
        except Exception:
            pass

    await client.close()
    return valid

# --------------------------
# Main
# --------------------------
def main():
    print(f"RUN CONFIG: MAX_PAGES={MAX_PAGES} SCRAPE_SLEEP={SLEEP_SECONDS} "
          f"ONLY_ASCII_TAGS={int(ONLY_ASCII_TAGS)} VALIDATE_INVITES={int(VALIDATE_INVITES)} "
          f"KEEP_DUPLICATE_TAGS={int(KEEP_DUPLICATE_TAGS)}")

    collected: List[Dict[str, str]] = []
    seen_pairs: Set[Tuple[str, str]] = set()

    for p in range(1, MAX_PAGES + 1):
        url = page_url(p)
        print(f"GET {url}")
        try:
            pairs = extract_tag_and_invites_from_page(url)
        except Exception as e:
            print(f"ERROR page={p}: {repr(e)}")
            break

        print(f"FOUND {len(pairs)} (tag, invite) pairs on page {p}")

        for tag, invite in pairs:
            key = (tag, invite)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)

            collected.append({"tag": tag, "invite": invite})

        time.sleep(SLEEP_SECONDS)

    print(f"Collected {len(collected)} candidates before validation.")

    # If you want duplicates by tag (multiple servers per same tag), do nothing.
    # If you ever want to collapse tags, set KEEP_DUPLICATE_TAGS=0 and de-dupe by tag here.
    if not KEEP_DUPLICATE_TAGS:
        best_by_tag: Dict[str, Dict[str, str]] = {}
        for r in collected:
            # First one wins (already sorted-ish by site order)
            best_by_tag.setdefault(r["tag"], r)
        collected = list(best_by_tag.values())
        print(f"Collapsed to {len(collected)} unique tags (KEEP_DUPLICATE_TAGS=0).")

    if VALIDATE_INVITES:
        import asyncio
        collected = asyncio.run(validate_invites(collected))
        print(f"Validated {len(collected)} invites.")

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(collected, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(collected)} records to {OUT_FILE}")

if __name__ == "__main__":
    main()
