import os
import re
import json
import time
from typing import List, Dict, Set, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import discord

BASE = "https://disboard.org"
SLEEP_SECONDS = float(os.getenv("SCRAPE_SLEEP", "1.0"))
MAX_TAG_PAGES = int(os.getenv("MAX_TAG_PAGES", "20"))              # how many tag pages to scrape per tag
MAX_SERVERS_PER_TAG = int(os.getenv("MAX_SERVERS_PER_TAG", "50"))  # cap server pages per tag page
MAX_TAGS = int(os.getenv("MAX_TAGS", "50"))                        # how many tag keywords to process

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

# Example tag page format on DISBOARD:
# https://disboard.org/servers/tag/<tag>?sort=-member_count&page=<n>
TAG_URL_TMPL = BASE + "/servers/tag/{tag}?sort=-member_count&page={page}"

INVITE_RE = re.compile(
    r"(https?://(?:www\.)?(?:discord\.gg|discord\.com/invite)/[A-Za-z0-9-]+)",
    re.IGNORECASE,
)

# Starter tags (edit as you like)
SEED_TAGS = [
    "gaming", "anime", "music", "memes", "roleplay", "art", "minecraft", "valorant",
    "genshin-impact", "fortnite", "csgo", "league-of-legends", "roblox", "pokemon",
    "technology", "coding", "community", "social", "chill", "18", "movies", "books",
]


def get_soup(url: str) -> BeautifulSoup:
    """
    Fetch HTML and ALWAYS log status + a small snippet.
    This makes it obvious when DISBOARD blocks GitHub Actions (403/Cloudflare/etc.).
    """
    r = requests.get(url, headers=HEADERS, timeout=30)

    # Always log response diagnostics (even on non-200).
    text = (r.text or "").replace("\n", " ").strip()
    print(f"GET {url} -> {r.status_code}")
    print("RESP SNIP:", text[:200])

    # If blocked, this will raise (and our caller will log the exception)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def extract_server_links_from_tag_page(tag: str, page: int) -> List[str]:
    url = TAG_URL_TMPL.format(tag=tag, page=page)
    soup = get_soup(url)

    # Optional extra visibility (keep if you want)
    print("TAG PAGE TITLE:", soup.title.string if soup.title else "NO TITLE")

    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # DISBOARD server pages look like /server/<id>
        if href.startswith("/server/"):
            links.append(urljoin(BASE, href))

    # de-dupe while preserving order
    seen: Set[str] = set()
    out: List[str] = []
    for x in links:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def extract_invite_from_server_page(server_url: str) -> Optional[str]:
    soup = get_soup(server_url)
    text = soup.get_text(" ", strip=True)

    m = INVITE_RE.search(text)
    if m:
        return m.group(1)

    # Sometimes invite is in href attributes
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "discord.gg/" in href or "discord.com/invite/" in href:
            return href

    return None


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
    tags = SEED_TAGS[:MAX_TAGS]
    all_records: List[Dict[str, str]] = []

    print("RUN CONFIG:",
          f"MAX_TAGS={MAX_TAGS}",
          f"MAX_TAG_PAGES={MAX_TAG_PAGES}",
          f"MAX_SERVERS_PER_TAG={MAX_SERVERS_PER_TAG}",
          f"SLEEP_SECONDS={SLEEP_SECONDS}",
          f"SEED_TAGS_COUNT={len(SEED_TAGS)}")

    for tag in tags:
        print(f"\n=== TAG: {tag} ===")
        for page in range(1, MAX_TAG_PAGES + 1):
            try:
                server_links = extract_server_links_from_tag_page(tag, page)
            except Exception as e:
                print(f"ERROR fetching tag page tag={tag} page={page}: {repr(e)}")
                break

            if not server_links:
                print(f"No server links found for tag={tag} page={page}. Stopping this tag.")
                break

            server_links = server_links[:MAX_SERVERS_PER_TAG]
            print(f"Found {len(server_links)} server pages on tag={tag} page={page} (capped).")

            for s in server_links:
                try:
                    invite = extract_invite_from_server_page(s)
                    if invite:
                        all_records.append({"tag": tag.upper(), "invite": invite})
                except Exception as e:
                    print(f"ERROR fetching server page {s}: {repr(e)}")

                time.sleep(SLEEP_SECONDS)

    print(f"\nCollected {len(all_records)} candidates before validation.")

    import asyncio
    valid = asyncio.run(validate_invites(all_records))
    print(f"Validated {len(valid)} invites.")
    write_tags_json(valid)


if __name__ == "__main__":
    main()
