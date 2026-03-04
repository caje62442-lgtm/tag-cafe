import os
import re
import json
import time
from typing import List, Dict, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup

# Optional: validate invites via Discord API (discord.py)
# pip install -r requirements_ingest.txt should include discord.py if you enable this
VALIDATE_INVITES = os.getenv("VALIDATE_INVITES", "0").strip() == "1"

# =========================
# Config (env overridable)
# =========================
BASE = "https://discord.band"
START_PATH = "/tags"

MAX_LIST_PAGES = int(os.getenv("MAX_LIST_PAGES", "50"))      # pages per listing (tags, a-z, etc.)
SCRAPE_SLEEP = float(os.getenv("SCRAPE_SLEEP", "1.0"))       # seconds between HTTP requests
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))
OUT_FILE = os.getenv("OUT_FILE", "tags.json")

# If 1, also crawl the filter pages at the top of /tags (A–Z, 2-char, etc.)
CRAWL_FILTER_PAGES = os.getenv("CRAWL_FILTER_PAGES", "1").strip() == "1"

HEADERS = {
    "User-Agent": os.getenv(
        "SCRAPE_UA",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

INVITE_RE = re.compile(
    r"(https?://)?(www\.)?(discord\.gg|discord\.com/invite)/(?P<code>[A-Za-z0-9-]+)",
    re.IGNORECASE,
)

# =========================
# HTTP helpers
# =========================
_session = requests.Session()
_session.headers.update(HEADERS)

def sleep():
    if SCRAPE_SLEEP > 0:
        time.sleep(SCRAPE_SLEEP)

def get_soup(url: str) -> BeautifulSoup:
    r = _session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def set_query_param(url: str, key: str, value: str) -> str:
    """
    Return url with ?key=value updated (preserving other params).
    """
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    q[key] = [value]
    new_query = urlencode(q, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

# =========================
# Parsing logic
# =========================
def normalize_tag(tag: str) -> str:
    # Your bot should search case-insensitively; storing uppercase keeps things consistent.
    return tag.strip().upper()

def normalize_invite(inv: str) -> Optional[str]:
    inv = inv.strip()
    if not inv:
        return None
    m = INVITE_RE.search(inv)
    if not m:
        return None
    code = m.group("code")
    return f"https://discord.gg/{code}"

def is_probable_tag_token(s: str) -> bool:
    """
    Heuristic: discord "server tags" are usually short (2–4),
    but discord.band includes unicode tags too.
    We'll allow up to 10 chars, no spaces, not a pure number, not a URL-ish token.
    """
    s = s.strip()
    if not s:
        return False
    if " " in s:
        return False
    if s.isdigit():
        return False
    if s.lower() in {"join", "login", "tag", "tags"}:
        return False
    if "http" in s.lower() or "discord.gg" in s.lower() or "discord.com" in s.lower():
        return False
    # Very long tokens are probably names, not tags
    if len(s) > 10:
        return False
    return True

def extract_card_tag(card: BeautifulSoup) -> Optional[str]:
    """
    Try multiple strategies to find the tag displayed on the server card.
    """
    # Strategy 1: images sometimes have alt text containing the tag.
    # Example from the /tags page: "Image: NYA" then "NYA". :contentReference[oaicite:1]{index=1}
    imgs = card.find_all("img", alt=True)
    for img in imgs:
        alt = (img.get("alt") or "").strip()
        # alt often equals the tag directly or includes it
        # Keep it conservative: prefer short, single-token alts
        if is_probable_tag_token(alt):
            return normalize_tag(alt)

    # Strategy 2: scan visible text tokens inside the card
    tokens: List[str] = []
    for t in card.stripped_strings:
        tokens.append(t.strip())

    # Often the tag appears very early; choose the first plausible tag token.
    for tok in tokens:
        if is_probable_tag_token(tok):
            return normalize_tag(tok)

    return None

def extract_card_invite(card: BeautifulSoup) -> Optional[str]:
    """
    Find a discord invite link inside the card (anchor href or visible text).
    """
    # Prefer hrefs
    for a in card.find_all("a", href=True):
        href = a["href"].strip()
        inv = normalize_invite(href)
        if inv:
            return inv

    # Fallback: sometimes invite appears in text (less common)
    text = " ".join(list(card.stripped_strings))
    m = INVITE_RE.search(text)
    if m:
        return f"https://discord.gg/{m.group('code')}"
    return None

def extract_records_from_listing_page(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """
    Extract {tag, invite} from a listing page like /tags or a filter page.

    The page includes multiple server "cards", each with a tag and a Join link. :contentReference[oaicite:2]{index=2}
    """
    records: List[Dict[str, str]] = []

    # We find anchors that look like invites, then climb to a parent container to parse the whole card.
    invite_anchors = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if INVITE_RE.search(href):
            invite_anchors.append(a)

    # If invites aren’t in hrefs (rare), we still try a broad card approach:
    # find blocks that contain the word "Join" and attempt parse.
    if not invite_anchors:
        for a in soup.find_all("a"):
            if a.get_text(strip=True).lower() == "join":
                invite_anchors.append(a)

    seen_cards: Set[int] = set()
    for a in invite_anchors:
        # Try to find a stable card container. We walk up a few levels.
        card = a
        for _ in range(6):
            parent = card.parent
            if not parent:
                break
            card = parent

        # Dedup by object id (bs4 element identity)
        card_id = id(card)
        if card_id in seen_cards:
            continue
        seen_cards.add(card_id)

        tag = extract_card_tag(card)
        invite = extract_card_invite(card)

        if tag and invite:
            records.append({"tag": tag, "invite": invite})

    # Final dedupe (tag+invite)
    out: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for r in records:
        key = (r["tag"], r["invite"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    return out

def discover_filter_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    """
    On /tags, there are filter buttons like A–Z Tags, 2-Character Tags, etc. :contentReference[oaicite:3]{index=3}
    We collect those URLs so we can crawl them too.
    """
    urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # keep only internal links under /tags
        if href.startswith("/tags"):
            urls.append(urljoin(base_url, href))
    # Dedup preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

# =========================
# Optional validation
# =========================
async def validate_with_discord(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Validate invite codes via Discord API.
    Does NOT store member count; only keeps the record if invite resolves.
    """
    import discord  # imported only if used

    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN is not set (required for VALIDATE_INVITES=1).")

    client = discord.Client(intents=discord.Intents.none())
    await client.login(token)

    valid: List[Dict[str, str]] = []
    seen_inv: Set[str] = set()

    for rec in records:
        inv = rec["invite"]
        if inv in seen_inv:
            continue
        seen_inv.add(inv)

        code = inv.rstrip("/").split("/")[-1]
        try:
            # with_counts=False ensures you’re not even requesting counts
            await client.fetch_invite(code, with_counts=False)
            valid.append(rec)
        except Exception:
            pass

    await client.close()
    return valid

# =========================
# Main crawl
# =========================
def crawl_listing(list_url: str, max_pages: int) -> List[Dict[str, str]]:
    """
    Crawl list_url, list_url?page=2, etc., collecting tag+invite records.
    """
    all_records: List[Dict[str, str]] = []
    for page in range(1, max_pages + 1):
        url = list_url if page == 1 else set_query_param(list_url, "page", str(page))
        print(f"LIST PAGE: {url}")
        try:
            soup = get_soup(url)
        except Exception as e:
            print(f"STOP (fetch failed) url={url} err={repr(e)}")
            break

        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        # If we ever get a “not authorized” footer, the content can still exist above it.
        # We don’t hard-stop based on that text; we stop if we extract nothing repeatedly.
        print(f"TITLE: {title}")

        records = extract_records_from_listing_page(soup)
        print(f"FOUND RECORDS: {len(records)} on page {page}")

        if not records:
            # No records usually means end of pagination or blocked content.
            print(f"STOP (no records) at page {page}")
            break

        all_records.extend(records)
        sleep()

    # Dedup overall
    seen: Set[Tuple[str, str]] = set()
    out: List[Dict[str, str]] = []
    for r in all_records:
        k = (r["tag"], r["invite"])
        if k in seen:
            continue
        seen.add(k)
        out.append(r)

    return out

def write_tags_json(records: List[Dict[str, str]], path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(records)} records to {path}")

def main():
    start_url = urljoin(BASE, START_PATH)
    print(f"START: {start_url}")

    # Always crawl main /tags
    soup0 = get_soup(start_url)
    targets = [start_url]

    # Also crawl filter pages (A–Z, 2-char, etc.) from the top buttons. :contentReference[oaicite:4]{index=4}
    if CRAWL_FILTER_PAGES:
        filters = discover_filter_urls(soup0, BASE)
        # Keep only a reasonable subset: “Tag List” pages can include internal duplicates.
        # Still safe to crawl; we dedupe.
        for u in filters:
            if u not in targets:
                targets.append(u)

    print("TARGET LIST PAGES:")
    for t in targets[:30]:
        print(" -", t)
    if len(targets) > 30:
        print(f" (+ {len(targets)-30} more)")

    all_records: List[Dict[str, str]] = []
    for t in targets:
        recs = crawl_listing(t, MAX_LIST_PAGES)
        all_records.extend(recs)

    # Global dedupe
    seen: Set[Tuple[str, str]] = set()
    deduped: List[Dict[str, str]] = []
    for r in all_records:
        k = (r["tag"], r["invite"])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)

    print(f"Collected {len(deduped)} records before validation.")

    if VALIDATE_INVITES:
        import asyncio
        deduped = asyncio.run(validate_with_discord(deduped))
        print(f"Validated {len(deduped)} invites.")

    write_tags_json(deduped, OUT_FILE)

if __name__ == "__main__":
    main()
