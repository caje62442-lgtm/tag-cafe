import os
import re
import json
import time
from typing import List, Dict, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup

# =========================
# Config (env overridable)
# =========================
BASE = "https://discord.band"
OUT_FILE = os.getenv("OUT_FILE", "tags.json")

SCRAPE_SLEEP = float(os.getenv("SCRAPE_SLEEP", "1.0"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))

# How many pages to crawl per section
MAX_PAGES_PER_SECTION = int(os.getenv("MAX_PAGES_PER_SECTION", "50"))

# If the run produces fewer than this many NEW records, do not overwrite the DB.
# (Prevents “oops it wrote 1 record and deleted 5K lines.”)
MIN_TOTAL_RECORDS_TO_WRITE = int(os.getenv("MIN_TOTAL_RECORDS_TO_WRITE", "1000"))
MIN_NEW_RECORDS_TO_WRITE = int(os.getenv("MIN_NEW_RECORDS_TO_WRITE", "200"))

# Keep duplicates by tag (you want prev/next cycling multiple servers per tag)
KEEP_DUPLICATE_TAGS = os.getenv("KEEP_DUPLICATE_TAGS", "1").strip() not in ("0", "false", "False")

# Optional validation via Discord API
VALIDATE_INVITES = os.getenv("VALIDATE_INVITES", "0").strip() == "1"

HEADERS = {
    "User-Agent": os.getenv(
        "SCRAPE_UA",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

INVITE_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:discord\.gg|discord\.com/invite)/(?P<code>[A-Za-z0-9-]+)",
    re.IGNORECASE,
)

# Discord “guild tag” tokens: allow A–Z/0–9 and short unicode-ish tokens.
# discord.band has unicode tags too; keep it permissive but avoid obvious junk.
def is_plausible_tag(token: str) -> bool:
    t = token.strip()
    if not t:
        return False
    if " " in t:
        return False
    if t.lower() in {"join", "tags", "tag", "discord", "server", "servers"}:
        return False
    if "http" in t.lower() or "discord.gg" in t.lower() or "discord.com" in t.lower():
        return False
    # Discord tags are typically short. discord.band lists 2–4 char groups heavily.
    if len(t) < 2 or len(t) > 10:
        return False
    return True

def normalize_tag(tag: str) -> str:
    # Store uppercase so your bot search can be case-insensitive while data is consistent.
    return tag.strip().upper()

def normalize_invite(url_or_code: str) -> Optional[str]:
    s = url_or_code.strip()
    if not s:
        return None
    m = INVITE_RE.search(s)
    if not m:
        return None
    code = m.group("code")
    return f"https://discord.gg/{code}"

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
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    q[key] = [value]
    new_query = urlencode(q, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

def load_existing(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            out = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                tag = str(item.get("tag", "")).strip()
                invite = str(item.get("invite", "")).strip()
                if tag and invite:
                    out.append({"tag": normalize_tag(tag), "invite": normalize_invite(invite) or invite})
            return out
    except Exception:
        pass
    return []

def write_backup(path: str):
    if not os.path.exists(path):
        return
    backup = path.replace(".json", "") + ".bak.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            old = f.read()
        with open(backup, "w", encoding="utf-8") as f:
            f.write(old)
        print(f"Backup written: {backup}")
    except Exception as e:
        print(f"Backup failed: {repr(e)}")

def nearest_tag_from_container(container: BeautifulSoup) -> Optional[str]:
    """
    Find the tag text near an invite button.
    We scan short tokens in container text and pick the first plausible one.
    """
    # Prefer alt text of images first (often the tag is an image)
    for img in container.find_all("img", alt=True):
        alt = (img.get("alt") or "").strip()
        if is_plausible_tag(alt):
            return normalize_tag(alt)

    # Then scan text tokens
    text = container.get_text(" ", strip=True)
    tokens = [t.strip("[](){}<>!@#$%^&*_=+|;:'\",.?/\\") for t in text.split()]
    for tok in tokens:
        if is_plausible_tag(tok):
            return normalize_tag(tok)

    return None

def extract_records_from_page(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """
    Strategy:
    - find all anchors linking to discord.gg / discord.com/invite
    - for each anchor, walk up a few parents to get the local card container
    - extract tag from that container
    """
    records: List[Dict[str, str]] = []
    seen_pairs: Set[Tuple[str, str]] = set()

    invite_anchors = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if INVITE_RE.search(href):
            invite_anchors.append(a)

    for a in invite_anchors:
        inv = normalize_invite(a["href"])
        if not inv:
            continue

        # Walk up to a “card-ish” container but not the whole page.
        container = a
        for _ in range(5):
            if container.parent:
                container = container.parent
            else:
                break

        tag = nearest_tag_from_container(container)
        if not tag:
            continue

        key = (tag, inv)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        records.append({"tag": tag, "invite": inv})

    return records

def candidate_page_urls(section_url: str, page: int) -> List[str]:
    """
    discord.band sometimes uses /page/N and sometimes ?page=N.
    Try both.
    """
    if page <= 1:
        return [section_url]
    a = section_url.rstrip("/") + f"/page/{page}"
    b = set_query_param(section_url, "page", str(page))
    return [a, b]

def crawl_section(section_url: str, max_pages: int) -> List[Dict[str, str]]:
    all_records: List[Dict[str, str]] = []
    empty_streak = 0

    for page in range(1, max_pages + 1):
        urls = candidate_page_urls(section_url, page)
        best_records: List[Dict[str, str]] = []

        for url in urls:
            print(f"GET {url}")
            try:
                soup = get_soup(url)
            except Exception as e:
                print(f"  fetch failed: {repr(e)}")
                continue

            title = soup.title.string.strip() if soup.title and soup.title.string else ""
            print(f"  title={title!r}")

            records = extract_records_from_page(soup)
            print(f"  found={len(records)}")
            if len(records) > len(best_records):
                best_records = records

        if not best_records:
            empty_streak += 1
            # allow a couple of misses before stopping to handle flaky pages
            if empty_streak >= 2:
                print(f"STOP section={section_url} page={page} (no records)")
                break
        else:
            empty_streak = 0
            all_records.extend(best_records)

        sleep()

    # Dedup in-section
    seen: Set[Tuple[str, str]] = set()
    out: List[Dict[str, str]] = []
    for r in all_records:
        k = (r["tag"], r["invite"])
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out

# =========================
# Optional validation
# =========================
async def validate_with_discord(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    import discord

    token = os.getenv("DISCORD_TOKEN", "").strip()
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
            await client.fetch_invite(code, with_counts=False)
            valid.append(rec)
        except Exception:
            pass

    await client.close()
    return valid

def main():
    sections = [
        urljoin(BASE, "/tags/2-characters"),
        urljoin(BASE, "/tags/3-characters"),
        urljoin(BASE, "/tags/4-characters"),
        urljoin(BASE, "/tags/a-z"),
        urljoin(BASE, "/tags/new"),
    ]

    print("RUN CONFIG:")
    print(f"  OUT_FILE={OUT_FILE}")
    print(f"  MAX_PAGES_PER_SECTION={MAX_PAGES_PER_SECTION}")
    print(f"  SCRAPE_SLEEP={SCRAPE_SLEEP}")
    print(f"  VALIDATE_INVITES={int(VALIDATE_INVITES)}")
    print(f"  KEEP_DUPLICATE_TAGS={int(KEEP_DUPLICATE_TAGS)}")
    print(f"  MIN_TOTAL_RECORDS_TO_WRITE={MIN_TOTAL_RECORDS_TO_WRITE}")
    print(f"  MIN_NEW_RECORDS_TO_WRITE={MIN_NEW_RECORDS_TO_WRITE}")

    existing = load_existing(OUT_FILE)
    existing_pairs: Set[Tuple[str, str]] = set((r["tag"], r["invite"]) for r in existing)
    print(f"Existing records: {len(existing)}")

    scraped: List[Dict[str, str]] = []

    for sec in sections:
        print(f"\n=== SECTION: {sec} ===")
        recs = crawl_section(sec, MAX_PAGES_PER_SECTION)
        print(f"SECTION TOTAL: {len(recs)}")
        scraped.extend(recs)

    # Dedup scraped
    seen: Set[Tuple[str, str]] = set()
    deduped: List[Dict[str, str]] = []
    for r in scraped:
        k = (r["tag"], r["invite"])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)

    new_only = [r for r in deduped if (r["tag"], r["invite"]) not in existing_pairs]
    print(f"\nScraped total (deduped): {len(deduped)}")
    print(f"New records: {len(new_only)}")

    if VALIDATE_INVITES:
        import asyncio
        deduped = asyncio.run(validate_with_discord(deduped))
        # recompute new_only after validation
        new_only = [r for r in deduped if (r["tag"], r["invite"]) not in existing_pairs]
        print(f"After validation total: {len(deduped)}")
        print(f"After validation new: {len(new_only)}")

    # Safety guard: do not wipe your DB if scraping underperforms
    merged = existing[:]  # keep old
    merged.extend(new_only)

    # If you ever choose to collapse by tag (you said you don’t want this), you could do it here.
    if not KEEP_DUPLICATE_TAGS:
        best_by_tag: Dict[str, Dict[str, str]] = {}
        for r in merged:
            best_by_tag.setdefault(r["tag"], r)
        merged = list(best_by_tag.values())

    if len(merged) < MIN_TOTAL_RECORDS_TO_WRITE or len(new_only) < MIN_NEW_RECORDS_TO_WRITE:
        print("\nREFUSING TO WRITE (safety guard tripped).")
        print(f"  merged would be {len(merged)} records")
        print(f"  new_only is {len(new_only)} records")
        print("  Keeping existing tags.json unchanged.")
        return

    write_backup(OUT_FILE)

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    print(f"\nWROTE {len(merged)} records to {OUT_FILE}")

if __name__ == "__main__":
    main()
