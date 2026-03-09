import os
import re
import json
import time
from typing import List, Dict, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

# =========================
# Config
# =========================
BASE = "https://discord.band"
START_URL = f"{BASE}/tags/alphabet"
OUT_FILE = os.getenv("OUT_FILE", "tags.json")

SCRAPE_SLEEP = float(os.getenv("SCRAPE_SLEEP", "0.75"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))

# Optional invite validation via Discord API
VALIDATE_INVITES = os.getenv("VALIDATE_INVITES", "0").strip() == "1"

# Safety guards
MIN_TOTAL_RECORDS_TO_WRITE = int(os.getenv("MIN_TOTAL_RECORDS_TO_WRITE", "1000"))
MIN_NEW_RECORDS_TO_WRITE = int(os.getenv("MIN_NEW_RECORDS_TO_WRITE", "1"))

# Keep multiple servers for the same tag
KEEP_DUPLICATE_TAGS = os.getenv("KEEP_DUPLICATE_TAGS", "1").strip() not in ("0", "false", "False")

HEADERS = {
    "User-Agent": os.getenv(
        "SCRAPE_UA",
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        ),
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

INVITE_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:discord\.gg|discord(?:app)?\.com/invite|discord\.com/invite)/(?P<code>[A-Za-z0-9-]+)",
    re.IGNORECASE,
)

TAG_CLEAN_RE = re.compile(r"^[^\s]{2,10}$")

_session = requests.Session()
_session.headers.update(HEADERS)


def sleep() -> None:
    if SCRAPE_SLEEP > 0:
        time.sleep(SCRAPE_SLEEP)


def get_soup(url: str) -> BeautifulSoup:
    response = _session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def normalize_tag(tag: str) -> str:
    return tag.strip().upper()


def is_plausible_tag(tag: str) -> bool:
    value = (tag or "").strip()
    if not value:
        return False
    if not TAG_CLEAN_RE.match(value):
        return False

    lower = value.lower()
    bad = {
        "join",
        "tags",
        "tag",
        "discord",
        "server",
        "servers",
        "login",
        "online",
        "members",
    }
    if lower in bad:
        return False
    if "http" in lower or "discord.gg" in lower or "discord.com" in lower:
        return False
    return True


def normalize_invite(url_or_code: str) -> Optional[str]:
    raw = (url_or_code or "").strip()
    if not raw:
        return None

    match = INVITE_RE.search(raw)
    if match:
        return f"https://discord.gg/{match.group('code')}"

    cleaned = raw.strip("/").split("/")[-1]
    if cleaned and re.fullmatch(r"[A-Za-z0-9-]+", cleaned):
        return f"https://discord.gg/{cleaned}"

    return None


def load_existing(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []

    try:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    output: List[Dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue

        tag = normalize_tag(str(item.get("tag", "")).strip())
        invite = normalize_invite(str(item.get("invite", "")).strip())
        if tag and invite:
            output.append({"tag": tag, "invite": invite})

    return output


def write_backup(path: str) -> None:
    if not os.path.exists(path):
        return

    backup = path.replace(".json", "") + ".bak.json"
    try:
        with open(path, "r", encoding="utf-8") as source:
            old = source.read()
        with open(backup, "w", encoding="utf-8") as dest:
            dest.write(old)
        print(f"Backup written: {backup}")
    except Exception as exc:
        print(f"Backup failed: {exc!r}")


def get_last_page(soup: BeautifulSoup) -> int:
    max_page = 1

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        match = re.search(r"/tags/alphabet/page/(\d+)", href)
        if match:
            max_page = max(max_page, int(match.group(1)))

    if max_page > 1:
        return max_page

    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(" ", strip=True).lower()
        if "last page" in text:
            match = re.search(r"/page/(\d+)", anchor["href"])
            if match:
                max_page = max(max_page, int(match.group(1)))

    return max_page


def find_local_container(anchor) -> BeautifulSoup:
    container = anchor
    for _ in range(6):
        if getattr(container, "parent", None) is None:
            break
        container = container.parent
    return container


def extract_tag_from_container(container: BeautifulSoup) -> Optional[str]:
    for image in container.find_all("img", alt=True):
        alt = (image.get("alt") or "").strip()
        if is_plausible_tag(alt):
            return normalize_tag(alt)

    text = container.get_text(" ", strip=True)
    tokens = [
        token.strip("[](){}<>!@#$%^&*_=+|;:'\",.?/\\")
        for token in text.split()
    ]
    for token in tokens:
        if is_plausible_tag(token):
            return normalize_tag(token)

    return None


def extract_records_from_page(soup: BeautifulSoup) -> List[Dict[str, str]]:
    records: List[Dict[str, str]] = []
    seen_pairs: Set[Tuple[str, str]] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        invite = normalize_invite(href)
        if not invite:
            continue

        container = find_local_container(anchor)
        tag = extract_tag_from_container(container)
        if not tag:
            continue

        pair = (tag, invite)
        if pair in seen_pairs:
            continue

        seen_pairs.add(pair)
        records.append({"tag": tag, "invite": invite})

    return records


def dedupe_records(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen: Set[Tuple[str, str]] = set()
    output: List[Dict[str, str]] = []

    for record in records:
        pair = (record["tag"], record["invite"])
        if pair in seen:
            continue
        seen.add(pair)
        output.append(record)

    return output


async def validate_with_discord(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    import discord

    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("DISCORD_TOKEN is not set but VALIDATE_INVITES=1.")

    client = discord.Client(intents=discord.Intents.none())
    await client.login(token)

    valid: List[Dict[str, str]] = []
    seen_invites: Set[str] = set()

    for record in records:
        invite = record["invite"]
        if invite in seen_invites:
            continue
        seen_invites.add(invite)

        code = invite.rstrip("/").split("/")[-1]
        try:
            await client.fetch_invite(code, with_counts=False)
            valid.append(record)
        except Exception:
            pass

    await client.close()
    return valid


def collapse_by_tag(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    best_by_tag: Dict[str, Dict[str, str]] = {}
    for record in records:
        best_by_tag.setdefault(record["tag"], record)
    return list(best_by_tag.values())


def main() -> None:
    print("RUN CONFIG:")
    print(f"  OUT_FILE={OUT_FILE}")
    print(f"  START_URL={START_URL}")
    print(f"  SCRAPE_SLEEP={SCRAPE_SLEEP}")
    print(f"  REQUEST_TIMEOUT={REQUEST_TIMEOUT}")
    print(f"  VALIDATE_INVITES={int(VALIDATE_INVITES)}")
    print(f"  KEEP_DUPLICATE_TAGS={int(KEEP_DUPLICATE_TAGS)}")
    print(f"  MIN_TOTAL_RECORDS_TO_WRITE={MIN_TOTAL_RECORDS_TO_WRITE}")
    print(f"  MIN_NEW_RECORDS_TO_WRITE={MIN_NEW_RECORDS_TO_WRITE}")

    existing = load_existing(OUT_FILE)
    existing_pairs: Set[Tuple[str, str]] = {
        (record["tag"], record["invite"]) for record in existing
    }
    print(f"Existing records: {len(existing)}")

    first_page = get_soup(START_URL)
    last_page = get_last_page(first_page)
    print(f"Detected last page: {last_page}")

    scraped: List[Dict[str, str]] = []

    for page in range(1, last_page + 1):
        url = START_URL if page == 1 else f"{START_URL}/page/{page}"
        print(f"GET {url}")

        try:
            soup = first_page if page == 1 else get_soup(url)
        except Exception as exc:
            print(f"  fetch failed: {exc!r}")
            sleep()
            continue

        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        page_records = extract_records_from_page(soup)

        print(f"  title={title!r}")
        print(f"  found={len(page_records)}")

        scraped.extend(page_records)
        sleep()

    deduped = dedupe_records(scraped)
    new_only = [
        record for record in deduped
        if (record["tag"], record["invite"]) not in existing_pairs
    ]

    print(f"\nScraped total (deduped): {len(deduped)}")
    print(f"New records: {len(new_only)}")

    if VALIDATE_INVITES:
        import asyncio

        deduped = asyncio.run(validate_with_discord(deduped))
        new_only = [
            record for record in deduped
            if (record["tag"], record["invite"]) not in existing_pairs
        ]
        print(f"After validation total: {len(deduped)}")
        print(f"After validation new: {len(new_only)}")

    merged = existing[:]
    merged.extend(new_only)
    merged = dedupe_records(merged)

    if not KEEP_DUPLICATE_TAGS:
        merged = collapse_by_tag(merged)

    if len(merged) < MIN_TOTAL_RECORDS_TO_WRITE or len(new_only) < MIN_NEW_RECORDS_TO_WRITE:
        print("\nREFUSING TO WRITE (safety guard tripped).")
        print(f"  merged would be {len(merged)} records")
        print(f"  new_only is {len(new_only)} records")
        print(f"  Keeping existing {OUT_FILE} unchanged.")
        return

    write_backup(OUT_FILE)

    with open(OUT_FILE, "w", encoding="utf-8") as file:
        json.dump(merged, file, indent=2, ensure_ascii=False)

    print(f"\nWROTE {len(merged)} records to {OUT_FILE}")


if __name__ == "__main__":
    main()
