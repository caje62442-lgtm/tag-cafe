import os
import re
import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

BASE = "https://discord.band"
TAGS_ROOT = f"{BASE}/tags"
OUT_FILE = os.getenv("OUT_FILE", "tags.json")

SCRAPE_SLEEP = float(os.getenv("SCRAPE_SLEEP", "0.75"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))

VALIDATE_INVITES = os.getenv("VALIDATE_INVITES", "0").strip() == "1"
KEEP_DUPLICATE_TAGS = os.getenv("KEEP_DUPLICATE_TAGS", "1").strip() not in ("0", "false", "False")
REPLACE_FULL_SNAPSHOT = os.getenv("REPLACE_FULL_SNAPSHOT", "1").strip() not in ("0", "false", "False")

MIN_TOTAL_RECORDS_TO_WRITE = int(os.getenv("MIN_TOTAL_RECORDS_TO_WRITE", "1000"))
MIN_NEW_RECORDS_TO_WRITE = int(os.getenv("MIN_NEW_RECORDS_TO_WRITE", "0"))

MAX_DISCOVERED_SECTIONS = int(os.getenv("MAX_DISCOVERED_SECTIONS", "100"))
MAX_PAGES_PER_SECTION_HARD_LIMIT = int(os.getenv("MAX_PAGES_PER_SECTION_HARD_LIMIT", "1000"))

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

PAGE_PATH_RE = re.compile(r"/page/(\d+)(?:/)?$", re.IGNORECASE)

# Discord.band tags are 2–4 characters.
# Keep it strict. Anything longer is not a valid tag for this dataset.
TAG_RE = re.compile(r"^[^\s]{2,4}$")

BLOCKED_TAG_TOKENS = {
    "join",
    "login",
    "discord",
    "server",
    "servers",
    "tags",
    "tag",
    "members",
    "member",
    "online",
    "new",
    "alphabet",
    "number",
    "numbers",
    "symbol",
    "symbols",
    "unicode",
    "only",
    "page",
    "invite",
    "find",
    "more",
}

session = requests.Session()
session.headers.update(HEADERS)


def sleep() -> None:
    if SCRAPE_SLEEP > 0:
        time.sleep(SCRAPE_SLEEP)


def get_soup(url: str) -> BeautifulSoup:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def absolute_url(href: str, base_url: str = BASE) -> str:
    return urljoin(base_url, href.strip())


def clean_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


def normalize_tag(tag: str) -> str:
    return tag.strip()


def normalize_invite(url_or_code: str) -> Optional[str]:
    raw = clean_text(url_or_code)
    if not raw:
        return None

    match = INVITE_RE.search(raw)
    if match:
        return f"https://discord.gg/{match.group('code')}"

    cleaned = raw.strip("/").split("/")[-1]
    if cleaned and re.fullmatch(r"[A-Za-z0-9-]+", cleaned):
        return f"https://discord.gg/{cleaned}"

    return None


def is_internal_url(url: str) -> bool:
    parsed = urlparse(url)
    if not parsed.netloc:
        return True
    return parsed.netloc.lower() in {"discord.band", "www.discord.band"}


def is_plausible_tag(token: str) -> bool:
    value = clean_text(token)
    if not value:
        return False
    if not TAG_RE.fullmatch(value):
        return False

    lower = value.lower()
    if lower in BLOCKED_TAG_TOKENS:
        return False
    if "http" in lower or "discord.gg" in lower or "discord.com" in lower:
        return False
    if "/" in value:
        return False

    return True


def load_existing(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []

    try:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    output: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue

        tag = normalize_tag(str(item.get("tag", "")).strip())
        invite = normalize_invite(str(item.get("invite", "")).strip())

        if not tag or not invite:
            continue
        if not is_plausible_tag(tag):
            continue

        normalized = dict(item)
        normalized["tag"] = tag
        normalized["invite"] = invite
        output.append(normalized)

    return output


def write_backup(path: str) -> None:
    if not os.path.exists(path):
        return

    backup_path = path.replace(".json", "") + ".bak.json"
    try:
        with open(path, "r", encoding="utf-8") as source:
            old = source.read()
        with open(backup_path, "w", encoding="utf-8") as dest:
            dest.write(old)
        print(f"Backup written: {backup_path}")
    except Exception as exc:
        print(f"Backup failed: {exc!r}")


def dedupe_by_tag_invite(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[Tuple[str, str]] = set()
    output: List[Dict[str, Any]] = []

    for record in records:
        pair = (record["tag"], record["invite"])
        if pair in seen:
            continue
        seen.add(pair)
        output.append(record)

    return output


def collapse_by_tag(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best_by_tag: Dict[str, Dict[str, Any]] = {}
    for record in records:
        best_by_tag.setdefault(record["tag"], record)
    return list(best_by_tag.values())


def discover_section_urls(root_soup: BeautifulSoup) -> List[str]:
    discovered: Set[str] = set()

    seeds = {
        f"{BASE}/tags/alphabet",
        f"{BASE}/tags/new",
        f"{BASE}/tags/number",
        f"{BASE}/tags/symbol",
        f"{BASE}/tags/unicode-only",
        f"{BASE}/tags/2-characters",
        f"{BASE}/tags/3-characters",
        f"{BASE}/tags/4-characters",
        f"{BASE}/tags/lowercase-only",
        f"{BASE}/tags/uppercase-only",
        f"{BASE}/tags/unique",
        f"{BASE}/tags/english",
        f"{BASE}/tags/non-english",
        f"{BASE}/tags/chinese",
        f"{BASE}/tags/japanese",
        f"{BASE}/tags/korean",
        f"{BASE}/tags/russian",
    }

    for seed in seeds:
        discovered.add(seed)

    for anchor in root_soup.find_all("a", href=True):
        href = clean_text(anchor.get("href", ""))
        if not href:
            continue

        full = absolute_url(href, BASE)
        if not is_internal_url(full):
            continue
        if normalize_invite(full):
            continue

        parsed = urlparse(full)
        path = parsed.path.rstrip("/")

        if path == "/tags":
            continue
        if not path.startswith("/tags/"):
            continue
        if PAGE_PATH_RE.search(path):
            continue

        slug = path.split("/tags/", 1)[-1].strip("/")
        if not slug:
            continue
        if "login" in slug.lower():
            continue

        discovered.add(f"{BASE}{path}")

    ordered = sorted(discovered)
    if len(ordered) > MAX_DISCOVERED_SECTIONS:
        ordered = ordered[:MAX_DISCOVERED_SECTIONS]

    return ordered


def discover_last_page(section_url: str, first_soup: BeautifulSoup) -> int:
    max_page = 1
    section_path = urlparse(section_url).path.rstrip("/")

    for anchor in first_soup.find_all("a", href=True):
        href = clean_text(anchor.get("href", ""))
        if not href:
            continue

        full = absolute_url(href, section_url)
        if not is_internal_url(full):
            continue

        parsed = urlparse(full)
        path = parsed.path.rstrip("/")

        if path.startswith(section_path + "/page/"):
            match = PAGE_PATH_RE.search(path)
            if match:
                max_page = max(max_page, int(match.group(1)))

    if max_page > MAX_PAGES_PER_SECTION_HARD_LIMIT:
        max_page = MAX_PAGES_PER_SECTION_HARD_LIMIT

    return max_page


def section_page_url(section_url: str, page: int) -> str:
    if page <= 1:
        return section_url
    return section_url.rstrip("/") + f"/page/{page}"


def find_smallest_single_invite_container(anchor: Tag) -> Tag:
    current: Tag = anchor
    best: Tag = anchor

    for _ in range(12):
        parent = current.parent
        if not isinstance(parent, Tag):
            break

        invite_count = 0
        for maybe in parent.find_all("a", href=True):
            if normalize_invite(clean_text(maybe.get("href", ""))):
                invite_count += 1

        if invite_count == 1:
            best = parent
            current = parent
            continue

        break

    return best


def tag_from_card_structure(container: Tag) -> Optional[str]:
    """
    Extract the displayed tag from the server card.

    Discord.band cards render a small icon/avatar, then the visible short tag token,
    then the server name below it. We only trust short standalone text nodes from
    the immediate card structure and image alt/title values that also satisfy 2–4 chars.
    """
    candidates: List[str] = []

    # Strongest signal: short text directly attached to compact elements near the top.
    for element in container.find_all(["span", "strong", "b", "div", "a", "p"], recursive=True):
        text = clean_text(element.get_text(" ", strip=True))
        if not text:
            continue
        if "\n" in text:
            continue
        if " " in text:
            continue
        if is_plausible_tag(text):
            candidates.append(text)

    # Also trust image alt/title values, but still enforce 2–4 chars.
    for image in container.find_all("img", recursive=True):
        for attr in ("alt", "title"):
            value = clean_text(image.get(attr, ""))
            if is_plausible_tag(value):
                candidates.append(value)

    if not candidates:
        return None

    # Prefer:
    # 1) uppercase 4-char tokens,
    # 2) uppercase 3-char tokens,
    # 3) any 4-char token,
    # 4) anything else valid.
    def score(value: str) -> Tuple[int, int, int, str]:
        length = len(value)
        is_upper = int(value.upper() == value and any(ch.isalpha() for ch in value))
        return (
            1 if length == 4 else 0,
            is_upper,
            1 if length == 3 else 0,
            value,
        )

    best = sorted(set(candidates), key=score, reverse=True)[0]
    return normalize_tag(best)


def extract_name_candidate(container: Tag, tag_value: Optional[str]) -> Optional[str]:
    for element in container.find_all(["h1", "h2", "h3", "strong", "b", "div", "p"], recursive=True):
        text = clean_text(element.get_text(" ", strip=True))
        if not text:
            continue
        if tag_value and text == tag_value:
            continue
        if is_plausible_tag(text):
            continue
        if len(text) < 2:
            continue
        if len(text) > 120:
            continue
        return text
    return None


def extract_list_page_records(soup: BeautifulSoup, section_url: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    seen_pairs: Set[Tuple[str, str]] = set()

    for anchor in soup.find_all("a", href=True):
        href = clean_text(anchor.get("href", ""))
        invite = normalize_invite(href)
        if not invite:
            continue

        if not isinstance(anchor, Tag):
            continue

        container = find_smallest_single_invite_container(anchor)
        tag_value = tag_from_card_structure(container)

        if not tag_value:
            continue
        if not is_plausible_tag(tag_value):
            continue

        pair = (tag_value, invite)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        records.append(
            {
                "tag": tag_value,
                "invite": invite,
                "server_name": extract_name_candidate(container, tag_value),
                "source_section": section_url,
                "tag_source": "list_card",
            }
        )

    return records


async def validate_with_discord(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import discord

    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("DISCORD_TOKEN is not set but VALIDATE_INVITES=1.")

    client = discord.Client(intents=discord.Intents.none())
    await client.login(token)

    valid_records: List[Dict[str, Any]] = []
    checked_valid_invites: Set[str] = set()

    for record in records:
        invite = record["invite"]

        if invite in checked_valid_invites:
            valid_records.append(record)
            continue

        code = invite.rstrip("/").split("/")[-1]

        try:
            await client.fetch_invite(code, with_counts=False)
            checked_valid_invites.add(invite)
            valid_records.append(record)
        except Exception:
            print(f"  invalid invite rejected: {invite}")

    await client.close()
    return valid_records


def build_output_records(existing: List[Dict[str, Any]], scraped: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    existing_pairs: Set[Tuple[str, str]] = {(record["tag"], record["invite"]) for record in existing}

    if REPLACE_FULL_SNAPSHOT:
        output = dedupe_by_tag_invite(scraped)
        if not KEEP_DUPLICATE_TAGS:
            output = collapse_by_tag(output)

        new_only = [
            record
            for record in output
            if (record["tag"], record["invite"]) not in existing_pairs
        ]
        return output, new_only

    new_only = [
        record
        for record in scraped
        if (record["tag"], record["invite"]) not in existing_pairs
    ]

    merged = existing[:] + new_only
    merged = dedupe_by_tag_invite(merged)

    if not KEEP_DUPLICATE_TAGS:
        merged = collapse_by_tag(merged)

    return merged, new_only


def main() -> None:
    print("RUN CONFIG:")
    print(f"  OUT_FILE={OUT_FILE}")
    print(f"  TAGS_ROOT={TAGS_ROOT}")
    print(f"  SCRAPE_SLEEP={SCRAPE_SLEEP}")
    print(f"  REQUEST_TIMEOUT={REQUEST_TIMEOUT}")
    print(f"  VALIDATE_INVITES={int(VALIDATE_INVITES)}")
    print(f"  KEEP_DUPLICATE_TAGS={int(KEEP_DUPLICATE_TAGS)}")
    print(f"  REPLACE_FULL_SNAPSHOT={int(REPLACE_FULL_SNAPSHOT)}")
    print(f"  MIN_TOTAL_RECORDS_TO_WRITE={MIN_TOTAL_RECORDS_TO_WRITE}")
    print(f"  MIN_NEW_RECORDS_TO_WRITE={MIN_NEW_RECORDS_TO_WRITE}")
    print(f"  MAX_DISCOVERED_SECTIONS={MAX_DISCOVERED_SECTIONS}")
    print(f"  MAX_PAGES_PER_SECTION_HARD_LIMIT={MAX_PAGES_PER_SECTION_HARD_LIMIT}")

    existing = load_existing(OUT_FILE)
    print(f"Existing records: {len(existing)}")

    root_soup = get_soup(TAGS_ROOT)
    sections = discover_section_urls(root_soup)
    print(f"Discovered sections: {len(sections)}")
    for section in sections:
        print(f"  SECTION {section}")

    scraped_records: List[Dict[str, Any]] = []

    for section_url in sections:
        print(f"\n=== SECTION: {section_url} ===")

        try:
            first_soup = get_soup(section_url)
        except Exception as exc:
            print(f"  section fetch failed: {exc!r}")
            sleep()
            continue

        last_page = discover_last_page(section_url, first_soup)
        print(f"  last_page={last_page}")

        for page in range(1, last_page + 1):
            url = section_page_url(section_url, page)
            print(f"GET {url}")

            try:
                soup = first_soup if page == 1 else get_soup(url)
            except Exception as exc:
                print(f"  page fetch failed: {exc!r}")
                sleep()
                continue

            page_title = soup.title.string.strip() if soup.title and soup.title.string else ""
            page_records = extract_list_page_records(soup, section_url)

            print(f"  title={page_title!r}")
            print(f"  records={len(page_records)}")

            scraped_records.extend(page_records)
            sleep()

    scraped_records = [
        record
        for record in scraped_records
        if record.get("tag")
        and record.get("invite")
        and is_plausible_tag(str(record["tag"]))
    ]
    scraped_records = dedupe_by_tag_invite(scraped_records)

    print(f"\nClean scraped total (deduped): {len(scraped_records)}")

    if VALIDATE_INVITES:
        import asyncio

        scraped_records = asyncio.run(validate_with_discord(scraped_records))
        scraped_records = dedupe_by_tag_invite(scraped_records)
        print(f"After invite validation total: {len(scraped_records)}")

    output_records, new_only = build_output_records(existing, scraped_records)

    print(f"New records compared to previous file: {len(new_only)}")
    print(f"Output records to write: {len(output_records)}")

    if len(output_records) < MIN_TOTAL_RECORDS_TO_WRITE:
        print("\nREFUSING TO WRITE (total-output safety guard tripped).")
        print(f"  output would be {len(output_records)} records")
        print(f"  minimum required is {MIN_TOTAL_RECORDS_TO_WRITE}")
        print(f"  Keeping existing {OUT_FILE} unchanged.")
        return

    if MIN_NEW_RECORDS_TO_WRITE > 0 and len(new_only) < MIN_NEW_RECORDS_TO_WRITE:
        print("\nREFUSING TO WRITE (new-record safety guard tripped).")
        print(f"  new_only is {len(new_only)} records")
        print(f"  minimum required is {MIN_NEW_RECORDS_TO_WRITE}")
        print(f"  Keeping existing {OUT_FILE} unchanged.")
        return

    write_backup(OUT_FILE)

    with open(OUT_FILE, "w", encoding="utf-8") as file:
        json.dump(output_records, file, indent=2, ensure_ascii=False)

    print(f"\nWROTE {len(output_records)} records to {OUT_FILE}")


if __name__ == "__main__":
    main()
