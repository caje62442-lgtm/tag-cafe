import asyncio
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

BASE = "https://discord.band"
TAGS_ROOT = f"{BASE}/tags"
OUT_FILE = os.getenv("OUT_FILE", "tags.json")

SCRAPE_SLEEP = float(os.getenv("SCRAPE_SLEEP", "0.5"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))

VALIDATE_INVITES = os.getenv("VALIDATE_INVITES", "0").strip() == "1"
KEEP_DUPLICATE_TAGS = os.getenv("KEEP_DUPLICATE_TAGS", "1").strip() not in ("0", "false", "False")
REPLACE_FULL_SNAPSHOT = os.getenv("REPLACE_FULL_SNAPSHOT", "1").strip() not in ("0", "false", "False")

MIN_TOTAL_RECORDS_TO_WRITE = int(os.getenv("MIN_TOTAL_RECORDS_TO_WRITE", "1"))
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
SERVER_ID_RE = re.compile(r"^\d{15,22}$")

# Discord.band tag filters are explicitly 2, 3, and 4 characters.
# Keep the ingest strict.
TAG_RE = re.compile(r"^.{2,4}$", re.DOTALL)

BLOCKED_TAG_TOKENS = {
    "join",
    "find",
    "more",
    "next",
    "page",
    "last",
    "tags",
    "tag",
    "discord",
    "server",
    "servers",
    "member",
    "members",
    "online",
    "login",
    "new",
    "only",
    "add",
    "list",
    "filter",
    "search",
    "premium",
    "statistics",
}

SEED_SECTIONS = {
    f"{BASE}/tags/alphabet",
    f"{BASE}/tags/new",
    f"{BASE}/tags/number",
    f"{BASE}/tags/symbol",
    f"{BASE}/tags/unicode-only",
    f"{BASE}/tags/lowercase-only",
    f"{BASE}/tags/uppercase-only",
    f"{BASE}/tags/unique",
    f"{BASE}/tags/english",
    f"{BASE}/tags/non-english",
    f"{BASE}/tags/chinese",
    f"{BASE}/tags/japanese",
    f"{BASE}/tags/korean",
    f"{BASE}/tags/russian",
    f"{BASE}/tags/2-characters",
    f"{BASE}/tags/3-characters",
    f"{BASE}/tags/4-characters",
}

session = requests.Session()
session.headers.update(HEADERS)


def sleep() -> None:
    if SCRAPE_SLEEP > 0:
        time.sleep(SCRAPE_SLEEP)


def clean_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


def absolute_url(href: str, base_url: str = BASE) -> str:
    return urljoin(base_url, href.strip())


def normalize_invite(url_or_code: str) -> Optional[str]:
    raw = clean_text(url_or_code)
    if not raw:
        return None

    match = INVITE_RE.search(raw)
    if match:
        return f"https://discord.gg/{match.group('code')}"

    if raw.startswith("/join/"):
        code = raw.rstrip("/").split("/")[-1]
        if code:
            return f"https://discord.gg/{code}"

    parsed = urlparse(raw)
    if not parsed.scheme and not parsed.netloc and "/" not in raw and re.fullmatch(r"[A-Za-z0-9-]+", raw):
        return f"https://discord.gg/{raw}"

    return None


def is_internal_url(url: str) -> bool:
    parsed = urlparse(url)
    if not parsed.netloc:
        return True
    return parsed.netloc.lower() in {"discord.band", "www.discord.band"}


def is_plausible_tag(token: str) -> bool:
    value = token.strip()
    if not value:
        return False
    if not TAG_RE.fullmatch(value):
        return False
    if any(ch.isspace() for ch in value):
        return False

    lower = value.lower()
    if lower in BLOCKED_TAG_TOKENS:
        return False
    if "http" in lower or "discord.gg" in lower or "discord.com" in lower:
        return False
    if "/" in value:
        return False
    if SERVER_ID_RE.fullmatch(value):
        return False

    return True


def tag_score(value: str) -> Tuple[int, int, int, int, str]:
    letters = sum(ch.isalpha() for ch in value)
    uppers = sum(ch.isupper() for ch in value)
    alnum = sum(ch.isalnum() for ch in value)
    length_pref = 3 if len(value) == 4 else 2 if len(value) == 3 else 1
    return (
        length_pref,
        1 if letters and uppers == letters else 0,
        alnum,
        letters,
        value,
    )


def get_soup(url: str) -> BeautifulSoup:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def ensure_json_file_exists(path: str) -> None:
    if os.path.exists(path):
        return
    with open(path, "w", encoding="utf-8") as file:
        json.dump([], file)


def safe_load_json_array(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []

    try:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    out: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue

        tag = str(item.get("tag", "")).strip()
        invite = normalize_invite(str(item.get("invite", "")).strip())
        if not tag or not invite:
            continue
        if not is_plausible_tag(tag):
            continue

        normalized = dict(item)
        normalized["tag"] = tag
        normalized["invite"] = invite
        out.append(normalized)

    return out


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


def atomic_write_json(path: str, data: List[Dict[str, Any]]) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
        file.flush()
        os.fsync(file.fileno())
    os.replace(tmp_path, path)


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
    discovered: Set[str] = set(SEED_SECTIONS)

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

    return min(max_page, MAX_PAGES_PER_SECTION_HARD_LIMIT)


def section_page_url(section_url: str, page: int) -> str:
    if page <= 1:
        return section_url
    return section_url.rstrip("/") + f"/page/{page}"


def resolve_invite_from_href(href: str) -> Optional[str]:
    href = clean_text(href)
    if not href:
        return None

    direct = normalize_invite(href)
    if direct:
        return direct

    full = absolute_url(href, BASE)

    try:
        response = session.get(full, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        final_url = response.url
    except Exception:
        return None

    return normalize_invite(final_url)


def find_smallest_join_container(anchor: Tag) -> Tag:
    current: Tag = anchor
    best: Tag = anchor

    for _ in range(12):
        parent = current.parent
        if not isinstance(parent, Tag):
            break

        join_count = 0
        for maybe in parent.find_all("a", href=True):
            text = clean_text(maybe.get_text(" ", strip=True)).lower()
            href = clean_text(maybe.get("href", ""))
            if text == "join" or href.startswith("/join/") or normalize_invite(href):
                join_count += 1

        if join_count == 1:
            best = parent
            current = parent
            continue

        break

    return best


def visible_text_of_node(node: Tag) -> str:
    return node.get_text(" ", strip=True).strip()


def top_row_tag_from_second_image(container: Tag) -> Optional[str]:
    """
    Canonical extraction strategy:
    - find all images inside the card
    - the first image is usually the server image
    - the second image is usually the tag image
    - trust the second image alt/title first
    - then trust the immediate visible short text right after that image
    """
    images = container.find_all("img", recursive=True)
    if len(images) < 2:
        return None

    tag_image = images[1]
    candidates: List[str] = []

    for attr in ("alt", "title"):
        value = tag_image.get(attr, "")
        value = value.strip()
        if is_plausible_tag(value):
            candidates.append(value)

    # Look at nearby siblings after the tag image.
    for sibling in tag_image.next_siblings:
        if isinstance(sibling, Tag):
            text = visible_text_of_node(sibling)
        else:
            text = str(sibling).strip()

        text = text.strip()
        if not text:
            continue

        # Only trust the first compact visible token after the image.
        token = text.split()[0].strip()
        if is_plausible_tag(token):
            candidates.append(token)

        # Once we hit any meaningful text, stop walking farther.
        break

    if not candidates:
        return None

    return sorted(set(candidates), key=tag_score, reverse=True)[0]


def fallback_short_tag_candidates(container: Tag) -> List[str]:
    candidates: List[str] = []

    for image in container.find_all("img", recursive=True):
        for attr in ("alt", "title"):
            value = image.get(attr, "").strip()
            if is_plausible_tag(value):
                candidates.append(value)

    for element in container.find_all(["span", "strong", "b", "a", "div", "p", "small"], recursive=True):
        text = visible_text_of_node(element)
        if not text or " " in text:
            continue
        if is_plausible_tag(text):
            candidates.append(text)

    return candidates


def choose_card_tag(container: Tag) -> Optional[str]:
    primary = top_row_tag_from_second_image(container)
    if primary:
        return primary

    fallback = fallback_short_tag_candidates(container)
    if fallback:
        return sorted(set(fallback), key=tag_score, reverse=True)[0]

    return None


def extract_server_name(container: Tag, tag_value: Optional[str]) -> Optional[str]:
    candidates: List[str] = []

    for element in container.find_all(["h1", "h2", "h3", "strong", "b", "div", "p", "a", "span"], recursive=True):
        text = visible_text_of_node(element)
        if not text:
            continue
        if tag_value and text == tag_value:
            continue
        if is_plausible_tag(text):
            continue
        if SERVER_ID_RE.fullmatch(text):
            continue
        if len(text) < 2 or len(text) > 120:
            continue
        candidates.append(text)

    if not candidates:
        return None

    def score_name(value: str) -> Tuple[int, int, str]:
        return (
            1 if 3 <= len(value) <= 40 else 0,
            -abs(len(value) - 16),
            value,
        )

    return sorted(set(candidates), key=score_name, reverse=True)[0]


def extract_list_page_records(soup: BeautifulSoup, section_url: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    seen_pairs: Set[Tuple[str, str]] = set()

    join_anchors: List[Tag] = []
    for anchor in soup.find_all("a", href=True):
        text = clean_text(anchor.get_text(" ", strip=True)).lower()
        href = clean_text(anchor.get("href", ""))
        if text == "join" or href.startswith("/join/") or normalize_invite(href):
            join_anchors.append(anchor)

    for anchor in join_anchors:
        href = clean_text(anchor.get("href", ""))
        invite = resolve_invite_from_href(href)
        if not invite:
            continue

        container = find_smallest_join_container(anchor)
        tag_value = choose_card_tag(container)

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
                "server_name": extract_server_name(container, tag_value),
                "source_section": section_url,
                "tag_source": "top_row",
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
    ensure_json_file_exists(OUT_FILE)

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

    existing = safe_load_json_array(OUT_FILE)
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
    atomic_write_json(OUT_FILE, output_records)

    print(f"\nWROTE {len(output_records)} records to {OUT_FILE}")


if __name__ == "__main__":
    main()
