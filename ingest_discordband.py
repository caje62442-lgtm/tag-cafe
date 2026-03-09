import os
import re
import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

# =========================================================
# Config
# =========================================================
BASE = "https://discord.band"
TAGS_ROOT = f"{BASE}/tags"
OUT_FILE = os.getenv("OUT_FILE", "tags.json")

SCRAPE_SLEEP = float(os.getenv("SCRAPE_SLEEP", "0.75"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))

# Safety and cleanliness over speed.
VALIDATE_INVITES = os.getenv("VALIDATE_INVITES", "0").strip() == "1"
KEEP_DUPLICATE_TAGS = os.getenv("KEEP_DUPLICATE_TAGS", "1").strip() not in ("0", "false", "False")
MIN_TOTAL_RECORDS_TO_WRITE = int(os.getenv("MIN_TOTAL_RECORDS_TO_WRITE", "1000"))
MIN_NEW_RECORDS_TO_WRITE = int(os.getenv("MIN_NEW_RECORDS_TO_WRITE", "1"))

# Conservative crawl limits.
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

# Only allow short, compact tag-like values.
SHORT_TOKEN_RE = re.compile(r"^[^\s]{2,6}$")

# Labels that strongly suggest a nearby tag value.
TAG_LABEL_RE = re.compile(r"\b(?:guild\s*tag|tag)\b", re.IGNORECASE)

BLOCKED_TAG_TOKENS = {
    "join",
    "login",
    "log-in",
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
}

session = requests.Session()
session.headers.update(HEADERS)


# =========================================================
# Helpers
# =========================================================
def sleep() -> None:
    if SCRAPE_SLEEP > 0:
        time.sleep(SCRAPE_SLEEP)


def get_soup(url: str) -> BeautifulSoup:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def absolute_url(href: str, base_url: str = BASE) -> str:
    return urljoin(base_url, href.strip())


def normalize_tag(tag: str) -> str:
    return tag.strip().upper()


def is_internal_url(url: str) -> bool:
    parsed = urlparse(url)
    if not parsed.netloc:
        return True
    return parsed.netloc.lower() in {"discord.band", "www.discord.band"}


def is_plausible_tag(token: str) -> bool:
    value = (token or "").strip()
    if not value:
        return False
    if not SHORT_TOKEN_RE.match(value):
        return False

    lower = value.lower()
    if lower in BLOCKED_TAG_TOKENS:
        return False
    if "http" in lower or "discord.gg" in lower or "discord.com" in lower:
        return False
    if "/" in value:
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


def clean_text(value: str) -> str:
    return " ".join((value or "").split()).strip()


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


# =========================================================
# Section discovery
# =========================================================
def discover_section_urls(root_soup: BeautifulSoup) -> List[str]:
    """
    Discover list sections from /tags.
    Keep only internal /tags/* paths that look like filter/list pages,
    not pagination URLs and not invite URLs.
    """
    discovered: Set[str] = set()

    # Seed with known major sections. Those matter even if discovery misses them.
    seeds = {
        f"{BASE}/tags/alphabet",
        f"{BASE}/tags/number",
        f"{BASE}/tags/symbol",
        f"{BASE}/tags/unicode-only",
        f"{BASE}/tags/new",
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

        # Exclude very noisy or obviously non-section routes.
        if "login" in slug.lower():
            continue

        discovered.add(f"{BASE}{path}")

    ordered = sorted(discovered)
    if len(ordered) > MAX_DISCOVERED_SECTIONS:
        ordered = ordered[:MAX_DISCOVERED_SECTIONS]

    return ordered


def discover_last_page(section_url: str, first_soup: BeautifulSoup) -> int:
    max_page = 1

    candidates = [section_url.rstrip("/"), urlparse(section_url).path.rstrip("/")]

    for anchor in first_soup.find_all("a", href=True):
        href = clean_text(anchor.get("href", ""))
        if not href:
            continue

        full = absolute_url(href, section_url)
        if not is_internal_url(full):
            continue

        parsed = urlparse(full)
        for base_candidate in candidates:
            base_candidate = base_candidate.rstrip("/")
            if not base_candidate:
                continue

            full_path = f"{base_candidate}/page/"
            if full.startswith(full_path):
                match = re.search(r"/page/(\d+)(?:/)?$", full)
                if match:
                    max_page = max(max_page, int(match.group(1)))

            path = parsed.path.rstrip("/")
            if path.startswith(urlparse(section_url).path.rstrip("/") + "/page/"):
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


# =========================================================
# Card discovery from list pages
# =========================================================
def find_smallest_single_invite_container(anchor: Tag) -> Tag:
    """
    Walk upward until the parent would include multiple invite links.
    That gives a card-sized block more often than a large grid wrapper.
    """
    current: Tag = anchor
    best: Tag = anchor

    for _ in range(10):
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


def extract_local_tag_candidate(container: Tag) -> Optional[str]:
    """
    Use only tight, local signals.
    Avoid scanning the entire page or large mixed text blocks.
    """
    # Strongest: explicit tag-labeled text.
    text_nodes = [clean_text(container.get_text(" ", strip=True))]
    for text in text_nodes:
        if not text:
            continue

        match = re.search(r"(?:guild\s*tag|tag)\s*[:\-]?\s*([^\s]{2,6})", text, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if is_plausible_tag(value):
                return normalize_tag(value)

    # Next: likely label elements.
    for element in container.find_all(["strong", "b", "span", "small", "div"]):
        text = clean_text(element.get_text(" ", strip=True))
        if not text or " " in text:
            continue
        if is_plausible_tag(text):
            return normalize_tag(text)

    # Next: image metadata.
    for image in container.find_all("img"):
        for attr in ("alt", "title"):
            value = clean_text(image.get(attr, ""))
            if is_plausible_tag(value):
                return normalize_tag(value)

    return None


def extract_detail_url_candidate(container: Tag, section_url: str) -> Optional[str]:
    """
    Find the best internal detail-page candidate inside the card.
    Exclude tag-list URLs, pagination URLs, invite URLs, and obvious utility links.
    """
    candidates: List[str] = []

    for anchor in container.find_all("a", href=True):
        href = clean_text(anchor.get("href", ""))
        if not href:
            continue

        full = absolute_url(href, section_url)
        if not is_internal_url(full):
            continue
        if normalize_invite(full):
            continue

        parsed = urlparse(full)
        path = parsed.path.rstrip("/")

        if not path or path == "/":
            continue
        if path == "/tags":
            continue
        if path.startswith("/tags/"):
            continue
        if PAGE_PATH_RE.search(path):
            continue
        if "login" in path.lower():
            continue

        candidates.append(full)

    # Prefer the shortest, cleanest candidate. Detail pages usually have one compact path.
    if not candidates:
        return None

    candidates = sorted(set(candidates), key=lambda value: (len(urlparse(value).path), value))
    return candidates[0]


def extract_name_candidate(container: Tag) -> Optional[str]:
    """
    Capture a local name candidate. Keep it as metadata only.
    """
    preferred_tags = ["h1", "h2", "h3", "strong", "b"]
    for tag_name in preferred_tags:
        for element in container.find_all(tag_name):
            text = clean_text(element.get_text(" ", strip=True))
            if not text:
                continue
            if len(text) < 2:
                continue
            if is_plausible_tag(text):
                continue
            return text

    # Fallback to first non-trivial text line.
    text = clean_text(container.get_text("\n", strip=True))
    for line in [clean_text(part) for part in text.split("\n")]:
        if not line:
            continue
        if len(line) < 2:
            continue
        if is_plausible_tag(line):
            continue
        return line

    return None


def extract_list_page_candidates(soup: BeautifulSoup, section_url: str) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    seen_pairs: Set[Tuple[str, Optional[str]]] = set()

    for anchor in soup.find_all("a", href=True):
        href = clean_text(anchor.get("href", ""))
        invite = normalize_invite(href)
        if not invite:
            continue

        if not isinstance(anchor, Tag):
            continue

        container = find_smallest_single_invite_container(anchor)
        detail_url = extract_detail_url_candidate(container, section_url)
        local_tag = extract_local_tag_candidate(container)
        local_name = extract_name_candidate(container)

        key = (invite, detail_url)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)

        candidates.append(
            {
                "invite": invite,
                "detail_url": detail_url,
                "local_tag": local_tag,
                "local_name": local_name,
                "source_section": section_url,
            }
        )

    return candidates


# =========================================================
# Detail page parsing
# =========================================================
def score_tag_candidate(source: str, value: str) -> int:
    base_scores = {
        "label": 100,
        "element_attr": 70,
        "title_pattern": 60,
        "heading": 40,
        "card_local": 20,
    }
    bonus = 0

    if value.isupper():
        bonus += 5
    if len(value) <= 4:
        bonus += 5

    return base_scores.get(source, 0) + bonus


def choose_best_tag(candidates: List[Tuple[str, str]]) -> Optional[Tuple[str, int, str]]:
    best_value: Optional[str] = None
    best_score = -1
    best_source = ""

    for source, raw_value in candidates:
        value = normalize_tag(raw_value)
        if not is_plausible_tag(value):
            continue

        score = score_tag_candidate(source, value)
        if score > best_score:
            best_score = score
            best_value = value
            best_source = source

    if best_value is None:
        return None

    return best_value, best_score, best_source


def parse_tag_candidates_from_detail(soup: BeautifulSoup) -> List[Tuple[str, str]]:
    candidates: List[Tuple[str, str]] = []

    # Meta title and document title patterns.
    title_candidates: List[str] = []

    if soup.title and soup.title.string:
        title_candidates.append(clean_text(soup.title.string))

    for prop in ("og:title", "twitter:title"):
        meta = soup.find("meta", attrs={"property": prop}) or soup.find("meta", attrs={"name": prop})
        if meta and meta.get("content"):
            title_candidates.append(clean_text(meta.get("content", "")))

    for title_text in title_candidates:
        match = re.search(r"(?:guild\s*tag|tag)\s*[:\-]?\s*([^\s|·•:]{2,6})", title_text, re.IGNORECASE)
        if match:
            value = clean_text(match.group(1))
            candidates.append(("title_pattern", value))

    # Explicit label text anywhere on the page.
    page_text = clean_text(soup.get_text("\n", strip=True))
    for match in re.finditer(r"(?:guild\s*tag|tag)\s*[:\-]?\s*([^\s]{2,6})", page_text, re.IGNORECASE):
        value = clean_text(match.group(1))
        candidates.append(("label", value))

    # Elements with tag-like classes, ids, or aria labels.
    for element in soup.find_all(True):
        attr_values = [
            clean_text(element.get("class", [""])[0]) if element.get("class") else "",
            clean_text(element.get("id", "")),
            clean_text(element.get("aria-label", "")),
            clean_text(element.get("title", "")),
        ]
        attr_blob = " ".join(part for part in attr_values if part).lower()

        if "tag" in attr_blob:
            text = clean_text(element.get_text(" ", strip=True))
            if is_plausible_tag(text):
                candidates.append(("element_attr", text))

        if element.name == "img":
            for attr in ("alt", "title"):
                value = clean_text(element.get(attr, ""))
                if is_plausible_tag(value):
                    candidates.append(("element_attr", value))

    # Headings often carry the prominent short token.
    for heading in soup.find_all(["h1", "h2", "h3"]):
        text = clean_text(heading.get_text(" ", strip=True))
        if is_plausible_tag(text):
            candidates.append(("heading", text))

    return candidates


def parse_name_from_detail(soup: BeautifulSoup) -> Optional[str]:
    for heading in soup.find_all(["h1", "h2", "h3"]):
        text = clean_text(heading.get_text(" ", strip=True))
        if not text:
            continue
        if is_plausible_tag(text):
            continue
        return text

    title_text = ""
    if soup.title and soup.title.string:
        title_text = clean_text(soup.title.string)
    if title_text:
        return title_text

    return None


def parse_invite_from_detail(soup: BeautifulSoup) -> Optional[str]:
    invites: List[str] = []

    for anchor in soup.find_all("a", href=True):
        invite = normalize_invite(clean_text(anchor.get("href", "")))
        if invite:
            invites.append(invite)

    if not invites:
        text = clean_text(soup.get_text(" ", strip=True))
        match = INVITE_RE.search(text)
        if match:
            invites.append(f"https://discord.gg/{match.group('code')}")

    if not invites:
        return None

    # Pick the most common invite on the page.
    counts: Dict[str, int] = {}
    for invite in invites:
        counts[invite] = counts.get(invite, 0) + 1

    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def parse_detail_page(detail_url: str, local_candidate: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    try:
        soup = get_soup(detail_url)
    except Exception as exc:
        print(f"  detail fetch failed: {detail_url} -> {exc!r}")
        return None

    invite_from_detail = parse_invite_from_detail(soup)
    tag_candidates = parse_tag_candidates_from_detail(soup)

    if local_candidate and local_candidate.get("local_tag"):
        tag_candidates.append(("card_local", str(local_candidate["local_tag"])))

    chosen = choose_best_tag(tag_candidates)
    if not chosen:
        print(f"  reject detail (no confident tag): {detail_url}")
        return None

    tag_value, tag_score, tag_source = chosen

    # Require a meaningful confidence threshold.
    if tag_score < 50:
        print(f"  reject detail (low-confidence tag={tag_value!r}, score={tag_score}): {detail_url}")
        return None

    invite_value = invite_from_detail
    if not invite_value and local_candidate:
        invite_value = normalize_invite(str(local_candidate.get("invite", "")))

    if not invite_value:
        print(f"  reject detail (no invite): {detail_url}")
        return None

    name_value = parse_name_from_detail(soup)
    if not name_value and local_candidate:
        name_value = local_candidate.get("local_name")

    record: Dict[str, Any] = {
        "tag": tag_value,
        "invite": invite_value,
        "detail_url": detail_url,
        "server_name": name_value,
        "tag_source": tag_source,
        "tag_score": tag_score,
        "source_section": local_candidate.get("source_section") if local_candidate else None,
    }

    return record


def build_clean_records(
    detail_candidates: List[Dict[str, Any]],
    fallback_candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    clean_records: List[Dict[str, Any]] = []

    detail_by_url: Dict[str, Dict[str, Any]] = {}
    for candidate in detail_candidates:
        detail_url = candidate.get("detail_url")
        if not detail_url:
            continue
        detail_by_url.setdefault(detail_url, candidate)

    print(f"Unique detail pages to fetch: {len(detail_by_url)}")

    for index, (detail_url, local_candidate) in enumerate(detail_by_url.items(), start=1):
        print(f"DETAIL {index}/{len(detail_by_url)} {detail_url}")
        record = parse_detail_page(detail_url, local_candidate)
        if record:
            clean_records.append(record)
        sleep()

    # Very conservative fallback: only use list-page records when there is no detail URL
    # and the local tag exists. Keep the score low but acceptable only if the tag is clean.
    for candidate in fallback_candidates:
        if candidate.get("detail_url"):
            continue

        local_tag = candidate.get("local_tag")
        invite = candidate.get("invite")
        if not local_tag or not invite:
            continue

        if not is_plausible_tag(str(local_tag)):
            continue

        record = {
            "tag": normalize_tag(str(local_tag)),
            "invite": normalize_invite(str(invite)),
            "detail_url": None,
            "server_name": candidate.get("local_name"),
            "tag_source": "card_local",
            "tag_score": score_tag_candidate("card_local", str(local_tag)),
            "source_section": candidate.get("source_section"),
        }

        # Only accept very clean local tags.
        if record["invite"] and record["tag_score"] >= 25:
            clean_records.append(record)

    clean_records = [record for record in clean_records if record.get("tag") and record.get("invite")]
    clean_records = dedupe_by_tag_invite(clean_records)

    return clean_records


# =========================================================
# Optional Discord validation
# =========================================================
async def validate_with_discord(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    import discord

    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("DISCORD_TOKEN is not set but VALIDATE_INVITES=1.")

    client = discord.Client(intents=discord.Intents.none())
    await client.login(token)

    valid_records: List[Dict[str, Any]] = []
    checked_invites: Set[str] = set()

    for record in records:
        invite = record["invite"]
        if invite in checked_invites:
            # Preserve one record per tag/invite pair later during dedupe.
            valid_records.append(record)
            continue

        checked_invites.add(invite)
        code = invite.rstrip("/").split("/")[-1]

        try:
            await client.fetch_invite(code, with_counts=False)
            valid_records.append(record)
        except Exception:
            print(f"  invalid invite rejected: {invite}")

    await client.close()
    return valid_records


# =========================================================
# Main
# =========================================================
def main() -> None:
    print("RUN CONFIG:")
    print(f"  OUT_FILE={OUT_FILE}")
    print(f"  TAGS_ROOT={TAGS_ROOT}")
    print(f"  SCRAPE_SLEEP={SCRAPE_SLEEP}")
    print(f"  REQUEST_TIMEOUT={REQUEST_TIMEOUT}")
    print(f"  VALIDATE_INVITES={int(VALIDATE_INVITES)}")
    print(f"  KEEP_DUPLICATE_TAGS={int(KEEP_DUPLICATE_TAGS)}")
    print(f"  MIN_TOTAL_RECORDS_TO_WRITE={MIN_TOTAL_RECORDS_TO_WRITE}")
    print(f"  MIN_NEW_RECORDS_TO_WRITE={MIN_NEW_RECORDS_TO_WRITE}")
    print(f"  MAX_DISCOVERED_SECTIONS={MAX_DISCOVERED_SECTIONS}")
    print(f"  MAX_PAGES_PER_SECTION_HARD_LIMIT={MAX_PAGES_PER_SECTION_HARD_LIMIT}")

    existing = load_existing(OUT_FILE)
    existing_pairs: Set[Tuple[str, str]] = {(record["tag"], record["invite"]) for record in existing}
    print(f"Existing records: {len(existing)}")

    root_soup = get_soup(TAGS_ROOT)
    sections = discover_section_urls(root_soup)
    print(f"Discovered sections: {len(sections)}")
    for section in sections:
        print(f"  SECTION {section}")

    detail_candidates: List[Dict[str, Any]] = []
    fallback_candidates: List[Dict[str, Any]] = []

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
            page_candidates = extract_list_page_candidates(soup, section_url)

            print(f"  title={page_title!r}")
            print(f"  candidates={len(page_candidates)}")

            for candidate in page_candidates:
                if candidate.get("detail_url"):
                    detail_candidates.append(candidate)
                else:
                    fallback_candidates.append(candidate)

            sleep()

    print(f"\nList-page candidates with detail URLs: {len(detail_candidates)}")
    print(f"List-page fallback-only candidates: {len(fallback_candidates)}")

    clean_scraped = build_clean_records(detail_candidates, fallback_candidates)

    print(f"\nClean scraped total (deduped): {len(clean_scraped)}")

    if VALIDATE_INVITES:
        import asyncio

        clean_scraped = asyncio.run(validate_with_discord(clean_scraped))
        clean_scraped = dedupe_by_tag_invite(clean_scraped)
        print(f"After invite validation total: {len(clean_scraped)}")

    new_only = [
        record
        for record in clean_scraped
        if (record["tag"], record["invite"]) not in existing_pairs
    ]

    print(f"New records: {len(new_only)}")

    merged = existing[:] + new_only
    merged = dedupe_by_tag_invite(merged)

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
