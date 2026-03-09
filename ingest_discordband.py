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
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

INVITE_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:discord\.gg|discord(?:app)?\.com/invite|discord\.com/invite)/(?P<code>[A-Za-z0-9-]+)",
    re.IGNORECASE,
)

PAGE_PATH_RE = re.compile(r"/page/(\d+)(?:/)?$", re.IGNORECASE)
SHORT_TOKEN_RE = re.compile(r"^[^\s]{2,6}$")

BLOCKED_TAG_TOKENS = {
    "join","login","discord","server","servers","tags","tag","members","member",
    "online","new","alphabet","number","numbers","symbol","symbols","unicode",
    "only","page","invite",
}

session = requests.Session()
session.headers.update(HEADERS)


def sleep():
    if SCRAPE_SLEEP > 0:
        time.sleep(SCRAPE_SLEEP)


def get_soup(url: str) -> BeautifulSoup:
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def absolute_url(href: str, base: str = BASE) -> str:
    return urljoin(base, href.strip())


def normalize_tag(tag: str) -> str:
    return tag.strip().upper()


def normalize_invite(value: str) -> Optional[str]:
    m = INVITE_RE.search(value)
    if m:
        return f"https://discord.gg/{m.group('code')}"
    return None


def is_plausible_tag(token: str) -> bool:
    t = token.strip()
    if not SHORT_TOKEN_RE.match(t):
        return False
    if t.lower() in BLOCKED_TAG_TOKENS:
        return False
    return True


def load_existing(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    out = []
    for r in data:
        tag = normalize_tag(str(r.get("tag","")))
        invite = normalize_invite(str(r.get("invite","")))
        if tag and invite:
            r["tag"] = tag
            r["invite"] = invite
            out.append(r)
    return out


def write_backup(path: str):
    if not os.path.exists(path):
        return
    backup = path.replace(".json","") + ".bak.json"
    with open(path,"r",encoding="utf-8") as f:
        old = f.read()
    with open(backup,"w",encoding="utf-8") as f:
        f.write(old)
    print("Backup written:", backup)


def dedupe(records: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    seen=set()
    out=[]
    for r in records:
        key=(r["tag"],r["invite"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def discover_sections(root: BeautifulSoup) -> List[str]:
    urls=set([
        f"{BASE}/tags/alphabet",
        f"{BASE}/tags/new",
        f"{BASE}/tags/number",
        f"{BASE}/tags/symbol",
        f"{BASE}/tags/unicode-only"
    ])
    for a in root.find_all("a",href=True):
        href=a["href"]
        if href.startswith("/tags/") and "/page/" not in href:
            urls.add(absolute_url(href))
    out=sorted(urls)
    return out[:MAX_DISCOVERED_SECTIONS]


def discover_last_page(section_url:str,soup:BeautifulSoup)->int:
    maxp=1
    base=urlparse(section_url).path.rstrip("/")
    for a in soup.find_all("a",href=True):
        href=a["href"]
        if "/page/" in href:
            m=re.search(r"/page/(\d+)",href)
            if m:
                maxp=max(maxp,int(m.group(1)))
    return min(maxp,MAX_PAGES_PER_SECTION_HARD_LIMIT)


def section_page(section_url:str,page:int)->str:
    if page<=1:
        return section_url
    return section_url.rstrip("/") + f"/page/{page}"


def find_card(anchor:Tag)->Tag:
    cur=anchor
    best=anchor
    for _ in range(8):
        parent=cur.parent
        if not isinstance(parent,Tag):
            break
        invites=len(parent.find_all("a",href=lambda x: x and "discord.gg" in x))
        if invites==1:
            best=parent
            cur=parent
            continue
        break
    return best


def extract_local_tag(card:Tag)->Optional[str]:
    text=card.get_text(" ",strip=True)
    m=re.search(r"(?:tag|guild tag)[:\s]+([^\s]{2,6})",text,re.I)
    if m and is_plausible_tag(m.group(1)):
        return normalize_tag(m.group(1))
    return None


def extract_candidates(soup:BeautifulSoup,section:str)->List[Dict[str,Any]]:
    out=[]
    seen=set()
    for a in soup.find_all("a",href=True):
        invite=normalize_invite(a["href"])
        if not invite:
            continue
        card=find_card(a)
        tag=extract_local_tag(card)
        name=card.get_text(" ",strip=True)[:80]
        key=(invite,tag)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "invite":invite,
            "tag":tag,
            "server_name":name,
            "source_section":section
        })
    return out


async def validate_with_discord(records):
    import discord
    token=os.getenv("DISCORD_TOKEN","")
    if not token:
        raise RuntimeError("DISCORD_TOKEN required for validation")
    client=discord.Client(intents=discord.Intents.none())
    await client.login(token)
    out=[]
    for r in records:
        code=r["invite"].split("/")[-1]
        try:
            await client.fetch_invite(code)
            out.append(r)
        except:
            print("Invalid invite:",r["invite"])
    await client.close()
    return out


def main():
    print("Starting ingest")
    existing=load_existing(OUT_FILE)

    root=get_soup(TAGS_ROOT)
    sections=discover_sections(root)

    scraped=[]
    for sec in sections:
        print("SECTION",sec)
        try:
            soup=get_soup(sec)
        except:
            continue

        last=discover_last_page(sec,soup)

        for page in range(1,last+1):
            url=section_page(sec,page)
            print("GET",url)
            try:
                soup=get_soup(url)
            except:
                continue
            recs=extract_candidates(soup,sec)
            scraped.extend(recs)
            print("found",len(recs))
            sleep()

    scraped=dedupe([r for r in scraped if r.get("tag") and r.get("invite")])
    print("scraped total",len(scraped))

    if VALIDATE_INVITES:
        import asyncio
        scraped=asyncio.run(validate_with_discord(scraped))
        scraped=dedupe(scraped)

    if REPLACE_FULL_SNAPSHOT:
        output=scraped
    else:
        existing_pairs={(r["tag"],r["invite"]) for r in existing}
        new=[r for r in scraped if (r["tag"],r["invite"]) not in existing_pairs]
        output=dedupe(existing+new)

    if not KEEP_DUPLICATE_TAGS:
        bytag={}
        for r in output:
            bytag.setdefault(r["tag"],r)
        output=list(bytag.values())

    print("output size",len(output))

    if len(output) < MIN_TOTAL_RECORDS_TO_WRITE:
        print("Refusing to write; scrape too small")
        return

    write_backup(OUT_FILE)

    with open(OUT_FILE,"w",encoding="utf-8") as f:
        json.dump(output,f,indent=2,ensure_ascii=False)

    print("Wrote",len(output),"records")


if __name__ == "__main__":
    main()
