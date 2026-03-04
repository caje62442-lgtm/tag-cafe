import os
import re
import json
import time
from typing import List, Dict, Optional, Set

import praw
import discord

# =========================================================
# Config
# =========================================================
SUBREDDITS = os.getenv(
    "REDDIT_SUBREDDITS",
    "discordservers,DiscordAdvertising,DiscordServer,DiscordServers"
).split(",")

POST_LIMIT_PER_SUB = int(os.getenv("REDDIT_POST_LIMIT", "200"))      # per subreddit
SLEEP_SECONDS = float(os.getenv("SCRAPE_SLEEP", "0.3"))              # be polite
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "tags.json")

# Extract [TAG] from title like: "[PAWS] join our server"
BRACKET_TAG_RE = re.compile(r"\[(?P<tag>[A-Za-z0-9]{2,12})\]")

# Find Discord invite links
INVITE_RE = re.compile(
    r"(https?://(?:www\.)?(?:discord\.gg|discord\.com/invite)/[A-Za-z0-9-]+)",
    re.IGNORECASE,
)

def pick_tag_from_text(title: str, body: str) -> Optional[str]:
    """
    Preferred: [TAG] in the title.
    Fallback: first 2–12 char alnum token that looks like a tag, uppercase.
    """
    m = BRACKET_TAG_RE.search(title or "")
    if m:
        return m.group("tag").upper().strip()

    # fallback: find a plausible short token in title
    tokens = re.findall(r"\b[A-Za-z0-9]{2,12}\b", title or "")
    for t in tokens:
        # skip common junk words
        if t.lower() in {"discord", "server", "join", "invite", "new", "the", "and"}:
            continue
        return t.upper().strip()

    # last resort: try body
    tokens = re.findall(r"\b[A-Za-z0-9]{2,12}\b", body or "")
    for t in tokens:
        if t.lower() in {"discord", "server", "join", "invite", "new", "the", "and"}:
            continue
        return t.upper().strip()

    return None

def pick_invite_from_text(text: str) -> Optional[str]:
    m = INVITE_RE.search(text or "")
    if m:
        return m.group(1).strip()
    return None

async def validate_invites(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Validates invites via Discord API.
    Keeps duplicates by tag (so paging can cycle multiple servers with same tag),
    but de-dupes exact invite URLs to avoid spam.
    """
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
            # with_counts=False so we don't store counts; your bot can fetch counts live
            await client.fetch_invite(code, with_counts=False)
            valid.append(rec)
        except Exception:
            pass

    await client.close()
    return valid

def write_tags_json(records: List[Dict[str, str]], path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(records)} records to {path}")

def main():
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT", "tag-cafe-bot")

    if not client_id or not client_secret:
        raise RuntimeError("Missing REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET.")

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )

    all_records: List[Dict[str, str]] = []
    total_scanned = 0

    for sub in [s.strip() for s in SUBREDDITS if s.strip()]:
        print(f"=== SUBREDDIT: r/{sub} ===")
        subreddit = reddit.subreddit(sub)

        try:
            # "new" is more likely to have fresh invites that still work
            for post in subreddit.new(limit=POST_LIMIT_PER_SUB):
                total_scanned += 1

                title = post.title or ""
                body = post.selftext or ""
                text_blob = f"{title}\n{body}"

                invite = pick_invite_from_text(text_blob)
                if not invite:
                    continue

                tag = pick_tag_from_text(title, body)
                if not tag:
                    continue

                all_records.append({"tag": tag, "invite": invite})

                # be polite
                time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print(f"ERROR reading r/{sub}: {repr(e)}")

    print(f"Scanned {total_scanned} posts.")
    print(f"Collected {len(all_records)} candidates before validation.")

    # Validate (recommended)
    import asyncio
    valid = asyncio.run(validate_invites(all_records))
    print(f"Validated {len(valid)} invites.")

    write_tags_json(valid, OUTPUT_PATH)

if __name__ == "__main__":
    main()
