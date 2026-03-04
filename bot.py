import os
import json
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import List, Dict, Any, Optional

import discord
from discord.ext import commands
from discord import app_commands

# =========================================================
# Minimal HTTP server (Render Web Service needs an open port)
# =========================================================
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok")

def run_web_server():
    port = int(os.environ.get("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()

threading.Thread(target=run_web_server, daemon=True).start()

# ======================
# Simple JSON "database"
# ======================
TAGS_FILE = "tags.json"

def load_tags() -> List[Dict[str, Any]]:
    if not os.path.exists(TAGS_FILE):
        return []

    with open(TAGS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        out = []
        for item in data:
            if not isinstance(item, dict):
                continue
            tag = str(item.get("tag", "")).upper().strip()
            invite = str(item.get("invite", "")).strip()
            if tag and invite:
                out.append({"tag": tag, "invite": invite})
        return out

    if isinstance(data, dict):
        out = []
        for k, v in data.items():
            if k and v:
                out.append({"tag": str(k).upper().strip(), "invite": str(v).strip()})
        return out

    return []

def search_tags(query: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    q = query.strip().upper()
    if not q:
        return []
    return [x for x in data if q in x["tag"]]

# ======================
# Invite parsing + fetch
# ======================
INVITE_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:discord\.gg|discord(?:app)?\.com/invite)/(?P<code>[A-Za-z0-9-]+)",
    re.IGNORECASE,
)

def extract_invite_code(invite: str) -> str:
    invite = invite.strip()
    m = INVITE_RE.search(invite)
    if m:
        return m.group("code")
    return invite.strip().strip("/")

async def fetch_invite_preview(bot: commands.Bot, invite_value: str) -> Dict[str, Optional[str]]:
    code = extract_invite_code(invite_value)
    try:
        inv = await bot.fetch_invite(code, with_counts=False)
        guild = inv.guild

        icon_url = None
        if guild and getattr(guild, "icon", None):
            icon_url = guild.icon.url

        return {
            "code": code,
            "guild_name": guild.name if guild else None,
            "icon_url": icon_url,
            "invite_url": f"https://discord.gg/{code}",
        }
    except Exception:
        return {
            "code": code,
            "guild_name": None,
            "icon_url": None,
            "invite_url": invite_value if invite_value.startswith("http") else f"https://discord.gg/{code}",
        }

# ======================
# Discord bot
# ======================
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

GUILD_ID_ENV = os.getenv("GUILD_ID")
GUILD_ID = int(GUILD_ID_ENV) if GUILD_ID_ENV and GUILD_ID_ENV.isdigit() else None

def make_embed(entry: Dict[str, Any], preview: Dict[str, Optional[str]], index: int, total: int) -> discord.Embed:
    tag = entry["tag"]
    invite_url = preview.get("invite_url") or entry["invite"]
    guild_name = preview.get("guild_name") or "Unknown server"
    icon_url = preview.get("icon_url")

    # Bigger "card" layout: title + multiple fields
    emb = discord.Embed(
        title=f"Guild Tag: {tag}",
        description="",
        color=0x5865F2  # Discord blurple feels “native”
    )

    emb.add_field(name="Server", value=guild_name, inline=False)
    emb.add_field(name="Invite", value=f"[Join this server]({invite_url})\n{invite_url}", inline=False)

    if icon_url:
        emb.set_thumbnail(url=icon_url)

    # Only show pager text if there are multiple results
    if total > 1:
        emb.set_footer(text=f"Result {index + 1} of {total}")

    return emb

class TagPager(discord.ui.View):
    def __init__(self, results: List[Dict[str, Any]], owner_id: int):
        super().__init__(timeout=180)
        self.results = results
        self.owner_id = owner_id
        self.i = 0
        self.preview_cache: Dict[int, Dict[str, Optional[str]]] = {}

        # Button order: Previous | Next | Join Server (cleaner)
        self.prev_button = discord.ui.Button(label="Previous", style=discord.ButtonStyle.secondary)
        self.next_button = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary)
        self.join_button = discord.ui.Button(label="Join Server", style=discord.ButtonStyle.link, url="https://discord.gg/")

        self.prev_button.callback = self.on_prev
        self.next_button.callback = self.on_next

        self.add_item(self.prev_button)
        self.add_item(self.next_button)
        self.add_item(self.join_button)

        self._sync_buttons()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the person who ran the command can use these buttons.",
                ephemeral=True
            )
            return False
        return True

    def _sync_buttons(self):
        total = len(self.results)
        if total <= 1:
            self.prev_button.disabled = True
            self.next_button.disabled = True
        else:
            self.prev_button.disabled = (self.i == 0)
            self.next_button.disabled = (self.i == total - 1)

    async def _get_preview(self, idx: int) -> Dict[str, Optional[str]]:
        if idx in self.preview_cache:
            return self.preview_cache[idx]
        entry = self.results[idx]
        preview = await fetch_invite_preview(bot, entry["invite"])
        self.preview_cache[idx] = preview
        return preview

    async def _render_current(self, interaction: discord.Interaction):
        preview = await self._get_preview(self.i)
        invite_url = preview.get("invite_url") or self.results[self.i]["invite"]
        self.join_button.url = invite_url

        self._sync_buttons()
        emb = make_embed(self.results[self.i], preview, self.i, len(self.results))
        await interaction.response.edit_message(embed=emb, view=self)

    async def on_prev(self, interaction: discord.Interaction):
        self.i -= 1
        await self._render_current(interaction)

    async def on_next(self, interaction: discord.Interaction):
        self.i += 1
        await self._render_current(interaction)

    async def on_timeout(self):
        self.prev_button.disabled = True
        self.next_button.disabled = True

@bot.event
async def on_ready():
    print(f"Online as {bot.user} (id={bot.user.id})")
    try:
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            synced = await bot.tree.sync(guild=guild)
            print("Synced to guild:", [c.name for c in synced])
        else:
            synced = await bot.tree.sync()
            print("Synced globally:", [c.name for c in synced])
    except Exception as e:
        print("Slash sync failed:", repr(e))

@bot.tree.command(name="ping", description="Test if the bot is alive")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("Pong!", ephemeral=True)

@bot.tree.command(name="searchtag", description="Search for a guild tag and get an invite.")
@app_commands.describe(tag="The tag to search for (example: PAWS)")
async def searchtag(interaction: discord.Interaction, tag: str):
    await interaction.response.defer(ephemeral=True)

    data = load_tags()
    results = search_tags(tag, data)

    if not results:
        await interaction.followup.send(
            f"No matches found for **{tag.strip().upper()}**.",
            ephemeral=True
        )
        return

    view = TagPager(results=results, owner_id=interaction.user.id)

    preview0 = await view._get_preview(0)
    view.join_button.url = preview0.get("invite_url") or results[0]["invite"]

    emb = make_embed(results[0], preview0, 0, len(results))
    await interaction.followup.send(embed=emb, view=view, ephemeral=False)

token = os.getenv("DISCORD_TOKEN")
if not token:
    raise RuntimeError("DISCORD_TOKEN is not set in Render Environment Variables.")
bot.run(token)
