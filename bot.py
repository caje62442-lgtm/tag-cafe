import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands

# =========================================================
# Minimal HTTP server (Render Web Service needs an open port)
# =========================================================
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format, *args):
        return


def run_web_server():
    port = int(os.environ.get("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


threading.Thread(target=run_web_server, daemon=True).start()

# ======================
# Simple JSON "database"
# ======================
TAGS_FILE = "tags.json"
TAGS_CACHE: List[Dict[str, Any]] = []

INVITE_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:discord\.gg|discord(?:app)?\.com/invite|discord\.com/invite)/(?P<code>[A-Za-z0-9-]+)",
    re.IGNORECASE,
)


def normalize_tag(tag: str) -> str:
    return tag.strip().upper()


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


def load_tags_from_disk() -> List[Dict[str, Any]]:
    if not os.path.exists(TAGS_FILE):
        print(f"[load_tags_from_disk] {TAGS_FILE} does not exist. Using empty dataset.")
        return []

    try:
        with open(TAGS_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception as exc:
        print(f"[load_tags_from_disk] Failed to parse {TAGS_FILE}: {exc!r}")
        return []

    output: List[Dict[str, Any]] = []

    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue

            tag = normalize_tag(str(item.get("tag", "")))
            invite = normalize_invite(str(item.get("invite", "")))
            if not tag or not invite:
                continue

            output.append({"tag": tag, "invite": invite})

    elif isinstance(data, dict):
        for key, value in data.items():
            tag = normalize_tag(str(key))
            invite = normalize_invite(str(value))
            if not tag or not invite:
                continue

            output.append({"tag": tag, "invite": invite})

    print(f"[load_tags_from_disk] Loaded {len(output)} records from {TAGS_FILE}")
    return output


def refresh_tags_cache() -> None:
    global TAGS_CACHE
    TAGS_CACHE = load_tags_from_disk()
    print(f"[refresh_tags_cache] Cache now has {len(TAGS_CACHE)} records")


def load_tags() -> List[Dict[str, Any]]:
    return TAGS_CACHE


def search_tags(query: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    q = normalize_tag(query)
    if not q:
        return []
    return [item for item in data if q in item["tag"]]


# ======================
# Invite parsing + fetch
# ======================
def extract_invite_code(invite: str) -> str:
    invite = invite.strip()
    match = INVITE_RE.search(invite)
    if match:
        return match.group("code")
    return invite.strip().strip("/")


async def fetch_invite_preview(bot: commands.Bot, invite_value: str) -> Dict[str, Optional[str]]:
    code = extract_invite_code(invite_value)

    try:
        invite = await bot.fetch_invite(code, with_counts=True)
        guild = invite.guild

        icon_url = None
        if guild and getattr(guild, "icon", None):
            icon_url = guild.icon.url

        member_count = getattr(invite, "approximate_member_count", None)
        online_count = getattr(invite, "approximate_presence_count", None)

        return {
            "code": code,
            "guild_name": guild.name if guild else None,
            "icon_url": icon_url,
            "invite_url": f"https://discord.gg/{code}",
            "member_count": str(member_count) if member_count is not None else None,
            "online_count": str(online_count) if online_count is not None else None,
        }
    except Exception as exc:
        print(f"[fetch_invite_preview] Failed for {invite_value!r}: {exc!r}")
        return {
            "code": code,
            "guild_name": None,
            "icon_url": None,
            "invite_url": invite_value if invite_value.startswith("http") else f"https://discord.gg/{code}",
            "member_count": None,
            "online_count": None,
        }


# ======================
# Discord bot
# ======================
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

GUILD_ID_ENV = os.getenv("GUILD_ID")
GUILD_ID = int(GUILD_ID_ENV) if GUILD_ID_ENV and GUILD_ID_ENV.isdigit() else None


def fmt_int(n: Optional[str]) -> Optional[str]:
    if n is None:
        return None
    try:
        return f"{int(n):,}"
    except Exception:
        return n


def make_embed(entry: Dict[str, Any], preview: Dict[str, Optional[str]], index: int, total: int) -> discord.Embed:
    tag = entry["tag"]
    invite_url = preview.get("invite_url") or entry["invite"]
    guild_name = preview.get("guild_name") or "Unknown server"

    member_count = fmt_int(preview.get("member_count"))
    online_count = fmt_int(preview.get("online_count"))
    icon_url = preview.get("icon_url")

    embed = discord.Embed(
        title=f"TAG: {tag}",
        color=0x2F3136,
    )

    embed.add_field(name="SERVER", value=guild_name, inline=True)

    if member_count and online_count:
        embed.add_field(name="MEMBERS", value=f"{member_count}\n({online_count} online)", inline=True)
    elif member_count:
        embed.add_field(name="MEMBERS", value=member_count, inline=True)
    else:
        embed.add_field(name="MEMBERS", value="—", inline=True)

    embed.add_field(name="INVITE LINK", value=invite_url, inline=False)

    if icon_url:
        embed.set_image(url=icon_url)

    if total > 1:
        embed.set_footer(text=f"Page {index + 1} of {total}")

    return embed


class TagPager(discord.ui.View):
    def __init__(self, results: List[Dict[str, Any]], owner_id: int):
        super().__init__(timeout=180)
        self.results = results
        self.owner_id = owner_id
        self.i = 0
        self.preview_cache: Dict[int, Dict[str, Optional[str]]] = {}

        self.prev_button = discord.ui.Button(label="Previous", style=discord.ButtonStyle.success)
        self.next_button = discord.ui.Button(label="Next", style=discord.ButtonStyle.success)
        self.join_button = discord.ui.Button(label="Join Server", style=discord.ButtonStyle.primary)

        self.prev_button.callback = self.on_prev
        self.next_button.callback = self.on_next
        self.join_button.callback = self.on_join

        self.add_item(self.prev_button)
        self.add_item(self.next_button)
        self.add_item(self.join_button)

        self._sync_buttons()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the person who ran the command can use these buttons.",
                ephemeral=True,
            )
            return False
        return True

    def _sync_buttons(self):
        total = len(self.results)
        if total <= 1:
            self.prev_button.disabled = True
            self.next_button.disabled = True
        else:
            self.prev_button.disabled = self.i == 0
            self.next_button.disabled = self.i == total - 1

    async def _get_preview(self, idx: int) -> Dict[str, Optional[str]]:
        if idx in self.preview_cache:
            return self.preview_cache[idx]

        entry = self.results[idx]
        preview = await fetch_invite_preview(bot, entry["invite"])
        self.preview_cache[idx] = preview
        return preview

    async def _render_current(self, interaction: discord.Interaction):
        preview = await self._get_preview(self.i)
        embed = make_embed(self.results[self.i], preview, self.i, len(self.results))
        self._sync_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    async def on_prev(self, interaction: discord.Interaction):
        self.i -= 1
        await self._render_current(interaction)

    async def on_next(self, interaction: discord.Interaction):
        self.i += 1
        await self._render_current(interaction)

    async def on_join(self, interaction: discord.Interaction):
        preview = await self._get_preview(self.i)
        invite_url = preview.get("invite_url") or self.results[self.i]["invite"]
        await interaction.response.send_message(invite_url, ephemeral=True)

    async def on_timeout(self):
        self.prev_button.disabled = True
        self.next_button.disabled = True
        self.join_button.disabled = True


@bot.event
async def on_ready():
    print(f"[on_ready] Online as {bot.user} (id={bot.user.id})")
    refresh_tags_cache()

    try:
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            synced = await bot.tree.sync(guild=guild)
            print("[on_ready] Synced to guild:", [command.name for command in synced])
        else:
            synced = await bot.tree.sync()
            print("[on_ready] Synced globally:", [command.name for command in synced])
    except Exception as exc:
        print("[on_ready] Slash sync failed:", repr(exc))


@bot.event
async def on_resumed():
    print("[on_resumed] Gateway session resumed")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    print(f"[app_command_error] Command error: {error!r}")

    try:
        if interaction.response.is_done():
            await interaction.followup.send("Something went wrong while handling that command.", ephemeral=True)
        else:
            await interaction.response.send_message("Something went wrong while handling that command.", ephemeral=True)
    except Exception as exc:
        print(f"[app_command_error] Failed to send error response: {exc!r}")


@bot.tree.command(name="ping", description="Test if the bot is alive")
async def ping(interaction: discord.Interaction):
    print(f"[ping] Received from user_id={interaction.user.id}")
    await interaction.response.defer(ephemeral=True, thinking=False)
    await interaction.followup.send("Pong!", ephemeral=True)
    print("[ping] Responded successfully")


@bot.tree.command(name="reloadtags", description="Reload tags.json into memory")
async def reloadtags(interaction: discord.Interaction):
    print(f"[reloadtags] Received from user_id={interaction.user.id}")
    await interaction.response.defer(ephemeral=True, thinking=False)
    refresh_tags_cache()
    await interaction.followup.send(f"Reloaded {len(TAGS_CACHE):,} tags into memory.", ephemeral=True)
    print("[reloadtags] Responded successfully")


@bot.tree.command(name="searchtag", description="Search for a guild tag and get an invite.")
@app_commands.describe(tag="The tag to search for (example: PAWS)")
async def searchtag(interaction: discord.Interaction, tag: str):
    print(f"[searchtag] Received query={tag!r} from user_id={interaction.user.id}")
    await interaction.response.defer(ephemeral=True)

    data = load_tags()
    results = search_tags(tag, data)

    print(f"[searchtag] Cache size={len(data)} results={len(results)}")

    if not results:
        await interaction.followup.send(
            f"No matches found for **{normalize_tag(tag)}**.",
            ephemeral=True,
        )
        return

    view = TagPager(results=results, owner_id=interaction.user.id)
    preview0 = await view._get_preview(0)
    embed = make_embed(results[0], preview0, 0, len(results))

    await interaction.followup.send(embed=embed, view=view, ephemeral=False)
    print("[searchtag] Responded successfully")


token = os.getenv("DISCORD_TOKEN")
if not token:
    raise RuntimeError("DISCORD_TOKEN is not set in Render Environment Variables.")

print("[startup] Refreshing initial cache before connecting")
refresh_tags_cache()
print("[startup] Starting bot")
bot.run(token)
