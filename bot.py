import os
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import List, Dict, Any

import discord
from discord.ext import commands
from discord import app_commands

# -----------------------------
# Minimal HTTP server for Render
# -----------------------------
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

# -----------------------------
# Tag DB
# -----------------------------
TAGS_FILE = "tags.json"

def load_tags() -> List[Dict[str, Any]]:
    if not os.path.exists(TAGS_FILE):
        return []
    with open(TAGS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # list format: [{"tag":"PAWS","invite":"https://discord.gg/..."}]
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

    # dict format: {"PAWS":"https://discord.gg/..."}
    if isinstance(data, dict):
        return [{"tag": str(k).upper(), "invite": str(v).strip()} for k, v in data.items() if k and v]

    return []

def search_tags(query: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    q = query.strip().upper()
    if not q:
        return []
    return [x for x in data if q in x["tag"]]

# -----------------------------
# Discord bot
# -----------------------------
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

def make_embed(entry: Dict[str, Any], index: int, total: int) -> discord.Embed:
    tag = entry["tag"]
    emb = discord.Embed(description=f"**[{tag}]**")
    emb.set_footer(text=f"{index + 1}/{total}")
    return emb

class TagPager(discord.ui.View):
    def __init__(self, results: List[Dict[str, Any]], owner_id: int):
        super().__init__(timeout=180)
        self.results = results
        self.owner_id = owner_id
        self.i = 0

        # Create buttons manually so we can have a LINK button with a dynamic URL.
        self.prev_button = discord.ui.Button(label="Previous", style=discord.ButtonStyle.secondary)
        self.next_button = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary)
        self.join_button = discord.ui.Button(label="Join Server", style=discord.ButtonStyle.link, url=self.results[0]["invite"])

        self.prev_button.callback = self.on_prev
        self.next_button.callback = self.on_next

        self.add_item(self.prev_button)
        self.add_item(self.join_button)
        self.add_item(self.next_button)

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
        self.prev_button.disabled = (total <= 1) or (self.i == 0)
        self.next_button.disabled = (total <= 1) or (self.i == total - 1)

        # Update the Join link to match the current result
        self.join_button.url = self.results[self.i]["invite"]

    async def on_prev(self, interaction: discord.Interaction):
        self.i -= 1
        self._sync_buttons()
        emb = make_embed(self.results[self.i], self.i, len(self.results))
        await interaction.response.edit_message(embed=emb, view=self)

    async def on_next(self, interaction: discord.Interaction):
        self.i += 1
        self._sync_buttons()
        emb = make_embed(self.results[self.i], self.i, len(self.results))
        await interaction.response.edit_message(embed=emb, view=self)

    async def on_timeout(self):
        # Disable paging buttons when expired; keep Join link active.
        self.prev_button.disabled = True
        self.next_button.disabled = True

@bot.event
async def on_ready():
    print(f"Online as {bot.user}")
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} command(s)")
    except Exception as e:
        print("Slash sync failed:", e)

@bot.tree.command(name="ping", description="Test if the bot is alive")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("Pong!", ephemeral=True)

@bot.tree.command(name="searchtag", description="Search for a guild tag and get an invite.")
@app_commands.describe(tag="The tag to search for (example: PAWS)")
async def searchtag(interaction: discord.Interaction, tag: str):
    # Defer to avoid the 3-second timeout in case Render is waking up.
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
    emb = make_embed(results[0], 0, len(results))

    # Show the result publicly, but keep button control locked to the user.
    await interaction.followup.send(embed=emb, view=view, ephemeral=False)

token = os.getenv("DISCORD_TOKEN")
if not token:
    raise RuntimeError("DISCORD_TOKEN is not set in Render Environment Variables.")
bot.run(token)
