import discord
from discord.ext import commands, tasks
from discord import app_commands
import ftplib
import io
import os
import json
import socket
import struct
import datetime
import asyncio
import concurrent.futures

rcon_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

# ══════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════
TOKEN        = os.getenv("DISCORD_TOKEN")
GUILD_ID     = 1481221875324555277

# Channels
CHANNEL_ANNOUNCEMENTS  = 1481224782786724030
CHANNEL_RULES          = 1481224827745603685
CHANNEL_LINKS          = 1481224857906581589
CHANNEL_SERVER_INFO    = 1481224906749378590
CHANNEL_CHANGELOG      = 1481224938584019035
CHANNEL_GENERAL        = 1481224986994938030
CHANNEL_BOT_COMMANDS   = 1481225021044162600
CHANNEL_SUGGESTIONS    = 1481225055001383003
CHANNEL_SUPPORT        = 1481225099175526470
CHANNEL_EVENTS         = 1481225133682065458
CHANNEL_MEDIA          = 1481225172039106621
CHANNEL_SELF_PROMO     = 1481225208470831124
CHANNEL_DASHBOARD      = 1481225272547086427
CHANNEL_LEADERBOARD    = 1481225303555575949
CHANNEL_RANKS          = 1481225333477740646
CHANNEL_WEEKLY_RECAP   = 1481225364708528260
CHANNEL_SERVER_STATUS  = 1481225418244886680
CHANNEL_NOTIFICATIONS  = 1481225447726645299
CHANNEL_INGAME_CHAT    = 1481225477904400424
CHANNEL_VIP_GENERAL    = 1481225516219629671
CHANNEL_VIP_PERKS      = 1481225554987454484
VOICE_PLAYERS_ONLINE   = 1481228273722851421
VOICE_MEMBERS          = 1481228388516757524

# Roles
ROLE_MEMBER  = int(os.getenv("ROLE_MEMBER", "0"))
ROLE_MASTER  = int(os.getenv("ROLE_MASTER", "0"))
ROLE_VIP     = int(os.getenv("ROLE_VIP", "0"))
ROLE_VIP_PLUS= int(os.getenv("ROLE_VIP_PLUS", "0"))
ROLE_MVP     = int(os.getenv("ROLE_MVP", "0"))
ROLE_MVP_PLUS= int(os.getenv("ROLE_MVP_PLUS", "0"))
ROLE_LEGEND  = int(os.getenv("ROLE_LEGEND", "0"))

# FTP
FTP_HOST     = os.getenv("FTP_HOST")
FTP_USER     = os.getenv("FTP_USER")
FTP_PASSWORD = os.getenv("FTP_PASSWORD")

# RCON
RCON_HOST    = os.getenv("RCON_HOST")
RCON_PORT    = int(os.getenv("RCON_PORT") or "25575")
RCON_PASSWORD= os.getenv("RCON_PASSWORD")

# Patreon
PATREON_WEBHOOK_SECRET = os.getenv("PATREON_WEBHOOK_SECRET", "")

# ══════════════════════════════════════════════
# RANK MILESTONES (free ranks by playtime)
# ══════════════════════════════════════════════
FREE_RANKS = [
    {"rank": "newcomer",    "hours": 0,  "chunks": 1,   "force": 1,  "homes": 1,  "color": 0x95A5A6, "label": "Newcomer",    "prefix": "&7[Newcomer]&r"},
    {"rank": "player",      "hours": 2,  "chunks": 5,   "force": 2,  "homes": 2,  "color": 0x2ECC71, "label": "Player",      "prefix": "&a[Player]&r"},
    {"rank": "regular",     "hours": 5,  "chunks": 10,  "force": 4,  "homes": 3,  "color": 0x3498DB, "label": "Regular",     "prefix": "&9[Regular]&r"},
    {"rank": "experienced", "hours": 10, "chunks": 15,  "force": 6,  "homes": 4,  "color": 0x9B59B6, "label": "Experienced", "prefix": "&5[Experienced]&r"},
    {"rank": "veteran",     "hours": 20, "chunks": 20,  "force": 8,  "homes": 6,  "color": 0xF39C12, "label": "Veteran",     "prefix": "&6[Veteran]&r"},
    {"rank": "expert",      "hours": 40, "chunks": 30,  "force": 11, "homes": 10, "color": 0xE74C3C, "label": "Expert",      "prefix": "&c[Expert]&r"},
    {"rank": "master",      "hours": 60, "chunks": 40,  "force": 15, "homes": 999,"color": 0xF1C40F, "label": "Master",      "prefix": "&e[Master]&r"},
]

PREMIUM_RANKS = [
    {"rank": "vip",      "tier": "VIP",    "price": "5€/mo",   "chunks": 100, "force": 20, "homes": 15, "color": 0x2ECC71},
    {"rank": "vip_plus", "tier": "VIP+",   "price": "7.50€/mo","chunks": 200, "force": 30, "homes": 20, "color": 0x3498DB},
    {"rank": "mvp",      "tier": "MVP",    "price": "10€/mo",  "chunks": 300, "force": 45, "homes": 25, "color": 0xF39C12},
    {"rank": "mvp_plus", "tier": "MVP+",   "price": "12.50€/mo","chunks":400, "force": 55, "homes": 30, "color": 0x9B59B6},
    {"rank": "legend",   "tier": "Legend", "price": "15€/mo",  "chunks": 500, "force": 70, "homes": 999,"color": 0xF1C40F},
]

QUEST_MILESTONES = [50, 100, 200, 350, 500, 750, 1000, 1500, 2000, 2500, 3000, 3500, 4038]

def get_rank_for_hours(hours: float) -> dict:
    rank = FREE_RANKS[0]
    for r in FREE_RANKS:
        if hours >= r["hours"]:
            rank = r
    return rank

# ══════════════════════════════════════════════
# FTP HELPERS
# ══════════════════════════════════════════════
def ftp_read(path: str) -> str:
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, 21)
    ftp.login(FTP_USER, FTP_PASSWORD)
    ftp.set_pasv(True)
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {path}", buf.write)
    ftp.quit()
    return buf.getvalue().decode("utf-8", errors="ignore")

def parse_stats(snbt: str) -> dict:
    stats = {}
    def extract(key, label):
        idx = snbt.find(f'"{key}"')
        if idx == -1:
            idx = snbt.find(f"'{key}'")
        if idx == -1:
            return 0
        rest = snbt[idx + len(key) + 2:]
        for i, c in enumerate(rest):
            if c.isdigit():
                end = i
                while end < len(rest) and rest[end].isdigit():
                    end += 1
                return int(rest[i:end])
        return 0
    stats["deaths"]        = extract("minecraft:deaths", "deaths")
    stats["mined"]         = extract("minecraft:blocks_mined", "mined")
    stats["walked_cm"]     = extract("minecraft:walk_one_cm", "walked")
    stats["crafted"]       = extract("minecraft:crafted", "crafted")
    stats["mob_kills"]     = extract("minecraft:mob_kills", "mob_kills")
    return stats

def fetch_all_players():
    players = []
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, 21)
        ftp.login(FTP_USER, FTP_PASSWORD)
        ftp.set_pasv(True)

        # Get player UUIDs from whitelist or usercache
        try:
            buf = io.BytesIO()
            ftp.retrbinary("RETR usercache.json", buf.write)
            cache = json.loads(buf.getvalue().decode("utf-8", errors="ignore"))
        except:
            cache = []

        for entry in cache:
            uuid = entry.get("uuid", "")
            name = entry.get("name", "unknown")
            if not uuid:
                continue

            # Playtime from FTB Ranks data
            playtime_hours = 0.0
            try:
                buf2 = io.BytesIO()
                ftp.retrbinary(f"RETR world/ftbranks/players/{uuid}.json", buf2.write)
                rank_data = json.loads(buf2.getvalue().decode("utf-8", errors="ignore"))
                playtime_ticks = rank_data.get("playtime", 0)
                playtime_hours = round(playtime_ticks / 72000, 2)
            except:
                pass

            # Quest count from FTB Quests
            quests = 0
            try:
                buf3 = io.BytesIO()
                ftp.retrbinary(f"RETR world/ftbquests/players/{uuid}/data.json", buf3.write)
                quest_data = json.loads(buf3.getvalue().decode("utf-8", errors="ignore"))
                quests = len([q for q in quest_data.get("tasks", []) if quest_data.get("tasks", {}).get(q, {}).get("completed", False)])
                if quests == 0:
                    quests = quest_data.get("completed_quests", 0)
            except:
                pass

            # Stats
            raw_stats = {}
            try:
                stats_content = ftp_read(f"world/stats/{uuid}.json")
                raw = json.loads(stats_content)
                s = raw.get("stats", {})
                raw_stats["deaths"]    = s.get("minecraft:custom", {}).get("minecraft:deaths", 0)
                raw_stats["mined"]     = sum(s.get("minecraft:mined", {}).values())
                raw_stats["walked_cm"] = s.get("minecraft:custom", {}).get("minecraft:walk_one_cm", 0)
                raw_stats["crafted"]   = sum(s.get("minecraft:crafted", {}).values())
                raw_stats["mob_kills"] = s.get("minecraft:custom", {}).get("minecraft:mob_kills", 0)
            except:
                raw_stats = {"deaths": 0, "mined": 0, "walked_cm": 0, "crafted": 0, "mob_kills": 0}

            players.append({
                "uuid": uuid,
                "name": name,
                "playtime_hours": playtime_hours,
                "quests": quests,
                **raw_stats
            })

        ftp.quit()
    except Exception as e:
        print(f"[FTP ERROR] {e}")
    return players

# ══════════════════════════════════════════════
# DASHBOARD BUILDER
# ══════════════════════════════════════════════
def build_dashboard_embed(players: list) -> discord.Embed:
    embed = discord.Embed(
        title="📊 HubUniverse — Live Dashboard",
        description="**Server:** 🟢 Online  |  **Modpack:** All The Mods 10",
        color=0x00B4D8,
        timestamp=datetime.datetime.utcnow()
    )

    # Quests leaderboard
    sorted_quests = sorted(players, key=lambda x: x["quests"], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉"]
    quest_lines = []
    for i, p in enumerate(sorted_quests):
        medal = medals[i] if i < 3 else f"`{i+1}.`"
        quest_lines.append(f"{medal} **{p['name']}** — {p['quests']} quests")
    embed.add_field(
        name="🏆 Quest Leaderboard",
        value="\n".join(quest_lines) if quest_lines else "No data yet",
        inline=True
    )

    # Playtime leaderboard
    sorted_time = sorted(players, key=lambda x: x["playtime_hours"], reverse=True)[:10]
    time_lines = []
    for i, p in enumerate(sorted_time):
        medal = medals[i] if i < 3 else f"`{i+1}.`"
        time_lines.append(f"{medal} **{p['name']}** — {p['playtime_hours']}h")
    embed.add_field(
        name="⏱️ Playtime Leaderboard",
        value="\n".join(time_lines) if time_lines else "No data yet",
        inline=True
    )

    embed.add_field(name="\u200b", value="\u200b", inline=False)

    # Global stats
    total_deaths  = sum(p["deaths"] for p in players)
    total_mined   = sum(p["mined"] for p in players)
    total_walked  = sum(p["walked_cm"] for p in players)
    total_crafted = sum(p["crafted"] for p in players)
    total_kills   = sum(p["mob_kills"] for p in players)
    total_km      = round(total_walked / 100000, 1)

    embed.add_field(
        name="🌍 Server Stats",
        value=(
            f"💀 Deaths: **{total_deaths:,}**\n"
            f"⛏️ Blocks mined: **{total_mined:,}**\n"
            f"🚶 Distance walked: **{total_km:,} km**\n"
            f"⚗️ Items crafted: **{total_crafted:,}**\n"
            f"🐾 Mobs killed: **{total_kills:,}**"
        ),
        inline=False
    )

    embed.set_footer(text="Updated every 60 minutes • HubUniverse")
    return embed

def build_leaderboard_embed(players: list) -> discord.Embed:
    embed = discord.Embed(
        title="🏆 HubUniverse — Leaderboard",
        color=0x00B4D8,
        timestamp=datetime.datetime.utcnow()
    )
    medals = ["🥇", "🥈", "🥉"]

    sorted_quests = sorted(players, key=lambda x: x["quests"], reverse=True)[:10]
    quest_lines = []
    for i, p in enumerate(sorted_quests):
        medal = medals[i] if i < 3 else f"`{i+1}.`"
        quest_lines.append(f"{medal} **{p['name']}** — {p['quests']} quests")
    embed.add_field(
        name="📜 Top Questers",
        value="\n".join(quest_lines) if quest_lines else "No data yet",
        inline=False
    )

    sorted_time = sorted(players, key=lambda x: x["playtime_hours"], reverse=True)[:10]
    time_lines = []
    for i, p in enumerate(sorted_time):
        medal = medals[i] if i < 3 else f"`{i+1}.`"
        time_lines.append(f"{medal} **{p['name']}** — {p['playtime_hours']}h")
    embed.add_field(
        name="⏱️ Most Active",
        value="\n".join(time_lines) if time_lines else "No data yet",
        inline=False
    )

    embed.set_footer(text="Updated every 60 minutes • HubUniverse")
    return embed

# ══════════════════════════════════════════════
# RCON
# ══════════════════════════════════════════════
def rcon_send_sync(command: str) -> str:
    """Synchronous RCON call — run in executor to avoid blocking the event loop."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((RCON_HOST, RCON_PORT))

        def send_packet(req_id, req_type, payload):
            payload_enc = payload.encode("utf-8") + b"\x00\x00"
            pkt = struct.pack("<iii", 10 + len(payload_enc), req_id, req_type) + payload_enc
            s.sendall(pkt)

        def recv_packet():
            raw = b""
            while len(raw) < 4:
                raw += s.recv(4096)
            length = struct.unpack("<i", raw[:4])[0]
            while len(raw) < 4 + length:
                raw += s.recv(4096)
            req_id, req_type = struct.unpack("<ii", raw[4:12])
            payload = raw[12:4+length-2].decode("utf-8", errors="ignore")
            return req_id, req_type, payload

        send_packet(1, 3, RCON_PASSWORD)
        recv_packet()
        send_packet(2, 2, command)
        _, _, response = recv_packet()
        s.close()
        return response
    except Exception as e:
        print(f"[RCON ERROR] {e}")
        return ""

def rcon_send(command: str) -> str:
    """Compatibility wrapper — use rcon_send_sync directly in sync contexts."""
    return rcon_send_sync(command)

async def rcon_async(command: str) -> str:
    """Non-blocking async RCON call."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(rcon_executor, rcon_send_sync, command)

async def apply_rank(player_name: str, rank: dict, previous_rank: str = None):
    if previous_rank:
        await rcon_async(f"ftbranks remove {player_name} {previous_rank}")
    result = await rcon_async(f"ftbranks add {player_name} {rank['rank']}")
    print(f"[RCON] Rank '{rank['rank']}' applied to {player_name} — {result}")

async def send_welcome_ingame(player_name: str):
    await rcon_async(f'title {player_name} title {{"text":"Welcome to HubUniverse","color":"aqua","bold":true}}')
    await rcon_async(f'title {player_name} subtitle {{"text":"Your adventure begins now.","color":"white"}}')
    await rcon_async(f'title {player_name} times 20 80 20')
    await rcon_async(f'tellraw {player_name} {{"text":"\\n§b§l══════════════════════════","color":"aqua"}}')
    await rcon_async(f'tellraw {player_name} {{"text":"  §f§lWelcome to §b§lHubUniverse§f§l!","bold":true}}')
    await rcon_async(f'tellraw {player_name} {{"text":"  §7All The Mods 10 | The center of your Minecraft universe."}}')
    await rcon_async(f'tellraw {player_name} {{"text":"  §7Use §b/ranks §7to see your progression."}}')
    await rcon_async(f'tellraw {player_name} {{"text":"§b§l══════════════════════════\\n"}}')
    print(f"[RCON] Welcome sent to {player_name}")

async def send_leaderboard_ingame(player_name: str, players: list):
    sorted_players = sorted(players, key=lambda x: x["quests"], reverse=True)[:3]
    medals = ["§6§l#1", "§7§l#2", "§c§l#3"]
    await rcon_async(f'tellraw {player_name} {{"text":"§b§l══ 🏆 TOP 3 QUESTS ══"}}')
    for i, p in enumerate(sorted_players):
        await rcon_async(f'tellraw {player_name} {{"text":"  {medals[i]} §f{p[chr(110)+chr(97)+chr(109)+chr(101)]} §7— §e{p[chr(113)+chr(117)+chr(101)+chr(115)+chr(116)+chr(115)]} quests"}}')
    await rcon_async(f'tellraw {player_name} {{"text":"§b§l═══════════════════"}}')

# ══════════════════════════════════════════════
# PLAYER TRACKING
# ══════════════════════════════════════════════
player_ranks: dict = {}
known_players: set = set()
linked_players: dict = {}  # discord_id -> minecraft_name

# ══════════════════════════════════════════════
# MILESTONE SYSTEM
# ══════════════════════════════════════════════
import random

# Points per player: { uuid: int }
player_points: dict = {}

# Current active event: None or dict
current_event: dict = None
# Snapshot of stats at event start: { uuid: { mined, mob_kills, crafted } }
event_baseline: dict = {}
# Progress during event: { uuid: int }
event_progress: dict = {}

EVENT_HOURS = [8, 14, 19, 0]  # UTC hours when events start

EVENT_TYPES = [
    {
        "type": "mined",
        "emoji": "⛏️",
        "name": "Mining Frenzy",
        "description": "Mine as many blocks as possible!",
        "goal_range": (200, 500),
        "unit": "blocks mined",
        "points_per_unit": 1,
    },
    {
        "type": "mob_kills",
        "emoji": "⚔️",
        "name": "Monster Hunt",
        "description": "Kill as many mobs as possible!",
        "goal_range": (50, 150),
        "unit": "mobs killed",
        "points_per_unit": 2,
    },
    {
        "type": "crafted",
        "emoji": "🔨",
        "name": "Crafting Rush",
        "description": "Craft as many items as possible!",
        "goal_range": (100, 300),
        "unit": "items crafted",
        "points_per_unit": 1,
    },
]

def generate_event() -> dict:
    template = random.choice(EVENT_TYPES)
    goal = random.randint(*template["goal_range"])
    now = datetime.datetime.utcnow()
    end_time = now + datetime.timedelta(hours=1)
    return {
        **template,
        "goal": goal,
        "start": now,
        "end": end_time,
        "message_id": None,
    }

async def start_milestone_event():
    global current_event, event_baseline, event_progress
    current_event = generate_event()
    event_progress = {}

    # Take baseline snapshot
    loop = asyncio.get_event_loop()
    players = await loop.run_in_executor(None, fetch_all_players)
    event_baseline = {p["uuid"]: p.get(current_event["type"], 0) for p in players}

    # Announce in #events
    ch = bot.get_channel(CHANNEL_EVENTS)
    if not ch:
        return

    end_ts = int(current_event["end"].timestamp())
    embed = discord.Embed(
        title=f"{current_event['emoji']} Community Event — {current_event['name']}",
        description=current_event["description"],
        color=0xF1C40F,
        timestamp=datetime.datetime.utcnow()
    )
    embed.add_field(name="🎯 Goal", value=f"Reach **{current_event['goal']} {current_event['unit']}** as a community!", inline=False)
    embed.add_field(name="⏰ Ends", value=f"<t:{end_ts}:R>", inline=True)
    embed.add_field(name="🪙 Points", value=f"**{current_event['points_per_unit']} point** per {current_event['unit'].split()[-1]}", inline=True)
    embed.set_footer(text="Progress updates every 5 minutes")

    msg = await ch.send("@everyone 🎉 A new community event has started!", embed=embed)
    current_event["message_id"] = msg.id
    print(f"[EVENT] Started: {current_event['name']}")

async def update_milestone_progress():
    global event_progress
    if not current_event:
        return

    loop = asyncio.get_event_loop()
    players = await loop.run_in_executor(None, fetch_all_players)

    total = 0
    for p in players:
        baseline = event_baseline.get(p["uuid"], p.get(current_event["type"], 0))
        progress = max(0, p.get(current_event["type"], 0) - baseline)
        event_progress[p["uuid"]] = {"name": p["name"], "progress": progress}
        total += progress

    # Update event embed
    ch = bot.get_channel(CHANNEL_EVENTS)
    if ch and current_event.get("message_id"):
        try:
            msg = await ch.fetch_message(current_event["message_id"])
            end_ts = int(current_event["end"].timestamp())
            pct = min(int((total / current_event["goal"]) * 20), 20)
            bar = "█" * pct + "░" * (20 - pct)
            progress_pct = round((total / current_event["goal"]) * 100) if current_event["goal"] > 0 else 0

            embed = discord.Embed(
                title=f"{current_event['emoji']} Community Event — {current_event['name']}",
                description=current_event["description"],
                color=0xF1C40F,
                timestamp=datetime.datetime.utcnow()
            )
            embed.add_field(name="🎯 Goal", value=f"**{total}/{current_event['goal']} {current_event['unit']}**", inline=False)
            embed.add_field(name="📊 Progress", value=f"`{bar}` {progress_pct}%", inline=False)
            embed.add_field(name="⏰ Ends", value=f"<t:{end_ts}:R>", inline=True)
            embed.add_field(name="🪙 Points", value=f"**{current_event['points_per_unit']} point** per {current_event['unit'].split()[-1]}", inline=True)
            embed.set_footer(text="Progress updates every 5 minutes")
            await msg.edit(embed=embed)
        except Exception as e:
            print(f"[EVENT] Progress update error: {e}")

async def end_milestone_event():
    global current_event, event_baseline, event_progress, player_points

    if not current_event:
        return

    # Sort by progress
    results = sorted(event_progress.values(), key=lambda x: x["progress"], reverse=True)
    total = sum(r["progress"] for r in results)
    goal_reached = total >= current_event["goal"]

    # Award points by ranking
    RANK_POINTS = [50, 30, 15]
    PARTICIPATION_POINTS = 5
    uuid_list = [uuid for uuid, data in sorted(event_progress.items(), key=lambda x: x[1]["progress"], reverse=True) if data["progress"] > 0]
    
    awarded = {}
    for i, uuid in enumerate(uuid_list):
        pts = RANK_POINTS[i] if i < 3 else PARTICIPATION_POINTS
        player_points[uuid] = player_points.get(uuid, 0) + pts
        awarded[uuid] = pts
    save_points_ftp()

    # Post result in #announcements
    ch = bot.get_channel(CHANNEL_ANNOUNCEMENTS)
    if ch:
        medals = ["🥇", "🥈", "🥉"]
        top_lines = ""
        for i, r in enumerate(results[:3]):
            if r["progress"] == 0:
                break
            uuid_entry = next((u for u, d in event_progress.items() if d["name"] == r["name"]), None)
            pts = awarded.get(uuid_entry, 0)
            top_lines += f"{medals[i]} **{r['name']}** — {r['progress']} {current_event['unit']} (+{pts} pts)\n"

        status = "✅ Goal reached! Amazing work!" if goal_reached else f"❌ Goal not reached ({total}/{current_event['goal']})"
        embed = discord.Embed(
            title=f"{current_event['emoji']} Event Results — {current_event['name']}",
            description=status,
            color=0x2ECC71 if goal_reached else 0xE74C3C,
            timestamp=datetime.datetime.utcnow()
        )
        embed.add_field(name="📊 Total", value=f"**{total} {current_event['unit']}**", inline=True)
        embed.add_field(name="🎯 Goal", value=f"**{current_event['goal']} {current_event['unit']}**", inline=True)
        if top_lines:
            embed.add_field(name="🏆 Top Players", value=top_lines, inline=False)
        embed.add_field(name="🪙 Points awarded", value="🥇 50pts · 🥈 30pts · 🥉 15pts · Others 5pts", inline=False)
        embed.set_footer(text="Use /shop in #bot-commands to spend your points!")
        await ch.send(embed=embed)

    print(f"[EVENT] Ended: {current_event['name']} — total: {total}/{current_event['goal']}")
    current_event = None
    event_baseline = {}
    event_progress = {}

@tasks.loop(minutes=5)
async def check_milestone_events():
    global current_event
    now = datetime.datetime.utcnow()

    # End event if time is up
    if current_event and now >= current_event["end"]:
        await update_milestone_progress()
        await end_milestone_event()
        return

    # Update progress if event running
    if current_event:
        await update_milestone_progress()
        return

    # Start new event if it's the right hour (within 5 min window)
    if now.hour in EVENT_HOURS and now.minute < 5:
        await start_milestone_event()


def save_points_ftp():
    """Save player_points to FTP as points.json"""
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, 21)
        ftp.login(FTP_USER, FTP_PASSWORD)
        ftp.set_pasv(True)
        data = json.dumps(player_points).encode("utf-8")
        ftp.storbinary("STOR points.json", io.BytesIO(data))
        ftp.quit()
        print("[POINTS] Saved to FTP")
    except Exception as e:
        print(f"[POINTS] Save error: {e}")

def load_points_ftp():
    """Load player_points from FTP"""
    global player_points
    try:
        buf = io.BytesIO()
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, 21)
        ftp.login(FTP_USER, FTP_PASSWORD)
        ftp.set_pasv(True)
        ftp.retrbinary("RETR points.json", buf.write)
        ftp.quit()
        player_points = json.loads(buf.getvalue().decode("utf-8"))
        print(f"[POINTS] Loaded {len(player_points)} players from FTP")
    except Exception as e:
        print(f"[POINTS] Load error (first run?): {e}")


# ══════════════════════════════════════════════
# BOT SETUP
# ══════════════════════════════════════════════
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# ══════════════════════════════════════════════
# VIEWS
# ══════════════════════════════════════════════
class RulesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="✅ I accept the rules", style=discord.ButtonStyle.success, custom_id="accept_rules_hu")
    async def accept_rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        role = interaction.guild.get_role(ROLE_MEMBER)
        if role and role in interaction.user.roles:
            await interaction.response.send_message("You've already accepted the rules!", ephemeral=True)
            return
        if role:
            await interaction.user.add_roles(role)
            await interaction.response.send_message("✅ Rules accepted! Welcome to **HubUniverse** 🌌", ephemeral=True)
            print(f"[OK] Member role given to {interaction.user.display_name}")
        else:
            await interaction.response.send_message("✅ Welcome to **HubUniverse** 🌌", ephemeral=True)

# ══════════════════════════════════════════════
# SLASH COMMANDS
# ══════════════════════════════════════════════
@tree.command(name="rank", description="Check your current rank and progression", guild=discord.Object(id=GUILD_ID))
async def rank_command(interaction: discord.Interaction):
    if interaction.channel_id != CHANNEL_BOT_COMMANDS:
        await interaction.response.send_message("Please use this command in #bot-commands.", ephemeral=True)
        return

    discord_id = str(interaction.user.id)
    mc_name = linked_players.get(discord_id)
    if not mc_name:
        await interaction.response.send_message("You haven't linked your Minecraft account yet! Use `/link <username>` first.", ephemeral=True)
        return

    players = fetch_all_players()
    player = next((p for p in players if p["name"].lower() == mc_name.lower()), None)
    if not player:
        await interaction.response.send_message(f"Player `{mc_name}` not found on the server.", ephemeral=True)
        return

    current = get_rank_for_hours(player["playtime_hours"])
    current_idx = next((i for i, r in enumerate(FREE_RANKS) if r["rank"] == current["rank"]), 0)
    next_rank = FREE_RANKS[current_idx + 1] if current_idx + 1 < len(FREE_RANKS) else None

    if next_rank:
        progress = player["playtime_hours"] - current["hours"]
        total = next_rank["hours"] - current["hours"]
        pct = min(int((progress / total) * 20), 20)
        bar = "█" * pct + "░" * (20 - pct)
        next_info = f"\n\n**Next rank:** {next_rank['label']} ({next_rank['hours']}h)\n`{bar}` {round(progress,1)}/{total}h"
    else:
        next_info = "\n\n🏆 **Maximum rank reached!**"

    embed = discord.Embed(
        title=f"🎖️ {interaction.user.display_name}'s Rank",
        color=current["color"]
    )
    embed.add_field(name="Minecraft Username", value=f"`{mc_name}`", inline=True)
    embed.add_field(name="Current Rank", value=f"**{current['label']}**", inline=True)
    embed.add_field(name="Playtime", value=f"⏱️ {player['playtime_hours']}h", inline=True)
    embed.add_field(name="Quests", value=f"📜 {player['quests']}", inline=True)
    pts = player_points.get(player["uuid"], 0)
    embed.add_field(name="🪙 Event Points", value=f"**{pts} pts**", inline=True)
    embed.add_field(name="Progression", value=next_info, inline=False)
    embed.set_thumbnail(url=f"https://minotar.net/avatar/{mc_name}/64")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(name="link", description="Link your Minecraft username to your Discord account", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(username="Your Minecraft username")
async def link_command(interaction: discord.Interaction, username: str):
    if interaction.channel_id != CHANNEL_BOT_COMMANDS:
        await interaction.response.send_message("Please use this command in #bot-commands.", ephemeral=True)
        return

    discord_id = str(interaction.user.id)
    linked_players[discord_id] = username
    await interaction.response.send_message(
        f"✅ Your Discord account is now linked to **{username}**!\nYou can now use `/rank` to check your progression.",
        ephemeral=True
    )
    print(f"[LINK] {interaction.user.display_name} linked to {username}")

# ══════════════════════════════════════════════
# SHOP
# ══════════════════════════════════════════════
SHOP_ITEMS = [
    {"id": "chunks", "emoji": "⛏️", "name": "+10 Force-loaded Chunks", "cost": 50, "description": "Permanently adds 10 force-loaded chunk slots to your account."},
    {"id": "homes",  "emoji": "🏠", "name": "+5 Homes",                 "cost": 30, "description": "Permanently adds 5 home slots to your account."},
    {"id": "star",   "emoji": "⭐", "name": "EventStar Prefix",          "cost": 100, "description": "Adds the ⭐ EventStar prefix to your in-game name."},
]

class ShopView(discord.ui.View):
    def __init__(self, mc_name: str, uuid: str):
        super().__init__(timeout=60)
        self.mc_name = mc_name
        self.uuid = uuid
        for item in SHOP_ITEMS:
            btn = discord.ui.Button(
                label=f"{item['emoji']} {item['name']} — {item['cost']} pts",
                style=discord.ButtonStyle.primary,
                custom_id=f"shop_{item['id']}"
            )
            btn.callback = self.make_callback(item)
            self.add_item(btn)

    def make_callback(self, item):
        async def callback(interaction: discord.Interaction):
            pts = player_points.get(self.uuid, 0)
            if pts < item["cost"]:
                await interaction.response.send_message(
                    f"❌ Not enough points! You have **{pts} pts** but need **{item['cost']} pts**.",
                    ephemeral=True
                )
                return

            # Deduct points
            player_points[self.uuid] = pts - item["cost"]
            save_points_ftp()

            # Apply reward via RCON
            if item["id"] == "chunks":
                # Get current value and increment
                current_chunks = 0
                try:
                    result = await rcon_async(f"ftbranks node list {self.mc_name}")
                    for line in result.split("\n"):
                        if "max_force_loaded" in line:
                            current_chunks = int(''.join(filter(str.isdigit, line.split("=")[-1])))
                except:
                    pass
                new_val = current_chunks + 10
                await rcon_async(f"ftbranks node add {self.mc_name} ftbchunks.max_force_loaded {new_val}")

            elif item["id"] == "homes":
                current_homes = 0
                try:
                    result = await rcon_async(f"ftbranks node list {self.mc_name}")
                    for line in result.split("\n"):
                        if "home.max" in line:
                            current_homes = int(''.join(filter(str.isdigit, line.split("=")[-1])))
                except:
                    pass
                new_val = current_homes + 5
                await rcon_async(f"ftbranks node add {self.mc_name} ftbessentials.home.max {new_val}")

            elif item["id"] == "star":
                await rcon_async(f"ftbranks node add {self.mc_name} ftbranks.name_format <&e⭐ &f{{name}}&r>")

            await interaction.response.send_message(
                f"✅ **{item['emoji']} {item['name']}** purchased! Applied in-game.\n"
                f"You now have **{player_points[self.uuid]} pts** remaining.",
                ephemeral=True
            )
            print(f"[SHOP] {self.mc_name} bought {item['id']} for {item['cost']} pts")
        return callback

@tree.command(name="shop", description="Spend your event points on rewards", guild=discord.Object(id=GUILD_ID))
async def shop_command(interaction: discord.Interaction):
    if interaction.channel_id != CHANNEL_BOT_COMMANDS:
        await interaction.response.send_message("Please use this command in #bot-commands.", ephemeral=True)
        return

    discord_id = str(interaction.user.id)
    mc_name = linked_players.get(discord_id)
    if not mc_name:
        await interaction.response.send_message("You haven't linked your Minecraft account yet! Use `/link <username>` first.", ephemeral=True)
        return

    # Find UUID
    loop = asyncio.get_event_loop()
    players = await loop.run_in_executor(None, fetch_all_players)
    player = next((p for p in players if p["name"].lower() == mc_name.lower()), None)
    if not player:
        await interaction.response.send_message(f"Player `{mc_name}` not found on the server.", ephemeral=True)
        return

    pts = player_points.get(player["uuid"], 0)

    embed = discord.Embed(
        title="🛒 HubUniverse — Event Shop",
        description=f"You have **{pts} pts** to spend!\nEarn points by participating in community events.",
        color=0xF1C40F
    )
    for item in SHOP_ITEMS:
        embed.add_field(
            name=f"{item['emoji']} {item['name']} — {item['cost']} pts",
            value=item["description"],
            inline=False
        )
    embed.set_footer(text="Rewards are applied instantly in-game!")

    await interaction.response.send_message(embed=embed, view=ShopView(mc_name, player["uuid"]), ephemeral=True)

# ══════════════════════════════════════════════
# TASKS
# ══════════════════════════════════════════════
@tasks.loop(minutes=60)
async def update_dashboard():
    print(f"[INFO] Dashboard update ({datetime.datetime.utcnow().strftime('%H:%M')})...")
    try:
        players = fetch_all_players()

        # Dashboard
        ch = bot.get_channel(CHANNEL_DASHBOARD)
        if ch:
            embed = build_dashboard_embed(players)
            found = False
            async for msg in ch.history(limit=5):
                if msg.author == bot.user and not msg.is_system():
                    await msg.edit(embed=embed)
                    found = True
                    break
            if not found:
                msg = await ch.send(embed=embed)
                await msg.pin()
            print("[OK] Dashboard updated.")

        # Leaderboard
        ch_lb = bot.get_channel(CHANNEL_LEADERBOARD)
        if ch_lb:
            embed_lb = build_leaderboard_embed(players)
            found = False
            async for msg in ch_lb.history(limit=5):
                if msg.author == bot.user and not msg.is_system():
                    await msg.edit(embed=embed_lb)
                    found = True
                    break
            if not found:
                msg = await ch_lb.send(embed=embed_lb)
                await msg.pin()
            print("[OK] Leaderboard updated.")

        # Quest notifications
        ch_notif = bot.get_channel(CHANNEL_NOTIFICATIONS)
        if ch_notif:
            for p in players:
                for milestone in QUEST_MILESTONES:
                    if p["quests"] >= milestone:
                        key = f"{p['uuid']}_{milestone}"
                        if key not in notified_milestones:
                            notified_milestones.add(key)
                            embed_notif = discord.Embed(
                                title="🎉 Quest Milestone Reached!",
                                description=f"**{p['name']}** just completed **{milestone} quests**!",
                                color=0x00B4D8
                            )
                            embed_notif.set_thumbnail(url=f"https://minotar.net/avatar/{p['name']}/64")
                            await ch_notif.send(embed=embed_notif)

    except Exception as e:
        print(f"[ERROR] Dashboard update failed: {e}")


@tasks.loop(minutes=30)
async def check_ranks():
    print("[INFO] Checking ranks...")
    try:
        players = fetch_all_players()
        for p in players:
            rank = get_rank_for_hours(p["playtime_hours"])
            current = player_ranks.get(p["uuid"])
            if current != rank["rank"]:
                await apply_rank(p["name"], rank, current)
                player_ranks[p["uuid"]] = rank["rank"]
                print(f"[RANK] {p['name']} → {rank['rank']} ({p['playtime_hours']}h)")

            if p["uuid"] not in known_players:
                known_players.add(p["uuid"])
                if len(known_players) > 1:
                    await send_welcome_ingame(p["name"])
                    await send_leaderboard_ingame(p["name"], players)
    except Exception as e:
        print(f"[ERROR] Rank check failed: {e}")


@tasks.loop(minutes=60)
async def update_voice_stats():
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return

        # Member count
        ch_members = guild.get_channel(VOICE_MEMBERS)
        if ch_members:
            await ch_members.edit(name=f"═══ 👥 {guild.member_count} Members ═══")

        # Players online (from RCON)
        response = await rcon_async("list")
        online = 0
        if response:
            import re
            match = re.search(r"(\d+) of a max", response)
            if match:
                online = int(match.group(1))
        ch_online = guild.get_channel(VOICE_PLAYERS_ONLINE)
        if ch_online:
            await ch_online.edit(name=f"═══ 🌐 {online} Players Online ═══")
        print("[OK] Voice stats updated.")
    except Exception as e:
        print(f"[ERROR] Voice stats: {e}")


@tasks.loop(minutes=60)
async def check_weekly_recap():
    now = datetime.datetime.utcnow()
    if now.weekday() == 6 and now.hour == 20:
        try:
            players = fetch_all_players()
            if not players:
                return
            best_quests = max(players, key=lambda x: x["quests"])
            best_time   = max(players, key=lambda x: x["playtime_hours"])

            embed = discord.Embed(
                title="📅 Weekly Recap",
                description="Here's what happened this week on HubUniverse!",
                color=0x00B4D8,
                timestamp=datetime.datetime.utcnow()
            )
            embed.add_field(
                name="🏆 Best Progression",
                value=f"**{best_quests['name']}** — {best_quests['quests']} quests completed",
                inline=False
            )
            embed.add_field(
                name="⏱️ Most Active Player",
                value=f"**{best_time['name']}** — {best_time['playtime_hours']}h played",
                inline=False
            )
            embed.set_footer(text="See you next week! • HubUniverse")

            ch = bot.get_channel(CHANNEL_WEEKLY_RECAP)
            if ch:
                await ch.send(embed=embed)
                print("[OK] Weekly recap sent.")
        except Exception as e:
            print(f"[ERROR] Weekly recap: {e}")


notified_milestones: set = set()

# ══════════════════════════════════════════════
# EVENTS
# ══════════════════════════════════════════════
@bot.event
async def on_member_join(member: discord.Member):
    ch = bot.get_channel(CHANNEL_ANNOUNCEMENTS)
    if ch:
        embed = discord.Embed(
            title=f"🌌 Welcome to HubUniverse, {member.display_name}!",
            description=(
                f"Hey {member.mention}, welcome aboard!\n\n"
                f"Head over to <#{CHANNEL_RULES}> to accept the rules and unlock the server.\n"
                f"Then check <#{CHANNEL_SERVER_INFO}> to get started!"
            ),
            color=0x00B4D8,
            timestamp=datetime.datetime.utcnow()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="📜 Quests", value=f"<#{CHANNEL_SERVER_INFO}>", inline=True)
        embed.add_field(name="🎖️ Ranks", value=f"<#{CHANNEL_RANKS}>", inline=True)
        embed.add_field(name="💬 Chat", value=f"<#{CHANNEL_GENERAL}>", inline=True)
        embed.set_footer(text=f"Member #{member.guild.member_count} • HubUniverse")
        await ch.send(embed=embed)


@bot.event
async def on_ready():
    print(f"[OK] Bot connected: {bot.user}")
    bot.add_view(RulesView())
    load_points_ftp()

    guild = discord.Object(id=GUILD_ID)
    await tree.sync(guild=guild)
    print("[OK] Slash commands synced.")

    await asyncio.sleep(3)

    # Rules embed
    ch_rules = bot.get_channel(CHANNEL_RULES)
    if ch_rules:
        rules_embed = discord.Embed(
            title="📋 HubUniverse — Server Rules",
            description="Welcome to **HubUniverse**! Please read and accept the rules to access the server.",
            color=0x00B4D8
        )
        rules_embed.add_field(name="1️⃣ Respect", value="Treat all members with respect. No harassment, discrimination, or toxic behavior.", inline=False)
        rules_embed.add_field(name="2️⃣ No Griefing", value="Do not destroy, steal, or modify other players' builds without permission.", inline=False)
        rules_embed.add_field(name="3️⃣ No Cheating", value="No hacks, exploits, or unauthorized mods. Play fair.", inline=False)
        rules_embed.add_field(name="4️⃣ Help Each Other", value="Share knowledge, help newcomers, and contribute to the community!", inline=False)
        rules_embed.add_field(name="5️⃣ English Only", value="Please communicate in English in public channels so everyone can understand.", inline=False)
        rules_embed.set_footer(text="Click the button below to accept the rules and gain access.")
        found = False
        async for msg in ch_rules.history(limit=10):
            if msg.author == bot.user and not msg.is_system():
                await msg.edit(embed=rules_embed, view=RulesView())
                found = True
                break
        if not found:
            msg = await ch_rules.send(embed=rules_embed, view=RulesView())
            await msg.pin()
        print("[OK] #rules updated.")

    # Server info embed
    ch_info = bot.get_channel(CHANNEL_SERVER_INFO)
    if ch_info:
        info_embed = discord.Embed(
            title="🗺️ HubUniverse — Server Info",
            description="Everything you need to get started!",
            color=0x00B4D8
        )
        info_embed.add_field(name="🔌 Server IP", value=f"`{os.getenv('MC_HOST', 'coming soon')}`", inline=True)
        info_embed.add_field(name="📦 Modpack", value="[All The Mods 10](https://www.curseforge.com/minecraft/modpacks/all-the-mods-10)", inline=True)
        info_embed.add_field(name="📜 Quests", value="4,536 tasks to complete!", inline=True)
        info_embed.add_field(name="🎖️ Ranks", value=f"Check <#{CHANNEL_RANKS}> for rank progression", inline=True)
        info_embed.add_field(name="💎 Premium", value="Support us on Patreon for exclusive perks!", inline=True)
        info_embed.add_field(name="💬 Discord ↔ Minecraft", value=f"In-game chat is linked to <#{CHANNEL_INGAME_CHAT}>", inline=True)
        info_embed.set_footer(text="HubUniverse • The center of your Minecraft universe.")
        found = False
        async for msg in ch_info.history(limit=10):
            if msg.author == bot.user and not msg.is_system():
                await msg.edit(embed=info_embed)
                found = True
                break
        if not found:
            msg = await ch_info.send(embed=info_embed)
            await msg.pin()
        print("[OK] #server-info updated.")

    # Ranks embed
    ch_ranks = bot.get_channel(CHANNEL_RANKS)
    if ch_ranks:
        ranks_embed = discord.Embed(
            title="🎖️ HubUniverse — Rank System",
            description="Progress through ranks by playing! Premium ranks are available via Patreon.",
            color=0x00B4D8
        )
        free_lines = ""
        for r in FREE_RANKS:
            homes = "∞" if r["homes"] == 999 else str(r["homes"])
            free_lines += f"**{r['label']}** — {r['hours']}h+ | {r['chunks']} chunks | {r['force']} force-loaded | {homes} homes\n"
        ranks_embed.add_field(name="🆓 Free Ranks (by playtime)", value=free_lines, inline=False)

        premium_lines = ""
        for r in PREMIUM_RANKS:
            homes = "∞" if r["homes"] == 999 else str(r["homes"])
            premium_lines += f"**{r['tier']}** — {r['price']} | {r['chunks']} chunks | {r['force']} force-loaded | {homes} homes\n"
        ranks_embed.add_field(name="💎 Premium Ranks (Patreon)", value=premium_lines, inline=False)
        ranks_embed.add_field(
            name="🔗 How to get a premium rank?",
            value=f"1. Subscribe on Patreon\n2. Link your Discord to Patreon\n3. Use `/link <username>` in <#{CHANNEL_BOT_COMMANDS}>\n4. Your rank will be applied automatically!",
            inline=False
        )
        ranks_embed.add_field(
            name="🪙 Event Points Shop",
            value=(
                "Earn points by participating in community events (4x/day)!\n"
                "🥇 1st: **50 pts** · 🥈 2nd: **30 pts** · 🥉 3rd: **15 pts** · Participation: **5 pts**\n\n"
                "**Spend your points with `/shop`:**\n"
                "⛏️ +10 force-loaded chunks — **50 pts**\n"
                "🏠 +5 homes — **30 pts**\n"
                "⭐ EventStar prefix in-game — **100 pts**"
            ),
            inline=False
        )
        ranks_embed.set_footer(text="Use /rank to check your progression · /shop to spend your points")
        found = False
        async for msg in ch_ranks.history(limit=10):
            if msg.author == bot.user and not msg.is_system():
                await msg.edit(embed=ranks_embed)
                found = True
                break
        if not found:
            msg = await ch_ranks.send(embed=ranks_embed)
            await msg.pin()
        print("[OK] #ranks updated.")

    # Start loops
    if not update_dashboard.is_running():
        update_dashboard.start()
    if not check_ranks.is_running():
        check_ranks.start()
    if not update_voice_stats.is_running():
        update_voice_stats.start()
    if not check_weekly_recap.is_running():
        check_weekly_recap.start()

    if not check_milestone_events.is_running():
        check_milestone_events.start()

    print("[OK] All tasks started.")

bot.run(TOKEN)
