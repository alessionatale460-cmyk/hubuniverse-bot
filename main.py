import discord
from discord.ext import commands, tasks
from discord import app_commands
import ftplib
import io
import os
import re
import json
import time
import socket
import struct
import random
import datetime
import asyncio
import concurrent.futures
from collections import defaultdict

rcon_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

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
CHANNEL_EVENT_INFO     = 1481396757475623002
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

# Staff-only logs channel (set CHANNEL_STAFF_LOGS env var to your staff channel ID)
CHANNEL_STAFF_LOGS = int(os.getenv("CHANNEL_STAFF_LOGS", "0"))

# Roles
ROLE_MEMBER  = int(os.getenv("ROLE_MEMBER",   "0"))
ROLE_MASTER  = int(os.getenv("ROLE_MASTER",   "0"))
ROLE_VIP     = int(os.getenv("ROLE_VIP",      "0"))
ROLE_VIP_PLUS= int(os.getenv("ROLE_VIP_PLUS", "0"))
ROLE_MVP     = int(os.getenv("ROLE_MVP",      "0"))
ROLE_MVP_PLUS= int(os.getenv("ROLE_MVP_PLUS", "0"))
ROLE_LEGEND  = int(os.getenv("ROLE_LEGEND",   "0"))

# FTP
FTP_HOST     = os.getenv("FTP_HOST")
FTP_USER     = os.getenv("FTP_USER")
FTP_PASSWORD = os.getenv("FTP_PASSWORD")

# RCON
RCON_HOST    = os.getenv("RCON_HOST")
RCON_PORT    = int(os.getenv("RCON_PORT") or "25575")
RCON_PASSWORD= os.getenv("RCON_PASSWORD")

# Daily reward settings
DAILY_REWARD_POINTS = 10
DAILY_STREAK_BONUS  = 5   # Extra pts per streak day (day2=+5, day3=+10...)
DAILY_STREAK_MAX    = 7   # Streak caps at 7 days

# Rate limiting
RATE_LIMIT_CALLS  = 3
RATE_LIMIT_WINDOW = 30   # seconds

# FTP player cache TTL
FTP_CACHE_TTL = datetime.timedelta(minutes=10)

# ══════════════════════════════════════════════
# RANK MILESTONES
# ══════════════════════════════════════════════
FREE_RANKS = [
    {"rank": "member",      "hours": 0,  "chunks": 1,   "force": 1,  "homes": 1,  "color": 0x95A5A6, "label": "Member",      "prefix": "&7[Newcomer]&r"},
    {"rank": "player",      "hours": 2,  "chunks": 5,   "force": 2,  "homes": 2,  "color": 0x2ECC71, "label": "Player",      "prefix": "&a[Player]&r"},
    {"rank": "regular",     "hours": 5,  "chunks": 10,  "force": 4,  "homes": 3,  "color": 0x3498DB, "label": "Regular",     "prefix": "&9[Regular]&r"},
    {"rank": "experienced", "hours": 10, "chunks": 15,  "force": 6,  "homes": 4,  "color": 0x9B59B6, "label": "Experienced", "prefix": "&5[Experienced]&r"},
    {"rank": "veteran",     "hours": 20, "chunks": 20,  "force": 8,  "homes": 6,  "color": 0xF39C12, "label": "Veteran",     "prefix": "&6[Veteran]&r"},
    {"rank": "expert",      "hours": 40, "chunks": 30,  "force": 11, "homes": 10, "color": 0xE74C3C, "label": "Expert",      "prefix": "&c[Expert]&r"},
    {"rank": "master",      "hours": 60, "chunks": 40,  "force": 15, "homes": 999,"color": 0xF1C40F, "label": "Master",      "prefix": "&e[Master]&r"},
]

PREMIUM_RANKS = [
    {"rank": "vip",      "tier": "VIP",    "price": "5eu/mo",    "chunks": 100, "force": 20, "homes": 15, "color": 0x2ECC71},
    {"rank": "vip_plus", "tier": "VIP+",   "price": "7.50eu/mo", "chunks": 200, "force": 30, "homes": 20, "color": 0x3498DB},
    {"rank": "mvp",      "tier": "MVP",    "price": "10eu/mo",   "chunks": 300, "force": 45, "homes": 25, "color": 0xF39C12},
    {"rank": "mvp_plus", "tier": "MVP+",   "price": "12.50eu/mo","chunks": 400, "force": 55, "homes": 30, "color": 0x9B59B6},
    {"rank": "legend",   "tier": "Legend", "price": "15eu/mo",   "chunks": 500, "force": 70, "homes": 999,"color": 0xF1C40F},
]

QUEST_MILESTONES = [50, 100, 200, 350, 500, 750, 1000, 1500, 2000, 2500, 3000, 3500, 4038]


def get_rank_for_hours(hours: float) -> dict:
    rank = FREE_RANKS[0]
    for r in FREE_RANKS:
        if hours >= r["hours"]:
            rank = r
    return rank

# ══════════════════════════════════════════════
# RATE LIMITER
# ══════════════════════════════════════════════
_rate_buckets: dict = defaultdict(list)


def is_rate_limited(user_id: int) -> bool:
    now = time.monotonic()
    _rate_buckets[user_id] = [t for t in _rate_buckets[user_id] if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_buckets[user_id]) >= RATE_LIMIT_CALLS:
        return True
    _rate_buckets[user_id].append(now)
    return False

# ══════════════════════════════════════════════
# PERSISTENT DATABASE  (single database.json on FTP)
# ══════════════════════════════════════════════
_db: dict = {
    "points":              {},   # uuid -> int
    "linked_players":      {},   # discord_id -> mc_name
    "daily":               {},   # discord_id -> {"last": ISO, "streak": int}
    "shop_purchases":      {},   # "uuid_item" -> int (purchase count)
    "notified_milestones": [],   # ["uuid_milestone", ...]
}


def _db_save() -> None:
    try:
        ftp = _ftp_connect()
        payload = json.dumps(_db, indent=2).encode("utf-8")
        ftp.storbinary("STOR hubuniverse/database.json", io.BytesIO(payload))
        ftp.quit()
    except Exception as e:
        print(f"[DB] Save error: {e}")


def _db_load() -> None:
    global _db
    try:
        ftp = _ftp_connect()
        buf = io.BytesIO()
        ftp.retrbinary("RETR hubuniverse/database.json", buf.write)
        ftp.quit()
        loaded = json.loads(buf.getvalue().decode("utf-8"))
        for key in _db:
            if key in loaded:
                _db[key] = loaded[key]
        print(f"[DB] Loaded — {len(_db['linked_players'])} links, {len(_db['points'])} point entries.")
    except Exception as e:
        print(f"[DB] Load error (first run?): {e}")


def db_points() -> dict:      return _db["points"]
def db_linked() -> dict:      return _db["linked_players"]
def db_daily() -> dict:       return _db["daily"]
def db_shop() -> dict:        return _db["shop_purchases"]
def db_milestones() -> set:   return set(_db["notified_milestones"])


def db_add_milestone(key: str) -> None:
    if key not in _db["notified_milestones"]:
        _db["notified_milestones"].append(key)

# ══════════════════════════════════════════════
# FTP HELPERS
# ══════════════════════════════════════════════
_ftp_cache:      list                   = []
_ftp_cache_time: datetime.datetime|None = None


def _ftp_connect() -> ftplib.FTP:
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, 21, timeout=10)
    ftp.login(FTP_USER, FTP_PASSWORD)
    ftp.set_pasv(True)
    return ftp


def _ftp_read_file(ftp: ftplib.FTP, path: str) -> str:
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {path}", buf.write)
    return buf.getvalue().decode("utf-8", errors="ignore")


def fetch_all_players(force: bool = False) -> list:
    global _ftp_cache, _ftp_cache_time
    now = datetime.datetime.utcnow()
    if not force and _ftp_cache_time and (now - _ftp_cache_time) < FTP_CACHE_TTL:
        return _ftp_cache

    players = []
    try:
        ftp = _ftp_connect()
        try:
            usercache = json.loads(_ftp_read_file(ftp, "usercache.json"))
        except Exception as e:
            print(f"[FTP] usercache.json error: {e}")
            usercache = []

        for entry in usercache:
            uuid = entry.get("uuid", "")
            name = entry.get("name", "unknown")
            if not uuid:
                continue

            playtime_hours = 0.0
            raw = {"deaths": 0, "mined": 0, "walked_cm": 0, "crafted": 0, "mob_kills": 0}
            try:
                stats_data = json.loads(_ftp_read_file(ftp, f"world/stats/{uuid}.json"))
                s = stats_data.get("stats", {})
                custom = s.get("minecraft:custom", {})
                playtime_hours          = round(custom.get("minecraft:play_time", 0) / 72000, 2)
                raw["deaths"]    = custom.get("minecraft:deaths", 0)
                raw["mined"]     = sum(s.get("minecraft:mined", {}).values())
                raw["walked_cm"] = custom.get("minecraft:walk_one_cm", 0)
                raw["crafted"]   = sum(s.get("minecraft:crafted", {}).values())
                raw["mob_kills"] = custom.get("minecraft:mob_kills", 0)
            except json.JSONDecodeError as e:
                print(f"[FTP] Stats JSON error for {name}: {e}")
            except Exception as e:
                print(f"[FTP] Stats read error for {name}: {e}")

            quests = 0
            try:
                snbt = _ftp_read_file(ftp, f"world/ftbquests/{uuid}.snbt")
                m = re.search(r'task_progress:\s*\{([^}]*)\}', snbt, re.DOTALL)
                if m:
                    quests = len(re.findall(r'[0-9A-Fa-f]{16}:\s*1L', m.group(1)))
            except Exception as e:
                print(f"[FTP] Quest error for {name}: {e}")

            players.append({"uuid": uuid, "name": name, "playtime_hours": playtime_hours, "quests": quests, **raw})

        ftp.quit()
    except Exception as e:
        print(f"[FTP ERROR] fetch_all_players: {e}")
        return _ftp_cache

    _ftp_cache = players
    _ftp_cache_time = now
    print(f"[FTP] Cache refreshed — {len(players)} players.")
    return players

# ══════════════════════════════════════════════
# STAFF LOG HELPER
# ══════════════════════════════════════════════
async def log_action(
    title: str,
    description: str,
    color: int = 0x95A5A6,
    thumbnail_url: str|None = None,
    fields: list|None = None,
    error: bool = False,
) -> None:
    if not CHANNEL_STAFF_LOGS:
        return
    ch = bot.get_channel(CHANNEL_STAFF_LOGS)
    if not ch:
        return
    embed = discord.Embed(
        title=("🚨 " if error else "") + title,
        description=description,
        color=0xE74C3C if error else color,
        timestamp=datetime.datetime.utcnow()
    )
    if thumbnail_url:
        embed.set_thumbnail(url=thumbnail_url)
    if fields:
        for name, value in fields:
            embed.add_field(name=name, value=value, inline=True)
    embed.set_footer(text="HubUniverse Staff Logs")
    try:
        await ch.send(embed=embed)
    except Exception as e:
        print(f"[LOG] Could not send staff log: {e}")

# ══════════════════════════════════════════════
# RCON
# ══════════════════════════════════════════════
def rcon_send_sync(command: str) -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(8)
        s.connect((RCON_HOST, RCON_PORT))

        def send_packet(req_id, req_type, payload):
            enc = payload.encode("utf-8") + b"\x00\x00"
            pkt = struct.pack("<iii", 10 + len(enc), req_id, req_type) + enc
            s.sendall(pkt)

        def recv_packet():
            raw = b""
            while len(raw) < 4:
                chunk = s.recv(4096)
                if not chunk:
                    break
                raw += chunk
            if len(raw) < 4:
                return 0, 0, ""
            length = struct.unpack("<i", raw[:4])[0]
            while len(raw) < 4 + length:
                chunk = s.recv(4096)
                if not chunk:
                    break
                raw += chunk
            if len(raw) < 12:
                return 0, 0, ""
            req_id, req_type = struct.unpack("<ii", raw[4:12])
            payload = raw[12:4 + length - 2].decode("utf-8", errors="ignore")
            return req_id, req_type, payload

        send_packet(1, 3, RCON_PASSWORD)
        recv_packet()
        send_packet(2, 2, command)
        _, _, response = recv_packet()
        s.close()
        return response
    except Exception as e:
        print(f"[RCON ERROR] {command!r}: {e}")
        return ""


async def rcon_async(command: str) -> str:
    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(rcon_executor, rcon_send_sync, command),
        timeout=10
    )


async def get_server_tps() -> float|None:
    try:
        response = await rcon_async("tps")
        m = re.search(r"([\d.]+),\s*([\d.]+),\s*([\d.]+)", response)
        if m:
            return float(m.group(1))
    except Exception as e:
        print(f"[RCON] TPS error: {e}")
    return None


async def apply_rank(player_name: str, rank: dict, previous_rank: str|None = None) -> None:
    try:
        if previous_rank:
            await rcon_async(f"ftbranks remove {player_name} {previous_rank}")
        await rcon_async(f"ftbranks add {player_name} {rank['rank']}")
        print(f"[RCON] {player_name}: {previous_rank} -> {rank['rank']}")
        if previous_rank:
            label = rank["label"]
            await rcon_async(f'title {player_name} title {{"text":"Rank Up!","color":"gold","bold":true}}')
            await rcon_async(f'title {player_name} subtitle {{"text":"You are now {label}!","color":"aqua"}}')
            await rcon_async(f'title {player_name} times 20 80 20')
            await rcon_async(f'tellraw {player_name} ["",{{"text":"\\n"}},{{"text":"  ★ Rank Up! ","color":"gold","bold":true}},{{"text":"You are now ","color":"yellow"}},{{"text":"{label}","color":"aqua","bold":true}},{{"text":"!","color":"yellow"}}]')
            await rcon_async(f'tellraw {player_name} ["",{{"text":"  ➜ ","color":"gray"}},{{"text":"Reconnect to the server to unlock your new perks!","color":"red","bold":true}}]')
            await rcon_async(f'tellraw {player_name} ["",{{"text":"\\n"}}]')
    except Exception as e:
        print(f"[RCON ERROR] apply_rank {player_name}: {e}")


async def send_welcome_ingame(player_name: str) -> None:
    cmds = [
        f'title {player_name} title {{"text":"Welcome to HubUniverse","color":"aqua","bold":true}}',
        f'title {player_name} subtitle {{"text":"Your adventure begins now.","color":"white"}}',
        f'title {player_name} times 20 80 20',
        f'tellraw {player_name} {{"text":"  Welcome to HubUniverse! Use /ranks to see your progression."}}',
    ]
    for cmd in cmds:
        await rcon_async(cmd)


async def send_leaderboard_ingame(player_name: str, players: list) -> None:
    top3 = sorted(players, key=lambda x: x["quests"], reverse=True)[:3]
    await rcon_async(f'tellraw {player_name} {{"text":"TOP 3 QUESTS:"}}')
    for i, p in enumerate(top3):
        await rcon_async(f'tellraw {player_name} {{"text":"  #{i+1} {p[chr(110)+chr(97)+chr(109)+chr(101)]} - {p[chr(113)+chr(117)+chr(101)+chr(115)+chr(116)+chr(115)]} quests"}}')

# ══════════════════════════════════════════════
# PLAYER TRACKING
# ══════════════════════════════════════════════
player_ranks:  dict = {}
known_players: set  = set()

# ══════════════════════════════════════════════
# MILESTONE / EVENT SYSTEM
# ══════════════════════════════════════════════
current_event:  dict|None = None
event_baseline: dict      = {}
event_progress: dict      = {}

EVENT_HOURS = [8, 14, 19, 0]
EVENT_TYPES = [
    {"type": "mined",     "emoji": "⛏️", "name": "Mining Frenzy",  "description": "Mine as many blocks as possible!",  "goal_range": (200, 500), "unit": "blocks mined",  "points_per_unit": 1},
    {"type": "mob_kills", "emoji": "⚔️", "name": "Monster Hunt",   "description": "Kill as many mobs as possible!",   "goal_range": (50, 150),  "unit": "mobs killed",   "points_per_unit": 2},
    {"type": "crafted",   "emoji": "🔨", "name": "Crafting Rush",   "description": "Craft as many items as possible!", "goal_range": (100, 300), "unit": "items crafted", "points_per_unit": 1},
]


def generate_event() -> dict:
    t = random.choice(EVENT_TYPES)
    now = datetime.datetime.utcnow()
    return {**t, "goal": random.randint(*t["goal_range"]), "start": now, "end": now + datetime.timedelta(hours=1), "message_id": None}


async def start_milestone_event() -> None:
    global current_event, event_baseline, event_progress
    current_event = generate_event()
    event_progress = {}
    loop = asyncio.get_running_loop()
    players = await loop.run_in_executor(None, fetch_all_players)
    event_baseline = {p["uuid"]: p.get(current_event["type"], 0) for p in players}

    ch = bot.get_channel(CHANNEL_EVENTS)
    if not ch:
        return
    end_ts = int(current_event["end"].timestamp())
    embed = discord.Embed(title=f"{current_event['emoji']} Community Event - {current_event['name']}", description=current_event["description"], color=0xF1C40F, timestamp=datetime.datetime.utcnow())
    embed.add_field(name="Goal",   value=f"Reach **{current_event['goal']} {current_event['unit']}** as a community!", inline=False)
    embed.add_field(name="Ends",   value=f"<t:{end_ts}:R>", inline=True)
    embed.add_field(name="Points", value=f"**{current_event['points_per_unit']} pt** per {current_event['unit'].split()[-1]}", inline=True)
    embed.set_footer(text="Progress updates every 5 minutes")
    msg = await ch.send("@everyone A new community event has started!", embed=embed)
    current_event["message_id"] = msg.id
    print(f"[EVENT] Started: {current_event['name']}")


async def update_milestone_progress() -> None:
    if not current_event:
        return
    loop = asyncio.get_running_loop()
    players = await loop.run_in_executor(None, fetch_all_players)
    total = 0
    for p in players:
        baseline = event_baseline.get(p["uuid"], p.get(current_event["type"], 0))
        progress = max(0, p.get(current_event["type"], 0) - baseline)
        event_progress[p["uuid"]] = {"name": p["name"], "progress": progress}
        total += progress

    ch = bot.get_channel(CHANNEL_EVENTS)
    if ch and current_event.get("message_id"):
        try:
            msg = await ch.fetch_message(current_event["message_id"])
            pct = min(int((total / current_event["goal"]) * 20), 20)
            bar = "█" * pct + "░" * (20 - pct)
            progress_pct = round((total / current_event["goal"]) * 100) if current_event["goal"] > 0 else 0
            end_ts = int(current_event["end"].timestamp())
            embed = discord.Embed(title=f"{current_event['emoji']} Community Event - {current_event['name']}", description=current_event["description"], color=0xF1C40F, timestamp=datetime.datetime.utcnow())
            embed.add_field(name="Goal",     value=f"**{total}/{current_event['goal']} {current_event['unit']}**", inline=False)
            embed.add_field(name="Progress", value=f"`{bar}` {progress_pct}%", inline=False)
            embed.add_field(name="Ends",     value=f"<t:{end_ts}:R>", inline=True)
            embed.add_field(name="Points",   value=f"**{current_event['points_per_unit']} pt** per {current_event['unit'].split()[-1]}", inline=True)
            embed.set_footer(text="Progress updates every 5 minutes")
            await msg.edit(embed=embed)
        except Exception as e:
            print(f"[EVENT] Progress update error: {e}")


async def end_milestone_event() -> None:
    global current_event, event_baseline, event_progress
    if not current_event:
        return
    results  = sorted(event_progress.values(), key=lambda x: x["progress"], reverse=True)
    total    = sum(r["progress"] for r in results)
    goal_ok  = total >= current_event["goal"]
    uuid_list = [u for u, d in sorted(event_progress.items(), key=lambda x: x[1]["progress"], reverse=True) if d["progress"] > 0]
    RANK_PTS  = [50, 30, 15]
    awarded   = {}
    for i, uuid in enumerate(uuid_list):
        pts = RANK_PTS[i] if i < 3 else 5
        db_points()[uuid] = db_points().get(uuid, 0) + pts
        awarded[uuid] = pts
    _db_save()

    ch = bot.get_channel(CHANNEL_EVENTS)
    if ch:
        medals    = ["1st", "2nd", "3rd"]
        top_lines = ""
        for i, r in enumerate(results[:3]):
            if r["progress"] == 0:
                break
            uuid_entry = next((u for u, d in event_progress.items() if d["name"] == r["name"]), None)
            pts = awarded.get(uuid_entry, 0)
            top_lines += f"**{medals[i]}** **{r['name']}** - {r['progress']} {current_event['unit']} (+{pts} pts)\n"
        status = "Goal reached! Amazing work!" if goal_ok else f"Goal not reached ({total}/{current_event['goal']})"
        embed = discord.Embed(title=f"{current_event['emoji']} Event Results - {current_event['name']}", description=status, color=0x2ECC71 if goal_ok else 0xE74C3C, timestamp=datetime.datetime.utcnow())
        embed.add_field(name="Total", value=f"**{total} {current_event['unit']}**", inline=True)
        embed.add_field(name="Goal",  value=f"**{current_event['goal']} {current_event['unit']}**", inline=True)
        if top_lines:
            embed.add_field(name="Top Players", value=top_lines, inline=False)
        embed.add_field(name="Points awarded", value="1st 50pts / 2nd 30pts / 3rd 15pts / Others 5pts", inline=False)
        embed.set_footer(text="Use /shop in #bot-commands to spend your points!")
        await ch.send(embed=embed)
    print(f"[EVENT] Ended: {current_event['name']} - {total}/{current_event['goal']}")
    current_event = None
    event_baseline = {}
    event_progress = {}


@tasks.loop(minutes=5)
async def check_milestone_events():
    global current_event
    now = datetime.datetime.utcnow()
    if current_event and now >= current_event["end"]:
        await update_milestone_progress()
        await end_milestone_event()
        return
    if current_event:
        await update_milestone_progress()
        return
    if now.hour in EVENT_HOURS and now.minute < 5:
        await start_milestone_event()

# ══════════════════════════════════════════════
# DASHBOARD BUILDER
# ══════════════════════════════════════════════
def build_dashboard_embed(players: list, tps: float|None = None) -> discord.Embed:
    embed = discord.Embed(title="HubUniverse - Live Dashboard", description="Server: Online | Modpack: All The Mods 10", color=0x00B4D8, timestamp=datetime.datetime.utcnow())
    medals = ["1st", "2nd", "3rd"]

    sq = sorted(players, key=lambda x: x["quests"], reverse=True)[:10]
    embed.add_field(name="Quest Leaderboard", value="\n".join(f"**{medals[i] if i < 3 else str(i+1)+'.'} {p['name']}** - {p['quests']} quests" for i, p in enumerate(sq)) or "No data", inline=True)

    st = sorted(players, key=lambda x: x["playtime_hours"], reverse=True)[:10]
    embed.add_field(name="Playtime Leaderboard", value="\n".join(f"**{medals[i] if i < 3 else str(i+1)+'.'} {p['name']}** - {p['playtime_hours']}h" for i, p in enumerate(st)) or "No data", inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=False)

    tps_str = ""
    if tps is not None:
        icon = "Green" if tps >= 19 else ("Yellow" if tps >= 15 else "Red")
        tps_str = f"\nTPS ({icon}): **{tps:.1f}**/20"

    embed.add_field(name="Server Stats", value=(
        f"Deaths: **{sum(p['deaths'] for p in players):,}**\n"
        f"Blocks mined: **{sum(p['mined'] for p in players):,}**\n"
        f"Distance: **{round(sum(p['walked_cm'] for p in players)/100000,1):,} km**\n"
        f"Items crafted: **{sum(p['crafted'] for p in players):,}**\n"
        f"Mobs killed: **{sum(p['mob_kills'] for p in players):,}**"
        f"{tps_str}"
    ), inline=False)
    embed.set_footer(text="Updated every 60 minutes - HubUniverse")
    return embed


def build_leaderboard_embed(players: list) -> discord.Embed:
    embed = discord.Embed(title="HubUniverse - Leaderboard", color=0x00B4D8, timestamp=datetime.datetime.utcnow())
    medals = ["1st", "2nd", "3rd"]
    sq = sorted(players, key=lambda x: x["quests"], reverse=True)[:10]
    embed.add_field(name="Top Questers", value="\n".join(f"**{medals[i] if i < 3 else str(i+1)+'.'} {p['name']}** - {p['quests']} quests" for i, p in enumerate(sq)) or "No data", inline=False)
    st = sorted(players, key=lambda x: x["playtime_hours"], reverse=True)[:10]
    embed.add_field(name="Most Active",  value="\n".join(f"**{medals[i] if i < 3 else str(i+1)+'.'} {p['name']}** - {p['playtime_hours']}h" for i, p in enumerate(st)) or "No data", inline=False)
    embed.set_footer(text="Updated every 60 minutes - HubUniverse")
    return embed

# ══════════════════════════════════════════════
# BOT SETUP
# ══════════════════════════════════════════════
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot  = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# ══════════════════════════════════════════════
# VIEWS
# ══════════════════════════════════════════════
class RulesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="I accept the rules", style=discord.ButtonStyle.success, custom_id="accept_rules_hu")
    async def accept_rules(self, interaction: discord.Interaction, button: discord.ui.Button):
        role = interaction.guild.get_role(ROLE_MEMBER)
        if role and role in interaction.user.roles:
            await interaction.response.send_message("You've already accepted the rules!", ephemeral=True)
            return
        if role:
            await interaction.user.add_roles(role)
        await interaction.response.send_message("Rules accepted! Welcome to HubUniverse!", ephemeral=True)

# ══════════════════════════════════════════════
# SLASH COMMANDS
# ══════════════════════════════════════════════
def _bot_channel_only(interaction: discord.Interaction) -> bool:
    return interaction.channel_id == CHANNEL_BOT_COMMANDS


@tree.command(name="rank", description="Check your current rank and progression", guild=discord.Object(id=GUILD_ID))
async def rank_command(interaction: discord.Interaction):
    if not _bot_channel_only(interaction):
        await interaction.response.send_message(f"Please use this command in <#{CHANNEL_BOT_COMMANDS}>.", ephemeral=True)
        return
    if is_rate_limited(interaction.user.id):
        await interaction.response.send_message("You're using commands too fast! Please wait a few seconds.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    discord_id = str(interaction.user.id)
    mc_name = db_linked().get(discord_id)
    if not mc_name:
        await interaction.followup.send("You haven't linked your Minecraft account yet! Use `/link <username>` first.", ephemeral=True)
        return

    loop = asyncio.get_running_loop()
    players = await loop.run_in_executor(None, fetch_all_players)
    player = next((p for p in players if p["name"].lower() == mc_name.lower()), None)
    if not player:
        await interaction.followup.send(f"Player `{mc_name}` not found on the server.", ephemeral=True)
        return

    current = get_rank_for_hours(player["playtime_hours"])
    current_idx = next((i for i, r in enumerate(FREE_RANKS) if r["rank"] == current["rank"]), 0)
    next_rank = FREE_RANKS[current_idx + 1] if current_idx + 1 < len(FREE_RANKS) else None

    if next_rank:
        progress = player["playtime_hours"] - current["hours"]
        needed   = next_rank["hours"] - current["hours"]
        pct = min(int((progress / needed) * 20), 20)
        bar = "█" * pct + "░" * (20 - pct)
        next_info = f"\n\n**Next rank:** {next_rank['label']} ({next_rank['hours']}h)\n`{bar}` {round(progress,1)}/{needed}h"
    else:
        next_info = "\n\nMaximum rank reached!"

    pts          = db_points().get(player["uuid"], 0)
    daily_info   = db_daily().get(discord_id, {})
    streak       = daily_info.get("streak", 0)

    embed = discord.Embed(title=f"{interaction.user.display_name}'s Rank", color=current["color"])
    embed.add_field(name="Minecraft",   value=f"`{mc_name}`",            inline=True)
    embed.add_field(name="Rank",        value=f"**{current['label']}**", inline=True)
    embed.add_field(name="Playtime",    value=f"{player['playtime_hours']}h", inline=True)
    embed.add_field(name="Quests",      value=str(player["quests"]),     inline=True)
    embed.add_field(name="Points",      value=f"**{pts} pts**",          inline=True)
    embed.add_field(name="Streak",      value=f"**{streak} days**",      inline=True)
    embed.add_field(name="Progression", value=next_info,                 inline=False)
    embed.set_thumbnail(url=f"https://minotar.net/avatar/{mc_name}/64")
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="stats", description="View your detailed in-game statistics", guild=discord.Object(id=GUILD_ID))
async def stats_command(interaction: discord.Interaction):
    if not _bot_channel_only(interaction):
        await interaction.response.send_message(f"Please use this command in <#{CHANNEL_BOT_COMMANDS}>.", ephemeral=True)
        return
    if is_rate_limited(interaction.user.id):
        await interaction.response.send_message("Too fast! Wait a few seconds.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    discord_id = str(interaction.user.id)
    mc_name = db_linked().get(discord_id)
    if not mc_name:
        await interaction.followup.send("Link your account first with `/link <username>`.", ephemeral=True)
        return

    loop = asyncio.get_running_loop()
    players = await loop.run_in_executor(None, fetch_all_players)
    player = next((p for p in players if p["name"].lower() == mc_name.lower()), None)
    if not player:
        await interaction.followup.send(f"Player `{mc_name}` not found.", ephemeral=True)
        return

    def ranking(key):
        sp  = sorted(players, key=lambda x: x.get(key, 0), reverse=True)
        pos = next((i + 1 for i, p in enumerate(sp) if p["uuid"] == player["uuid"]), "?")
        return f"#{pos}/{len(players)}"

    km = round(player["walked_cm"] / 100000, 1)
    embed = discord.Embed(title=f"{mc_name}'s Statistics", color=0x00B4D8, timestamp=datetime.datetime.utcnow())
    embed.add_field(name="Playtime",       value=f"**{player['playtime_hours']}h** {ranking('playtime_hours')}", inline=True)
    embed.add_field(name="Quests",         value=f"**{player['quests']}** {ranking('quests')}",                 inline=True)
    embed.add_field(name="Deaths",         value=f"**{player['deaths']:,}**",                                   inline=True)
    embed.add_field(name="Blocks Mined",   value=f"**{player['mined']:,}** {ranking('mined')}",                 inline=True)
    embed.add_field(name="Distance",       value=f"**{km:,} km** {ranking('walked_cm')}",                       inline=True)
    embed.add_field(name="Items Crafted",  value=f"**{player['crafted']:,}** {ranking('crafted')}",             inline=True)
    embed.add_field(name="Mobs Killed",    value=f"**{player['mob_kills']:,}** {ranking('mob_kills')}",         inline=True)
    embed.set_thumbnail(url=f"https://minotar.net/avatar/{mc_name}/64")
    embed.set_footer(text="Rankings shown as #position/total - HubUniverse")
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="top", description="Show live leaderboards on demand", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(category="Which leaderboard to display")
@app_commands.choices(category=[
    app_commands.Choice(name="Quests",       value="quests"),
    app_commands.Choice(name="Playtime",     value="playtime_hours"),
    app_commands.Choice(name="Blocks Mined", value="mined"),
    app_commands.Choice(name="Mobs Killed",  value="mob_kills"),
    app_commands.Choice(name="Points",       value="points"),
])
async def top_command(interaction: discord.Interaction, category: str = "quests"):
    if not _bot_channel_only(interaction):
        await interaction.response.send_message(f"Please use this command in <#{CHANNEL_BOT_COMMANDS}>.", ephemeral=True)
        return
    if is_rate_limited(interaction.user.id):
        await interaction.response.send_message("Too fast! Wait a moment.", ephemeral=True)
        return

    await interaction.response.defer()
    loop = asyncio.get_running_loop()
    players = await loop.run_in_executor(None, fetch_all_players)

    labels = {"quests": "Quest", "playtime_hours": "Playtime", "mined": "Mining", "mob_kills": "PvE", "points": "Points"}
    units  = {"quests": "quests", "playtime_hours": "h", "mined": "blocks", "mob_kills": "kills", "points": "pts"}
    medals = ["1st", "2nd", "3rd"]

    if category == "points":
        combined = [{**p, "points": db_points().get(p["uuid"], 0)} for p in players]
        sorted_p = sorted(combined, key=lambda x: x["points"], reverse=True)[:10]
    else:
        sorted_p = sorted(players, key=lambda x: x.get(category, 0), reverse=True)[:10]

    lines = [f"**{medals[i] if i < 3 else str(i+1)+'.'} {p['name']}** - {p.get(category,0):,} {units[category]}" for i, p in enumerate(sorted_p)]
    embed = discord.Embed(title=f"{labels[category]} Top 10 - HubUniverse", description="\n".join(lines) or "No data.", color=0x00B4D8, timestamp=datetime.datetime.utcnow())
    embed.set_footer(text="Use /stats for personal stats - HubUniverse")
    await interaction.followup.send(embed=embed)


@tree.command(name="link", description="Link your Minecraft username to your Discord account", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(username="Your Minecraft username")
async def link_command(interaction: discord.Interaction, username: str):
    if not _bot_channel_only(interaction):
        await interaction.response.send_message("Please use this command in #bot-commands.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    # Validate username exists on the server
    loop = asyncio.get_running_loop()
    players = await loop.run_in_executor(None, fetch_all_players)
    if players and not any(p["name"].lower() == username.lower() for p in players):
        await interaction.followup.send(
            f"Username `{username}` was not found on the server.\n"
            "Make sure you've joined at least once, or check for typos.",
            ephemeral=True
        )
        return

    discord_id = str(interaction.user.id)
    db_linked()[discord_id] = username
    _db_save()

    await interaction.followup.send(f"Your Discord account is now linked to **{username}**!\nUse `/rank` to check your progression.", ephemeral=True)
    await log_action(
        title="Account Linked",
        description=f"**{interaction.user.display_name}** linked to `{username}`",
        color=0x3498DB,
        thumbnail_url=f"https://minotar.net/avatar/{username}/64",
    )
    print(f"[LINK] {interaction.user.display_name} -> {username}")


@tree.command(name="unlink", description="Unlink your Minecraft account from Discord", guild=discord.Object(id=GUILD_ID))
async def unlink_command(interaction: discord.Interaction):
    discord_id = str(interaction.user.id)
    if discord_id not in db_linked():
        await interaction.response.send_message("You don't have a linked account.", ephemeral=True)
        return
    mc_name = db_linked().pop(discord_id)
    _db_save()
    await interaction.response.send_message(f"Unlinked from **{mc_name}**.", ephemeral=True)
    print(f"[UNLINK] {interaction.user.display_name} unlinked from {mc_name}")


@tree.command(name="daily", description="Claim your daily reward of points!", guild=discord.Object(id=GUILD_ID))
async def daily_command(interaction: discord.Interaction):
    if not _bot_channel_only(interaction):
        await interaction.response.send_message(f"Please use this command in <#{CHANNEL_BOT_COMMANDS}>.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    discord_id = str(interaction.user.id)
    mc_name = db_linked().get(discord_id)
    if not mc_name:
        await interaction.followup.send("Link your account first with `/link <username>`.", ephemeral=True)
        return

    now        = datetime.datetime.utcnow()
    daily_info = db_daily().get(discord_id, {"last": None, "streak": 0})
    last_str   = daily_info.get("last")
    streak     = daily_info.get("streak", 0)

    if last_str:
        last_dt = datetime.datetime.fromisoformat(last_str)
        delta   = now - last_dt
        if delta < datetime.timedelta(hours=24):
            remaining = datetime.timedelta(hours=24) - delta
            h, rem = divmod(int(remaining.total_seconds()), 3600)
            m = rem // 60
            await interaction.followup.send(f"Come back in **{h}h {m}m** for your next reward!\nCurrent streak: **{streak} days**", ephemeral=True)
            return
        if delta > datetime.timedelta(hours=48):
            streak = 0  # Streak broken

    streak = min(streak + 1, DAILY_STREAK_MAX)
    bonus  = (streak - 1) * DAILY_STREAK_BONUS
    reward = DAILY_REWARD_POINTS + bonus

    loop = asyncio.get_running_loop()
    players = await loop.run_in_executor(None, fetch_all_players)
    player = next((p for p in players if p["name"].lower() == mc_name.lower()), None)
    if not player:
        await interaction.followup.send(f"Player `{mc_name}` not found.", ephemeral=True)
        return

    db_points()[player["uuid"]] = db_points().get(player["uuid"], 0) + reward
    db_daily()[discord_id] = {"last": now.isoformat(), "streak": streak}
    _db_save()

    total_pts = db_points()[player["uuid"]]
    embed = discord.Embed(title="Daily Reward Claimed!", color=0x2ECC71, timestamp=now)
    embed.add_field(name="Reward",       value=f"**+{reward} pts**",  inline=True)
    embed.add_field(name="Total Points", value=f"**{total_pts} pts**", inline=True)
    embed.add_field(name="Streak",       value=f"**{streak} days** {'(+' + str(bonus) + ' bonus!)' if bonus else ''}", inline=False)
    if streak < DAILY_STREAK_MAX:
        embed.add_field(name="Tomorrow", value=f"Come back for **+{DAILY_REWARD_POINTS + streak*DAILY_STREAK_BONUS} pts**!", inline=False)
    else:
        embed.add_field(name="Max Streak!", value="Maximum streak bonus reached!", inline=False)
    embed.set_footer(text="Use /shop to spend your points!")
    embed.set_thumbnail(url=f"https://minotar.net/avatar/{mc_name}/64")
    await interaction.followup.send(embed=embed, ephemeral=True)

    await log_action(
        title="Daily Reward Claimed",
        description=f"**{mc_name}** claimed their daily reward.",
        color=0x2ECC71,
        thumbnail_url=f"https://minotar.net/avatar/{mc_name}/64",
        fields=[("Reward", f"+{reward} pts"), ("Streak", f"{streak} days"), ("Total", f"{total_pts} pts")],
    )
    print(f"[DAILY] {mc_name}: +{reward} pts (streak {streak}) -> {total_pts} total")


@tree.command(name="event", description="Check the current community event status", guild=discord.Object(id=GUILD_ID))
async def event_command(interaction: discord.Interaction):
    if not current_event:
        embed = discord.Embed(title="No Active Event", description=f"No event is running right now.\nNext events at **08:00, 14:00, 19:00 and 00:00 UTC**.", color=0x95A5A6)
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return
    total = sum(d["progress"] for d in event_progress.values())
    pct   = min(int((total / current_event["goal"]) * 20), 20)
    bar   = "█" * pct + "░" * (20 - pct)
    pct_n = round((total / current_event["goal"]) * 100) if current_event["goal"] > 0 else 0
    end_ts = int(current_event["end"].timestamp())
    embed = discord.Embed(title=f"{current_event['emoji']} {current_event['name']} - In Progress", description=current_event["description"], color=0xF1C40F, timestamp=datetime.datetime.utcnow())
    embed.add_field(name="Progress", value=f"`{bar}` **{total}/{current_event['goal']}** {current_event['unit']} ({pct_n}%)", inline=False)
    embed.add_field(name="Ends",     value=f"<t:{end_ts}:R>", inline=True)
    embed.add_field(name="Points",   value=f"**{current_event['points_per_unit']} pt** per {current_event['unit'].split()[-1]}", inline=True)
    embed.set_footer(text="Use /rank to check your points - HubUniverse")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@tree.command(name="map", description="Get the link to the live server map", guild=discord.Object(id=GUILD_ID))
async def map_command(interaction: discord.Interaction):
    map_url = os.getenv("BLUEMAP_URL", "")
    if not map_url:
        await interaction.response.send_message("The live map is not configured yet. Stay tuned!", ephemeral=True)
        return
    embed = discord.Embed(title="HubUniverse - Live Map", description=f"[Click here to open the map]({map_url})", color=0x00B4D8)
    embed.set_footer(text="Powered by BlueMap - HubUniverse")
    await interaction.response.send_message(embed=embed)

# ══════════════════════════════════════════════
# SHOP
# ══════════════════════════════════════════════
SHOP_ITEMS = [
    {"id": "chunks", "emoji": "Pick", "name": "+2 Force-loaded Chunks", "cost": 50,  "description": "Permanently adds 2 force-loaded chunk slots to your account."},
    {"id": "homes",  "emoji": "Home", "name": "+5 Homes",                "cost": 30,  "description": "Permanently adds 5 home slots to your account."},
    {"id": "star",   "emoji": "Star", "name": "EventStar Prefix",         "cost": 100, "description": "Adds the EventStar prefix to your in-game name."},
]


class ShopView(discord.ui.View):
    def __init__(self, mc_name: str, uuid: str):
        super().__init__(timeout=60)
        self.mc_name = mc_name
        self.uuid    = uuid
        for item in SHOP_ITEMS:
            btn = discord.ui.Button(label=f"{item['name']} - {item['cost']} pts", style=discord.ButtonStyle.primary, custom_id=f"shop_{item['id']}")
            btn.callback = self.make_callback(item)
            self.add_item(btn)

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True

    def make_callback(self, item):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=True)
            pts = db_points().get(self.uuid, 0)
            if pts < item["cost"]:
                await interaction.followup.send(f"Not enough points! You have **{pts} pts** but need **{item['cost']} pts**.", ephemeral=True)
                return

            try:
                if item["id"] == "chunks":
                    pk       = f"{self.uuid}_chunks"
                    purchases = db_shop().get(pk, 0)
                    base     = next(r["force"] for r in FREE_RANKS if r["rank"] == player_ranks.get(self.uuid, "member"))
                    await rcon_async(f"ftbranks node add {self.mc_name} ftbchunks.max_force_loaded {base + (purchases+1)*2}")
                    db_shop()[pk] = purchases + 1
                elif item["id"] == "homes":
                    pk       = f"{self.uuid}_homes"
                    purchases = db_shop().get(pk, 0)
                    base     = next(r["homes"] for r in FREE_RANKS if r["rank"] == player_ranks.get(self.uuid, "member"))
                    await rcon_async(f"ftbranks node add {self.mc_name} ftbessentials.home.max {base + (purchases+1)*5}")
                    db_shop()[pk] = purchases + 1
                elif item["id"] == "star":
                    await rcon_async(f"ftbranks node add {self.mc_name} ftbranks.name_format <Star {{name}}>")
            except Exception as e:
                print(f"[SHOP] RCON error for {self.mc_name}: {e}")
                await log_action(
                    title="RCON Error - Shop Purchase Failed",
                    description=f"**{self.mc_name}** tried to buy **{item['name']}** but RCON failed. Points NOT deducted.",
                    error=True,
                    fields=[("Item", item["name"]), ("Cost", f"{item['cost']} pts"), ("Error", str(e))],
                )
                await interaction.followup.send("An error occurred. Points were **not** deducted. Please try again.", ephemeral=True)
                return

            db_points()[self.uuid] = pts - item["cost"]
            _db_save()
            remaining = db_points()[self.uuid]
            await interaction.followup.send(f"**{item['name']}** purchased! Applied in-game.\nYou now have **{remaining} pts** remaining.", ephemeral=True)
            await log_action(
                title="Shop Purchase",
                description=f"**{self.mc_name}** bought **{item['name']}**",
                color=0xF1C40F,
                thumbnail_url=f"https://minotar.net/avatar/{self.mc_name}/64",
                fields=[("Cost", f"{item['cost']} pts"), ("Remaining", f"{remaining} pts")],
            )
            print(f"[SHOP] {self.mc_name} bought {item['id']} for {item['cost']} pts")
        return callback


@tree.command(name="shop", description="Spend your event points on rewards", guild=discord.Object(id=GUILD_ID))
async def shop_command(interaction: discord.Interaction):
    if not _bot_channel_only(interaction):
        await interaction.response.send_message(f"Please use this command in <#{CHANNEL_BOT_COMMANDS}>.", ephemeral=True)
        return
    if is_rate_limited(interaction.user.id):
        await interaction.response.send_message("Too fast! Wait a moment.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    discord_id = str(interaction.user.id)
    mc_name = db_linked().get(discord_id)
    if not mc_name:
        await interaction.followup.send(f"Link your account first with `/link <username>` in <#{CHANNEL_BOT_COMMANDS}>.", ephemeral=True)
        return

    loop = asyncio.get_running_loop()
    players = await loop.run_in_executor(None, fetch_all_players)
    player = next((p for p in players if p["name"].lower() == mc_name.lower()), None)
    if not player:
        await interaction.followup.send(f"Player `{mc_name}` not found.", ephemeral=True)
        return

    pts = db_points().get(player["uuid"], 0)
    embed = discord.Embed(title="HubUniverse - Event Shop", description=f"You have **{pts} pts** to spend!\nEarn points with events or `/daily`.", color=0xF1C40F)
    for item in SHOP_ITEMS:
        embed.add_field(name=f"{item['name']} - {item['cost']} pts", value=item["description"], inline=False)
    embed.set_footer(text="Rewards are applied instantly in-game!")
    await interaction.followup.send(embed=embed, view=ShopView(mc_name, player["uuid"]), ephemeral=True)

# ══════════════════════════════════════════════
# TASKS
# ══════════════════════════════════════════════
@tasks.loop(minutes=10)
async def refresh_ftp_cache():
    """Dedicated task to warm the FTP cache independently."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: fetch_all_players(force=True))


@tasks.loop(minutes=60)
async def update_dashboard():
    print(f"[INFO] Dashboard update ({datetime.datetime.utcnow().strftime('%H:%M')})...")
    try:
        players = fetch_all_players()
        tps     = await get_server_tps()

        ch = bot.get_channel(CHANNEL_DASHBOARD)
        if ch:
            embed = build_dashboard_embed(players, tps=tps)
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

        ch_notif = bot.get_channel(CHANNEL_NOTIFICATIONS)
        if ch_notif:
            milestones = db_milestones()
            for p in players:
                for milestone in QUEST_MILESTONES:
                    if p["quests"] >= milestone:
                        key = f"{p['uuid']}_{milestone}"
                        if key not in milestones:
                            db_add_milestone(key)
                            embed_n = discord.Embed(title="Quest Milestone Reached!", description=f"**{p['name']}** completed **{milestone} quests**!", color=0x00B4D8)
                            embed_n.set_thumbnail(url=f"https://minotar.net/avatar/{p['name']}/64")
                            await ch_notif.send(embed=embed_n)
            _db_save()

    except Exception as e:
        print(f"[ERROR] Dashboard update failed: {e}")


@tasks.loop(minutes=30)
async def check_ranks():
    print("[INFO] Checking ranks...")
    try:
        players  = fetch_all_players()
        ch_notif = bot.get_channel(CHANNEL_NOTIFICATIONS)
        for p in players:
            rank    = get_rank_for_hours(p["playtime_hours"])
            current = player_ranks.get(p["uuid"])
            if current != rank["rank"]:
                await apply_rank(p["name"], rank, current)
                player_ranks[p["uuid"]] = rank["rank"]
                print(f"[RANK] {p['name']} -> {rank['rank']} ({p['playtime_hours']}h)")

                if current is not None and ch_notif:
                    discord_id = next((did for did, mc in db_linked().items() if mc.lower() == p["name"].lower()), None)
                    mention    = f"<@{discord_id}>" if discord_id else f"**{p['name']}**"
                    embed = discord.Embed(title="Rank Up!", description=f"Congratulations to {mention} who just reached the **{rank['label']}** rank!", color=rank["color"], timestamp=datetime.datetime.utcnow())
                    embed.set_thumbnail(url=f"https://minotar.net/avatar/{p['name']}/64")
                    embed.add_field(name="Playtime", value=f"{p['playtime_hours']}h", inline=True)
                    embed.set_footer(text="Keep playing to unlock more perks! - HubUniverse")
                    await ch_notif.send(embed=embed)
                    await log_action(
                        title="Rank Up (Automatic)",
                        description=f"**{p['name']}** promoted to **{rank['label']}**",
                        color=rank["color"],
                        thumbnail_url=f"https://minotar.net/avatar/{p['name']}/64",
                        fields=[("Previous", current or "none"), ("New", rank["label"]), ("Playtime", f"{p['playtime_hours']}h")],
                    )

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
        ch_m = guild.get_channel(VOICE_MEMBERS)
        if ch_m:
            await ch_m.edit(name=f"Members: {guild.member_count}")
        response = await rcon_async("list")
        online = 0
        if response:
            m = re.search(r"(\d+) of a max", response)
            if m:
                online = int(m.group(1))
        ch_o = guild.get_channel(VOICE_PLAYERS_ONLINE)
        if ch_o:
            await ch_o.edit(name=f"Online: {online} Players")
        print("[OK] Voice stats updated.")
    except Exception as e:
        print(f"[ERROR] Voice stats: {e}")


@tasks.loop(minutes=60)
async def check_weekly_recap():
    now = datetime.datetime.utcnow()
    if now.weekday() != 6 or now.hour != 20:
        return
    try:
        players = fetch_all_players()
        if not players:
            return
        best_q = max(players, key=lambda x: x["quests"])
        best_t = max(players, key=lambda x: x["playtime_hours"])
        embed = discord.Embed(title="Weekly Recap", description="Here's what happened this week on HubUniverse!", color=0x00B4D8, timestamp=datetime.datetime.utcnow())
        embed.add_field(name="Best Progression",  value=f"**{best_q['name']}** - {best_q['quests']} quests", inline=False)
        embed.add_field(name="Most Active Player", value=f"**{best_t['name']}** - {best_t['playtime_hours']}h played", inline=False)
        embed.set_footer(text="See you next week! - HubUniverse")
        ch = bot.get_channel(CHANNEL_WEEKLY_RECAP)
        if ch:
            await ch.send(embed=embed)
            print("[OK] Weekly recap sent.")
    except Exception as e:
        print(f"[ERROR] Weekly recap: {e}")

# ══════════════════════════════════════════════
# CHANNEL SETUP HELPERS
# ══════════════════════════════════════════════
async def _upsert_embed(channel_id: int, embed: discord.Embed, view=None) -> None:
    ch = bot.get_channel(channel_id)
    if not ch:
        return
    found = False
    async for msg in ch.history(limit=10):
        if msg.author == bot.user and not msg.is_system():
            await msg.edit(embed=embed, view=view)
            found = True
            break
    if not found:
        msg = await ch.send(embed=embed, view=view)
        await msg.pin()


async def setup_rules_channel() -> None:
    embed = discord.Embed(title="HubUniverse - Server Rules", description="Please read and accept the rules to access the server.", color=0x00B4D8)
    embed.add_field(name="1. Respect",         value="No harassment, discrimination, or toxic behavior.", inline=False)
    embed.add_field(name="2. No Griefing",     value="Do not destroy or steal other players' builds.", inline=False)
    embed.add_field(name="3. No Cheating",     value="No hacks, exploits, or unauthorized mods.", inline=False)
    embed.add_field(name="4. Help Each Other", value="Share knowledge, help newcomers!", inline=False)
    embed.add_field(name="5. English Only",    value="Please communicate in English in public channels.", inline=False)
    embed.set_footer(text="Click the button below to accept the rules.")
    await _upsert_embed(CHANNEL_RULES, embed, view=RulesView())
    print("[OK] #rules updated.")


async def setup_server_info_channel() -> None:
    embed = discord.Embed(title="HubUniverse - Server Info", description="Everything you need to get started!", color=0x00B4D8)
    embed.add_field(name="Server IP",  value=f"`{os.getenv('MC_HOST', 'coming soon')}`", inline=True)
    embed.add_field(name="Modpack",    value="[All The Mods 10](https://www.curseforge.com/minecraft/modpacks/all-the-mods-10)", inline=True)
    embed.add_field(name="Quests",     value="4,536 tasks to complete!", inline=True)
    embed.add_field(name="Ranks",      value=f"Check <#{CHANNEL_RANKS}> for rank progression", inline=True)
    embed.add_field(name="Premium",    value="Support us on Patreon for exclusive perks!", inline=True)
    embed.add_field(name="Discord <> Minecraft", value=f"Chat linked to <#{CHANNEL_INGAME_CHAT}>", inline=True)
    embed.set_footer(text="HubUniverse - The center of your Minecraft universe.")
    await _upsert_embed(CHANNEL_SERVER_INFO, embed)
    print("[OK] #server-info updated.")


async def setup_ranks_channel() -> None:
    embed = discord.Embed(title="HubUniverse - Rank System", description="Progress through ranks by playing on the server!", color=0x00B4D8)
    lines = ""
    for r in FREE_RANKS:
        homes = "unlimited" if r["homes"] == 999 else str(r["homes"])
        lines += f"**{r['label']}** - {r['hours']}h+ | {r['chunks']} chunks | {r['force']} force-loaded | {homes} homes\n"
    embed.add_field(name="Free Ranks (by playtime)", value=lines, inline=False)
    embed.set_footer(text="Use /rank and /shop in #bot-commands - Link your account first with /link")
    await _upsert_embed(CHANNEL_RANKS, embed)
    print("[OK] #ranks updated.")


async def setup_event_info_channel() -> None:
    ch = bot.get_channel(CHANNEL_EVENT_INFO)
    if not ch:
        return
    embed1 = discord.Embed(title="Step 1 - Link your Minecraft account", description=f"Go to <#{CHANNEL_BOT_COMMANDS}> and type:\n```/link <your_minecraft_username>```\nYou only need to do this **once**!", color=0x3498DB)
    embed2 = discord.Embed(
        title="Community Events - How it works",
        description=(
            "**4 events per day** at **8:00, 14:00, 19:00 and 00:00 UTC** - each lasts **1 hour**.\n\n"
            "Event types: Mining Frenzy / Monster Hunt / Crafting Rush\n\n"
            "Points: 1st 50pts / 2nd 30pts / 3rd 15pts / Participation 5pts\n\n"
            f"Check current event with `/event` - Results in <#{CHANNEL_EVENTS}>"
        ),
        color=0xF1C40F
    )
    embed3 = discord.Embed(
        title="Event Shop + Daily Reward",
        description=(
            f"Spend points in <#{CHANNEL_BOT_COMMANDS}> with `/shop`:\n\n"
            "+2 Force-loaded Chunks - 50pts\n"
            "+5 Homes - 30pts\n"
            "EventStar Prefix - 100pts\n\n"
            f"Use `/daily` every 24h for **{DAILY_REWARD_POINTS} free pts**!\n"
            f"Build a streak for bonus points (up to +{DAILY_STREAK_MAX * DAILY_STREAK_BONUS} pts/day)!\n\n"
            f"Check your stats with `/rank`, `/stats`, `/top` in <#{CHANNEL_BOT_COMMANDS}>."
        ),
        color=0x2ECC71
    )
    embed3.set_footer(text="HubUniverse - ATM10")
    await ch.purge(limit=10, check=lambda m: m.author == bot.user)
    await ch.send(embed=embed1)
    await ch.send(embed=embed2)
    m3 = await ch.send(embed=embed3)
    await m3.pin()
    print("[OK] #event-info updated.")

# ══════════════════════════════════════════════
# DISCORD EVENTS
# ══════════════════════════════════════════════
@bot.event
async def on_member_join(member: discord.Member):
    ch = bot.get_channel(CHANNEL_ANNOUNCEMENTS)
    if not ch:
        return
    embed = discord.Embed(
        title=f"Welcome to HubUniverse, {member.display_name}!",
        description=f"Hey {member.mention}!\nHead over to <#{CHANNEL_RULES}> to accept the rules.\nThen check <#{CHANNEL_SERVER_INFO}> to get started!",
        color=0x00B4D8,
        timestamp=datetime.datetime.utcnow()
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="Quests", value=f"<#{CHANNEL_SERVER_INFO}>", inline=True)
    embed.add_field(name="Ranks",  value=f"<#{CHANNEL_RANKS}>",       inline=True)
    embed.add_field(name="Chat",   value=f"<#{CHANNEL_GENERAL}>",     inline=True)
    embed.set_footer(text=f"Member #{member.guild.member_count} - HubUniverse")
    await ch.send(embed=embed)


@bot.event
async def on_ready():
    print(f"[OK] Bot connected: {bot.user}")
    bot.add_view(RulesView())
    _db_load()

    await tree.sync(guild=discord.Object(id=GUILD_ID))
    print("[OK] Slash commands synced.")
    await asyncio.sleep(3)

    await setup_rules_channel()
    await setup_server_info_channel()
    await setup_ranks_channel()
    await setup_event_info_channel()

    for task in (refresh_ftp_cache, update_dashboard, check_ranks, update_voice_stats, check_weekly_recap, check_milestone_events):
        if not task.is_running():
            task.start()

    print("[OK] All tasks started.")


bot.run(TOKEN)
