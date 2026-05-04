#!/usr/bin/env python3
"""
Padel Match & Tournament Monitor Bot (@Findmypadelmatchbot)
Platforms: Playtomic, MATCHi, play.fi
Features:
  - Interactive wizard with inline buttons
  - Multi-location monitoring (Lahti, Limassol, custom)
  - Time spinners 0:00–23:30 (±1h / ±30m) — filters by EVENT START TIME
  - Filters: level, date range, time range, min players
  - Only new events in continuous monitoring
  - Deep links to Playtomic app, MATCHi, play.fi
"""

import os, json, logging, asyncio, re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import urllib.request, urllib.error

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, MenuButtonCommands
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
    MessageHandler, filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    print("ERROR: TELEGRAM_BOT_TOKEN environment variable is not set!")
    print("Set it in Railway: Variables tab -> New Variable")
    exit(1)
# Use Railway Volume mount path if available, fallback to local
DATA_DIR = os.environ.get("DATA_DIR", "/data" if os.path.isdir("/data") else ".")
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    DATA_DIR = "."
SETTINGS_FILE = os.path.join(DATA_DIR, "user_settings.json")
log.info(f"Settings file: {SETTINGS_FILE}")

# ─── Location presets ───────────────────────────────────────────────
LOCATIONS = {
    "Lahti":    {"lat": 61.0054, "lon": 25.4834, "tz": "Europe/Helsinki"},
    "Limassol": {"lat": 34.6841, "lon": 33.0379, "tz": "Asia/Nicosia"},
}

# MATCHi facility slugs per location (only those with activities AND within typical radius)
MATCHI_FACILITIES = {
    "Lahti": [
        "padelmarina",        # Sisäpelikeskus PadelMarina, Hollola
    ],
    "Limassol": [],   # No MATCHi facilities with activities in Cyprus
    "Helsinki": [     # Available if user adds Helsinki as a location
        "opmyllypuro",        # Open Padel Myllypuro
        "opkaivoksela",       # Open Padel & Golf Kaivoksela
        "opmartinlaakso",     # Open Padel Martinlaakso
        "bergenwalltennis",   # Bergenwall Tennis (27 activities)
        "actionpadelnacka",   # Actionpadel Nacka
    ],
}

# ─── Persistence ────────────────────────────────────────────────────
def load_all_settings():
    try:
        with open(SETTINGS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def save_all_settings(data):
    with open(SETTINGS_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_user(uid: int) -> dict:
    return load_all_settings().get(str(uid), {})

def set_user(uid: int, cfg: dict):
    data = load_all_settings()
    data[str(uid)] = cfg
    save_all_settings(data)

def get_added(uid: int) -> set:
    return set(get_user(uid).get("calendar_added", []))

def toggle_added(uid: int, mid: str) -> bool:
    """Toggle 'added to calendar' for a match. Returns new state."""
    u = get_user(uid)
    added = set(u.get("calendar_added", []))
    if mid in added:
        added.discard(mid); state = False
    else:
        added.add(mid); state = True
    u["calendar_added"] = list(added)
    set_user(uid, u)
    return state

def mark_added(uid: int, mid: str):
    """Mark match as added (idempotent)."""
    u = get_user(uid)
    added = set(u.get("calendar_added", []))
    if mid not in added:
        added.add(mid)
        u["calendar_added"] = list(added)
        set_user(uid, u)

# ─── HTTP helpers ───────────────────────────────────────────────────
def api_get(url: str, timeout: int = 20):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; PadelBot/2.0)",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning("API error %s: %s", url[:100], e)
        return None

# ─── Playtomic API (verified endpoints) ────────────────────────────
BASE = "https://api.playtomic.io/v1"

def playtomic_clubs(lat, lon, radius_m=50000):
    """Search clubs near coordinates. Returns list of tenant dicts."""
    url = f"{BASE}/tenants?sport_id=PADEL&coordinate={lat},{lon}&radius={radius_m}&size=50"
    return api_get(url) or []

def playtomic_matches_by_tenants(tenant_ids: list, date_from=None, max_pages=8):
    """Get matches for given tenant IDs. Smart pagination: only fetches more pages
    for clubs where page 0 doesn't cover the target date range."""
    if not tenant_ids:
        return []
    target_start = date_from or "2026-04-01"
    all_matches = []
    for tid in tenant_ids:
        for page in range(max_pages):
            url = f"{BASE}/matches?sport_id=PADEL&tenant_id={tid}&page={page}&size=100"
            data = api_get(url)
            if not isinstance(data, list) or not data:
                break
            all_matches.extend(data)
            # API returns desc by date. Stop if oldest match on this page
            # is before our target date range.
            oldest = min(m.get("start_date", "9999")[:10] for m in data)
            if oldest <= target_start:
                break
            # Also stop if we got fewer than 100 (last page)
            if len(data) < 100:
                break
    return all_matches

def playtomic_tournaments(lat, lon, radius_m=50000):
    """Search tournaments near coordinates."""
    url = f"{BASE}/tournaments?sport_id=PADEL&coordinate={lat},{lon}&radius={radius_m}&size=400"
    data = api_get(url)
    return data if isinstance(data, list) else []

# ─── MATCHi parsing ─────────────────────────────────────────────
def matchi_fetch_activities(slug):
    """Fetch activities from a MATCHi facility page. Returns list of event dicts."""
    url = f"https://www.matchi.se/facilities/{slug}?sport=PADEL"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        log.warning("MATCHi error %s: %s", slug, e)
        return []

    events = []
    # Get facility display name
    fac_name_m = re.search(r'<title>([^<|]+)', html)
    fac_name = fac_name_m.group(1).strip().replace('&amp;', '&') if fac_name_m else slug

    # Get venue numeric ID for deep links
    venue_id_m = re.search(r'venue_id["\s:=]+["\']?(\d+)', html)
    venue_id = venue_id_m.group(1) if venue_id_m else None

    # Find activities: <a name="ClassActivity-ID"></a> ... <h4>Name</h4>
    act_positions = [
        (m.start(), m.group(1), m.group(2).strip().replace('&amp;', '&'))
        for m in re.finditer(
            r'<a name="ClassActivity-(\d+)">\s*</a>\s*.*?<h4[^>]*>([^<]+)</h4>',
            html, re.DOTALL
        )
    ]

    # Find all occasions with their positions
    occ_pattern = (
        r'<strong>(\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2})</strong>\s*<br>\s*'
        r'<small>(\d{4}-\d{2}-\d{2})</small>.*?(\d+)/(\d+)'
    )
    occ_positions = [
        (m.start(), m.group(1).strip(), m.group(2), int(m.group(3)), int(m.group(4)))
        for m in re.finditer(occ_pattern, html, re.DOTALL)
    ]

    # Pair each occasion with its parent activity (closest activity before it)
    for occ_pos, time_range, date_str, current, total in occ_positions:
        best_act = None
        for act_pos, act_id, act_name in act_positions:
            if act_pos < occ_pos:
                best_act = (act_id, act_name)
            else:
                break
        if not best_act:
            continue

        # Skip full events
        if total > 0 and current >= total:
            continue

        act_id, act_name = best_act
        start_time = time_range.split("-")[0].strip()
        # Deep link: /venues/{venueId}#ClassActivity-{actId} opens MATCHi app
        if venue_id:
            link = f"https://www.matchi.se/venues/{venue_id}#ClassActivity-{act_id}"
        else:
            link = f"https://www.matchi.se/activities/{act_id}"
        events.append({
            "platform": "matchi",
            "facility": fac_name,
            "activity_name": act_name,
            "start_date": f"{date_str}T{start_time}:00",
            "time_range": time_range,
            "date": date_str,
            "registered_count": current,
            "max_players": total,
            "level_description": "",
            "link": link,
            "activity_id": act_id,
        })

    log.info("MATCHi %s: %d events from %d activities", slug, len(events), len(act_positions))
    return events

def matchi_events_for_location(loc_name):
    """Get all MATCHi events for a location."""
    slugs = MATCHI_FACILITIES.get(loc_name, [])
    all_events = []
    for slug in slugs:
        all_events.extend(matchi_fetch_activities(slug))
    return all_events

# ─── Parsing helpers ────────────────────────────────────────────────
def parse_dt(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt)+3], fmt)
        except (ValueError, IndexError):
            continue
    return None

def match_players(m):
    """Count total players in match across all teams."""
    return sum(len(t.get("players", [])) for t in m.get("teams", []))

def match_max_players(m):
    return sum(t.get("max_players", 0) for t in m.get("teams", []))

def match_level_range(m):
    """Extract min/max level from match players or restrictions."""
    levels = []
    for team in m.get("teams", []):
        for p in team.get("players", []):
            lv = p.get("level_value")
            if lv is not None:
                levels.append(float(lv))
    restriction = m.get("skill_level_restriction") or {}
    rmin = restriction.get("min")
    rmax = restriction.get("max")
    if rmin is not None:
        return float(rmin), float(rmax) if rmax else 10.0
    if levels:
        return min(levels), max(levels)
    return None, None

def tourn_level_range(t):
    """Parse level_description like '0.00 - 7.00'."""
    desc = t.get("level_description", "")
    m = re.match(r"([\d.]+)\s*-\s*([\d.]+)", desc)
    if m:
        return float(m.group(1)), float(m.group(2))
    return None, None

# ─── Filtering ──────────────────────────────────────────────────────
def _to_local(dt, loc_name):
    """Convert UTC dt to local for filtering. Returns naive local datetime."""
    local = utc_to_local(dt, loc_name)
    return local.replace(tzinfo=None) if local and hasattr(local, 'replace') else dt

def filter_matches(matches, cfg, loc_dates):
    result = []
    date_from = loc_dates.get("from")
    date_to = loc_dates.get("to")
    time_from = cfg.get("time_from")
    time_to = cfg.get("time_to")
    level_min = cfg.get("level_min")
    level_max = cfg.get("level_max")
    min_pl = cfg.get("min_players_match", 0)
    loc_name = ""

    for m in matches:
        loc_name = m.get("_location", loc_name)
        # Skip non-open
        if m.get("status") in ("CANCELED", "FINISHED", "CONFIRMED"):
            continue

        # Skip private matches (link-only, not searchable on Playtomic).
        # All public matches have visibility=VISIBLE — include everything visible.
        if m.get("visibility") and m.get("visibility") != "VISIBLE":
            continue

        dt = parse_dt(m.get("start_date"))
        local_dt = _to_local(dt, loc_name) if dt else None

        # Date filter (using LOCAL time)
        if local_dt and date_from:
            try:
                if local_dt.date() < datetime.strptime(date_from, "%Y-%m-%d").date():
                    continue
            except: pass
        if local_dt and date_to:
            try:
                if local_dt.date() > datetime.strptime(date_to, "%Y-%m-%d").date():
                    continue
            except: pass

        # ★ Time filter — event START TIME in LOCAL timezone
        if local_dt and time_from:
            try:
                hh, mm = map(int, time_from.split(":"))
                event_mins = local_dt.hour * 60 + local_dt.minute
                filter_mins = hh * 60 + mm
                if event_mins < filter_mins:
                    continue
            except: pass
        if local_dt and time_to:
            try:
                hh, mm = map(int, time_to.split(":"))
                event_mins = local_dt.hour * 60 + local_dt.minute
                filter_mins = hh * 60 + mm
                if event_mins > filter_mins:
                    continue
            except: pass

        # Level filter — check by average player level
        if level_min is not None or level_max is not None:
            restriction = m.get("skill_level_restriction") or {}
            r_min = restriction.get("min")
            r_max = restriction.get("max")
            if r_min is not None:
                # Match has explicit level restriction
                try:
                    if level_min is not None and float(level_min) > float(r_max or 10):
                        continue
                    if level_max is not None and float(level_max) < float(r_min):
                        continue
                except: pass
            else:
                # No restriction — check actual player levels
                player_levels = [float(p.get("level_value")) for team in m.get("teams", [])
                                 for p in team.get("players", []) if p.get("level_value") is not None]
                if player_levels:
                    # Skip if ANY player is below user's min level
                    if level_min is not None and min(player_levels) < level_min:
                        continue
                    if level_max is not None and max(player_levels) > level_max + 0.5:
                        continue

        # Min players
        if match_players(m) < min_pl:
            continue

        # Hide full matches
        cur = match_players(m)
        max_p = match_max_players(m)
        if max_p > 0 and cur >= max_p:
            continue

        result.append(m)
    return result

def filter_tournaments(tournaments, cfg, loc_dates):
    result = []
    date_from = loc_dates.get("from")
    date_to = loc_dates.get("to")
    time_from = cfg.get("time_from")
    time_to = cfg.get("time_to")
    level_min = cfg.get("level_min")
    level_max = cfg.get("level_max")
    min_pl = cfg.get("min_players_tourn", 0)
    loc_name = ""

    for t in tournaments:
        loc_name = t.get("_location", loc_name)
        if t.get("is_cancelled"):
            continue
        if t.get("tournament_status") not in (None, "REGISTRATION_OPEN", "OPEN", "PENDING"):
            continue

        dt = parse_dt(t.get("start_date"))
        local_dt = _to_local(dt, loc_name) if dt else None

        # Date filter (LOCAL time)
        if local_dt and date_from:
            try:
                if local_dt.date() < datetime.strptime(date_from, "%Y-%m-%d").date():
                    continue
            except: pass
        if local_dt and date_to:
            try:
                if local_dt.date() > datetime.strptime(date_to, "%Y-%m-%d").date():
                    continue
            except: pass

        # ★ Time filter — LOCAL time
        if local_dt and time_from:
            try:
                hh, mm = map(int, time_from.split(":"))
                event_mins = local_dt.hour * 60 + local_dt.minute
                filter_mins = hh * 60 + mm
                if event_mins < filter_mins:
                    continue
            except: pass
        if local_dt and time_to:
            try:
                hh, mm = map(int, time_to.split(":"))
                event_mins = local_dt.hour * 60 + local_dt.minute
                filter_mins = hh * 60 + mm
                if event_mins > filter_mins:
                    continue
            except: pass

        # Level filter — user level must fit in tournament range
        if level_min is not None or level_max is not None:
            t_min, t_max = tourn_level_range(t)
            if t_min is not None and t_max is not None:
                user_lvl = level_min if level_min is not None else 0
                if user_lvl < t_min or user_lvl > t_max:
                    # Also check if level_max fits
                    user_lvl2 = level_max if level_max is not None else 10
                    if user_lvl2 < t_min or user_lvl2 > t_max:
                        # Neither end of user range fits tournament
                        if not (user_lvl <= t_min and user_lvl2 >= t_max):
                            continue

        # Min players
        reg = len(t.get("registered_players", []))
        if reg < min_pl:
            continue

        # Hide full tournaments (available_places == 0 or reg >= max_players)
        max_pl_val = t.get("max_players", 0)
        avail = t.get("available_places")
        if avail is not None and avail <= 0:
            continue
        if max_pl_val and reg >= max_pl_val:
            continue

        result.append(t)
    return result

def filter_matchi_events(events, cfg, loc_dates):
    """Filter MATCHi events by user settings."""
    result = []
    date_from = loc_dates.get("from")
    date_to = loc_dates.get("to")
    time_from = cfg.get("time_from")
    time_to = cfg.get("time_to")
    min_pl = cfg.get("min_players_tourn", 0)  # treat as group events

    for ev in events:
        dt = parse_dt(ev.get("start_date"))

        # Date filter
        if dt and date_from:
            try:
                if dt.date() < datetime.strptime(date_from, "%Y-%m-%d").date():
                    continue
            except: pass
        if dt and date_to:
            try:
                if dt.date() > datetime.strptime(date_to, "%Y-%m-%d").date():
                    continue
            except: pass

        # Time filter — event START TIME
        if dt and time_from:
            try:
                hh, mm = map(int, time_from.split(":"))
                if dt.hour * 60 + dt.minute < hh * 60 + mm:
                    continue
            except: pass
        if dt and time_to:
            try:
                hh, mm = map(int, time_to.split(":"))
                if dt.hour * 60 + dt.minute > hh * 60 + mm:
                    continue
            except: pass

        # Min players
        if ev.get("registered_count", 0) < min_pl:
            continue

        result.append(ev)
    return result

# ─── Timezone helpers ────────────────────────────────────────────────
def utc_to_local(dt, loc_name):
    """Convert naive UTC datetime to local time for the given location."""
    if dt is None:
        return None
    tz_name = LOCATIONS.get(loc_name, {}).get("tz", "UTC")
    try:
        utc_dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return utc_dt.astimezone(ZoneInfo(tz_name))
    except Exception:
        return dt

def fmt_local_dt(dt, loc_name):
    """Format datetime as local time string dd.mm HH:MM."""
    local = utc_to_local(dt, loc_name)
    return local.strftime("%d.%m %H:%M") if local else "?"

# ─── Formatting ─────────────────────────────────────────────────────
def fmt_match(m):
    dt = parse_dt(m.get("start_date"))
    loc = m.get("_location", "")
    dt_str = fmt_local_dt(dt, loc)
    club = m.get("location") or m.get("tenant", {}).get("tenant_name", "?")
    players = match_players(m)
    max_p = match_max_players(m)
    status = m.get("status", "")
    mid = m.get("match_id", "")
    link = f"https://app.playtomic.io/matches/{mid}?product_type=open_match" if mid else ""

    lmin, lmax = match_level_range(m)
    lvl = f" | Ур. {lmin:.1f}–{lmax:.1f}" if lmin is not None else ""

    line = f"🏸 <b>{dt_str}</b> — {club}{lvl}\n"
    line += f"   👥 {players}/{max_p}"
    if link:
        line += f' | <a href="{link}">Playtomic</a>'
    # Show players in match
    all_players = [p for team in m.get("teams", []) for p in team.get("players", [])]
    if all_players:
        names = []
        for p in all_players:
            pname = (p.get("full_name") or p.get("name") or "").strip() or "?"
            plvl = p.get("level_value")
            if plvl is not None:
                names.append(f"{pname} ({plvl:.1f})")
            else:
                names.append(pname)
        line += f"\n   📝 {', '.join(names)}"
    return line

def fmt_tournament(t):
    dt = parse_dt(t.get("start_date"))
    loc = t.get("_location", "")
    dt_str = fmt_local_dt(dt, loc)
    name = t.get("tournament_name", "Турнир")
    club = t.get("tenant", {}).get("tenant_name", "")
    players = t.get("registered_players", [])
    reg = len(players)
    max_p = t.get("max_players", "?")
    lvl = t.get("level_description", "")
    price = t.get("price", "")
    tid = t.get("tournament_id", "")
    link = f"https://app.playtomic.io/tournaments/{tid}?product_type=tournament" if tid else ""
    avail = t.get("available_places")

    line = f"🏆 <b>{dt_str}</b> — {name}\n"
    line += f"   📍 {club}"
    if lvl:
        line += f" | Ур. {lvl}"
    line += f"\n   👥 {reg}/{max_p}"
    if avail is not None:
        line += f" (свободно: {avail})"
    if price:
        line += f" | {price}"
    if link:
        line += f'\n   📲 <a href="{link}">Playtomic</a>'
    # Show registered players
    if players:
        line += "\n   📝 Участники:"
        for p in players[:12]:
            pname = (p.get("full_name") or p.get("name") or "?").strip()
            if not pname or pname == "?":
                pname = "Noname"
            plvl = p.get("level_value")
            lvl_str = f" ({plvl:.2f})" if plvl is not None else ""
            line += f"\n      • {pname}{lvl_str}"
        if len(players) > 12:
            line += f"\n      ... и ещё {len(players)-12}"
    return line

def fmt_matchi(ev):
    dt = parse_dt(ev.get("start_date"))
    loc = ev.get("_location", "")
    dt_str = fmt_local_dt(dt, loc)
    name = ev.get("activity_name", "?")
    facility = ev.get("facility", "")
    reg = ev.get("registered_count", "?")
    max_p = ev.get("max_players", "?")
    lvl = ev.get("level_description", "")
    time_r = ev.get("time_range", "")
    link = ev.get("link", "")

    line = f"🎾 <b>{dt_str}</b> ({time_r}) — {name}\n"
    line += f"   📍 {facility}"
    if lvl:
        line += f" | Ур. {lvl}"
    line += f"\n   👥 {reg}/{max_p}"
    if link:
        line += f'\n   📲 <a href="{link}">MATCHi</a>'
    return line

# ─── Search engine ──────────────────────────────────────────────────
def do_search(w):
    """Run search across Playtomic + MATCHi for all configured locations."""
    all_matches = []
    all_tournaments = []
    all_matchi = []
    radius_m = w.get("radius_km", 50) * 1000

    for loc_name in w.get("locations", []):
        coords = LOCATIONS.get(loc_name)
        if not coords:
            continue
        lat, lon = coords["lat"], coords["lon"]
        loc_dates = w.get("loc_dates", {}).get(loc_name, {})

        # 1) Playtomic: Get clubs → then matches by tenant IDs
        clubs = playtomic_clubs(lat, lon, radius_m)
        tenant_ids = [c["tenant_id"] for c in clubs if "tenant_id" in c]
        log.info("Location %s: %d Playtomic clubs found", loc_name, len(tenant_ids))

        date_start = loc_dates.get("from", "2026-04-01")
        matches = playtomic_matches_by_tenants(tenant_ids, date_from=date_start)
        for m in matches:
            m["_location"] = loc_name
        filtered_m = filter_matches(matches, w, loc_dates)
        all_matches.extend(filtered_m)

        # 2) Playtomic: Tournaments by coordinate
        tournaments = playtomic_tournaments(lat, lon, radius_m)
        for t in tournaments:
            t["_location"] = loc_name
        filtered_t = filter_tournaments(tournaments, w, loc_dates)
        all_tournaments.extend(filtered_t)

        # 3) MATCHi: Activities from known facilities
        matchi_raw = matchi_events_for_location(loc_name)
        for ev in matchi_raw:
            ev["_location"] = loc_name
        filtered_mc = filter_matchi_events(matchi_raw, w, loc_dates)
        all_matchi.extend(filtered_mc)

        log.info("Location %s: PT %d/%d matches, %d/%d tournaments, MATCHi %d/%d",
                 loc_name, len(filtered_m), len(matches),
                 len(filtered_t), len(tournaments),
                 len(filtered_mc), len(matchi_raw))

    return all_matches, all_tournaments, all_matchi

def event_key(ev):
    if ev.get("platform") == "matchi":
        # MATCHi: activity + date (same activity on different dates = different events)
        return f"matchi_{ev.get('activity_id', '')}_{ev.get('date', '')}"
    eid = ev.get("match_id") or ev.get("tournament_id") or ev.get("id", "")
    if ev.get("tournament_id"):
        # Tournament: include available_places so it reappears when spot opens
        avail = ev.get("available_places", len(ev.get("registered_players", [])))
        return f"{eid}_avail{avail}"
    # Match: just match_id. If a low-level player leaves, the match
    # was previously filtered out (not in seen) → now passes filter → appears as new.
    return eid

def _get_event_dt(ev):
    """Get datetime for sorting any event type, converted to local time."""
    for key in ("start_date", "started_at", "start", "date"):
        val = ev.get(key)
        if val:
            dt = parse_dt(str(val))
            if dt:
                loc = ev.get("_location", "")
                return utc_to_local(dt, loc) or dt
    return datetime(2099, 1, 1)

def _group_by_date(events):
    """Group events by date string, sorted chronologically."""
    from collections import OrderedDict
    groups = {}
    for ev in sorted(events, key=_get_event_dt):
        dt = _get_event_dt(ev)
        date_key = dt.strftime("%a %d.%m") if dt.year < 2099 else "?"
        if date_key not in groups:
            groups[date_key] = []
        groups[date_key].append(ev)
    return groups

DAY_NAMES_RU = {"Mon": "Пн", "Tue": "Вт", "Wed": "Ср", "Thu": "Чт", "Fri": "Пт", "Sat": "Сб", "Sun": "Вс"}
def _ru_day(day_str):
    for en, ru in DAY_NAMES_RU.items():
        day_str = day_str.replace(en, ru)
    return day_str

def format_results(matches, tournaments, matchi_events=None, title=""):
    """Format results grouped by location, then by date."""
    parts = []
    if title:
        parts.append(f"<b>{title}</b>")

    # Group all events by location
    locations = {}
    for m in matches:
        loc = m.get("_location", "?")
        locations.setdefault(loc, {"matches": [], "tournaments": [], "matchi": []})
        locations[loc]["matches"].append(m)
    for t in tournaments:
        loc = t.get("_location", "?")
        locations.setdefault(loc, {"matches": [], "tournaments": [], "matchi": []})
        locations[loc]["tournaments"].append(t)
    for ev in (matchi_events or []):
        loc = ev.get("_location", "?")
        locations.setdefault(loc, {"matches": [], "tournaments": [], "matchi": []})
        locations[loc]["matchi"].append(ev)

    if not locations:
        parts.append("\nНичего не найдено")
        return "\n".join(parts)

    for loc_name, data in locations.items():
        m_count = len(data["matches"])
        t_count = len(data["tournaments"])
        mc_count = len(data["matchi"])
        total = m_count + t_count + mc_count
        parts.append(f"\n\n📍 <b>{loc_name}</b> — {total} событий")
        parts.append(f"(🏸 {m_count} матчей | 🏆 {t_count} турниров" + (f" | 🎾 {mc_count} MATCHi" if mc_count else "") + ")")

        # Matches grouped by date
        if data["matches"]:
            parts.append("")
            date_groups = _group_by_date(data["matches"])
            for date_label, evs in date_groups.items():
                parts.append(f"\n<b>📅 {_ru_day(date_label)}</b> — 🏸 Матчи:")
                for m in evs[:15]:
                    parts.append(fmt_match(m))
                if len(evs) > 15:
                    parts.append(f"  ... и ещё {len(evs)-15}")

        # Tournaments grouped by date
        if data["tournaments"]:
            parts.append("")
            date_groups = _group_by_date(data["tournaments"])
            for date_label, evs in date_groups.items():
                parts.append(f"\n<b>📅 {_ru_day(date_label)}</b> — 🏆 Турниры:")
                for t in evs[:15]:
                    parts.append(fmt_tournament(t))
                if len(evs) > 15:
                    parts.append(f"  ... и ещё {len(evs)-15}")

        # MATCHi grouped by date
        if data["matchi"]:
            parts.append("")
            date_groups = _group_by_date(data["matchi"])
            for date_label, evs in date_groups.items():
                parts.append(f"\n<b>📅 {_ru_day(date_label)}</b> — 🎾 MATCHi:")
                for ev in evs[:15]:
                    parts.append(fmt_matchi(ev))
                if len(evs) > 15:
                    parts.append(f"  ... и ещё {len(evs)-15}")

    return "\n".join(parts)

def split_message(text, limit=4000):
    if len(text) <= limit:
        return [text]
    parts = []
    while text:
        if len(text) <= limit:
            parts.append(text)
            break
        idx = text.rfind("\n", 0, limit)
        if idx == -1:
            idx = limit
        parts.append(text[:idx])
        text = text[idx:].lstrip("\n")
    return parts

# ─── Wizard state ───────────────────────────────────────────────────
def wiz(uid):
    u = get_user(uid)
    if "wizard" not in u or u["wizard"] is None:
        u["wizard"] = {
            "step": "location",
            "locations": [],
            "radius_km": 50,
            "loc_dates": {},
            "min_players_match": 1,
            "min_players_tourn": 0,
            "level_min": None,
            "level_max": None,
            "level_phase": "min",
            "time_from": None,
            "time_to": None,
            "time_from_h": 0, "time_from_m": 0,
            "time_to_h": 23, "time_to_m": 30,
            "frequency": 60,
            "dates_sub": None,
        }
        set_user(uid, u)
    return u

# ─── Keyboard builders ─────────────────────────────────────────────
def kb_location():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📍 Lahti", callback_data="loc_Lahti"),
         InlineKeyboardButton("📍 Limassol", callback_data="loc_Limassol")],
        [InlineKeyboardButton("📍 Обе", callback_data="loc_both")],
    ])

def kb_radius(km):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ -5", callback_data="rad_-5"),
         InlineKeyboardButton("+5 ▶", callback_data="rad_+5")],
        [InlineKeyboardButton("◀ -1", callback_data="rad_-1"),
         InlineKeyboardButton("+1 ▶", callback_data="rad_+1")],
        [InlineKeyboardButton("Подтвердить", callback_data="rad_ok")],
    ])

def kb_dates(loc_name, phase, days_ahead=21):
    today = datetime.utcnow().date()
    buttons = []
    row = []
    for i in range(days_ahead):
        d = today + timedelta(days=i)
        label = d.strftime("%d.%m")
        row.append(InlineKeyboardButton(label, callback_data=f"date_{loc_name}_{phase}_{d.isoformat()}"))
        if len(row) == 5:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)

def kb_min_players_match():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("0", callback_data="mpm_0"),
         InlineKeyboardButton("1 ✓", callback_data="mpm_1"),
         InlineKeyboardButton("2", callback_data="mpm_2"),
         InlineKeyboardButton("3", callback_data="mpm_3")],
    ])

def kb_min_players_tourn(val):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ -1", callback_data="mpt_-1"),
         InlineKeyboardButton("+1 ▶", callback_data="mpt_+1")],
        [InlineKeyboardButton("◀ -4", callback_data="mpt_-4"),
         InlineKeyboardButton("+4 ▶", callback_data="mpt_+4")],
        [InlineKeyboardButton("Подтвердить", callback_data="mpt_ok")],
    ])

def kb_level(phase, val):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ -0.5", callback_data=f"lvl_{phase}_-0.5"),
         InlineKeyboardButton("+0.5 ▶", callback_data=f"lvl_{phase}_+0.5")],
        [InlineKeyboardButton("◀ -1.0", callback_data=f"lvl_{phase}_-1"),
         InlineKeyboardButton("+1.0 ▶", callback_data=f"lvl_{phase}_+1")],
        [InlineKeyboardButton("Подтвердить", callback_data=f"lvl_{phase}_ok")],
        [InlineKeyboardButton("Любой уровень", callback_data="lvl_any")],
    ])

def fmt_time(h, m):
    return f"{h:02d}:{m:02d}"

def kb_time_spinner(phase, h, m):
    """Time spinner 0:00–23:30, step ±1h / ±30min. phase = 'from' or 'to'."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ -1ч", callback_data=f"t_{phase}_h-1"),
         InlineKeyboardButton("+1ч ▶", callback_data=f"t_{phase}_h+1")],
        [InlineKeyboardButton("◀ -30м", callback_data=f"t_{phase}_m-30"),
         InlineKeyboardButton("+30м ▶", callback_data=f"t_{phase}_m+30")],
        [InlineKeyboardButton("Подтвердить", callback_data=f"t_{phase}_ok")],
        [InlineKeyboardButton("Любое время", callback_data=f"t_{phase}_any")],
    ])

def kb_frequency(val):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ -10", callback_data="freq_-10"),
         InlineKeyboardButton("+10 ▶", callback_data="freq_+10")],
        [InlineKeyboardButton("◀ -1", callback_data="freq_-1"),
         InlineKeyboardButton("+1 ▶", callback_data="freq_+1")],
        [InlineKeyboardButton("Подтвердить", callback_data="freq_ok")],
    ])

def summary_text(w):
    locs = ", ".join(w.get("locations", [])) or "—"
    radius = w.get("radius_km", 50)
    dates_parts = []
    for loc, d in w.get("loc_dates", {}).items():
        dates_parts.append(f"{loc}: {d.get('from','?')} → {d.get('to','?')}")
    dates_str = "; ".join(dates_parts) if dates_parts else "—"
    tf = w.get("time_from") or "Любое"
    tt = w.get("time_to") or "Любое"
    lmin = w.get("level_min")
    lmax = w.get("level_max")
    lvl = "Любой" if lmin is None and lmax is None else f"{lmin or '?'} – {lmax or '?'}"

    return (
        f"📋 <b>Настройки мониторинга</b>\n\n"
        f"📍 Локации: {locs}\n"
        f"📏 Радиус: {radius} км\n"
        f"📅 Даты: {dates_str}\n"
        f"👥 Мин. игроков (матчи): {w.get('min_players_match', 0)}\n"
        f"👥 Мин. участников (турниры): {w.get('min_players_tourn', 0)}\n"
        f"🎯 Уровень: {lvl}\n"
        f"🕐 Время начала: {tf} – {tt}\n"
        f"🔄 Частота: каждые {w.get('frequency', 60)} мин\n"
    )

def kb_confirm(modifying=False):
    buttons = []
    if modifying:
        buttons.append([InlineKeyboardButton("💾 Применить (без перезапуска)", callback_data="wiz_apply")])
    buttons.append([InlineKeyboardButton("🚀 Запустить мониторинг", callback_data="wiz_go")])
    buttons.append([InlineKeyboardButton("🔍 Поиск сейчас (без мониторинга)", callback_data="wiz_search")])
    buttons.append([InlineKeyboardButton("⚙️ Перенастроить с нуля", callback_data="wiz_restart")])
    return InlineKeyboardMarkup(buttons)

# ─── Step renderer ──────────────────────────────────────────────────
async def show_step(source, uid, context):
    u = wiz(uid)
    w = u["wizard"]
    step = w["step"]

    async def send(text, kb):
        if hasattr(source, "edit_message_text"):
            try:
                await source.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
                return
            except Exception:
                pass
        chat_id = source.message.chat_id if hasattr(source, "message") and source.message else None
        if chat_id:
            await context.bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")

    if step == "location":
        await send("📍 <b>Шаг 1/8 — Выбери локацию:</b>", kb_location())

    elif step == "radius":
        await send(f"📏 <b>Шаг 2/8 — Радиус поиска:</b>\n\nТекущий: {w['radius_km']} км", kb_radius(w["radius_km"]))

    elif step == "dates":
        locs = w["locations"]
        done = list(w.get("loc_dates", {}).keys())
        remaining = [l for l in locs if l not in done]
        if not remaining:
            w["step"] = "min_players_m"
            set_user(uid, u)
            await show_step(source, uid, context)
            return
        loc = remaining[0]
        w["dates_sub"] = loc
        set_user(uid, u)
        has_from = "from" in w.get("loc_dates", {}).get(loc, {})
        phase = "to" if has_from else "from"
        label = "окончания" if phase == "to" else "начала"
        await send(f"📅 <b>Шаг 3/8 — Дата {label} для {loc}:</b>", kb_dates(loc, phase))

    elif step == "min_players_m":
        await send("👥 <b>Шаг 4/8 — Мин. игроков для матчей:</b>", kb_min_players_match())

    elif step == "min_players_t":
        await send(f"👥 <b>Шаг 5/8 — Мин. участников для турниров:</b>\n\nТекущий: {w['min_players_tourn']}", kb_min_players_tourn(w["min_players_tourn"]))

    elif step == "level":
        phase = w.get("level_phase", "min")
        val = w.get(f"level_{phase}") or (2.0 if phase == "min" else 4.0)
        label = "минимальный" if phase == "min" else "максимальный"
        await send(f"🎯 <b>Шаг 6/8 — Уровень ({label}):</b>\n\nТекущий: {val:.1f}", kb_level(phase, val))

    elif step == "time_from":
        h, m = w.get("time_from_h", 0), w.get("time_from_m", 0)
        await send(
            f"🕐 <b>Шаг 7/8 — Время начала события ОТ:</b>\n\nТекущее: {fmt_time(h, m)}\n"
            "Фильтр по фактическому времени начала матча/турнира.",
            kb_time_spinner("from", h, m)
        )

    elif step == "time_to":
        h, m = w.get("time_to_h", 23), w.get("time_to_m", 30)
        await send(
            f"🕐 <b>Шаг 7/8 — Время начала события ДО:</b>\n\nТекущее: {fmt_time(h, m)}\n"
            "Фильтр по фактическому времени начала матча/турнира.",
            kb_time_spinner("to", h, m)
        )

    elif step == "frequency":
        await send(f"🔄 <b>Шаг 8/8 — Частота обновления:</b>\n\nТекущая: каждые {w['frequency']} мин", kb_frequency(w["frequency"]))

    elif step == "confirm":
        modifying = bool(w.get("editing"))
        await send(summary_text(w), kb_confirm(modifying=modifying))

# ─── Handlers ───────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Открыть меню. Никого не сбрасывает — настройки и мониторинг сохраняются."""
    uid = update.effective_user.id
    u = get_user(uid)

    pt_id = u.get("playtomic_user_id")
    if not pt_id:
        await update.message.reply_text(
            "<b>Padel Monitor</b>\n\n"
            "Это бот для игроков Playtomic. Он ищет открытые матчи и турниры под твой уровень, "
            "присылает уведомления, показывает твоё расписание, календарь и PDF.\n\n"
            "Чтобы начать, пришли ссылку на свой профиль Playtomic. В приложении: "
            "Профиль → Делиться → выбери Telegram и этот чат. Ссылка выглядит так:\n"
            "<code>https://app.playtomic.io/profile/user/9436699</code>",
            parse_mode="HTML", disable_web_page_preview=True
        )
        return

    await update.message.reply_text(
        _main_menu_text(u, context, uid),
        parse_mode="HTML",
        reply_markup=_main_menu_kb(u, context, uid),
        disable_web_page_preview=True
    )

def _main_menu_text(u, context, uid):
    pt_id = u.get("playtomic_user_id", "—")
    has_wizard = bool(u.get("wizard"))
    search_on = bool(context.job_queue.get_jobs_by_name(f"watch_{uid}"))
    my_on = bool(context.job_queue.get_jobs_by_name(f"my_watch_{uid}"))
    return (
        "<b>Padel Monitor</b>\n\n"
        "Открытые матчи и турниры Playtomic под твой уровень, "
        "уведомления о новых слотах, личное расписание и PDF-календарь.\n\n"
        f"Playtomic: <code>{pt_id}</code>\n"
        f"Поиск новых игр: {'включён' if search_on else ('настроен, но остановлен' if has_wizard else 'не настроен')}\n"
        f"Мониторинг моих матчей: {'включён' if my_on else 'выключен'}"
    )

def _main_menu_kb(u, context, uid):
    has_wizard = bool(u.get("wizard"))
    search_on = bool(context.job_queue.get_jobs_by_name(f"watch_{uid}"))
    rows = []
    if has_wizard and not search_on:
        rows.append([InlineKeyboardButton("▶ Возобновить поиск игр", callback_data="resume_search")])
    if has_wizard and search_on:
        rows.append([InlineKeyboardButton("Перенастроить поиск", callback_data="wiz_begin"),
                     InlineKeyboardButton("Остановить", callback_data="stop_monitoring")])
    elif not has_wizard:
        rows.append([InlineKeyboardButton("Настроить поиск игр", callback_data="wiz_begin")])
    my_on = bool(context.job_queue.get_jobs_by_name(f"my_watch_{uid}"))
    rows += [
        [InlineKeyboardButton("Мои матчи — добавить в календарь, открыть маршрут", callback_data="my_schedule")],
        [InlineKeyboardButton("PDF календарь на печать", callback_data="pdf_menu")],
        [InlineKeyboardButton(
            "Уведомления о моих матчах: выключить" if my_on else "Уведомления о моих матчах: включить",
            callback_data="my_watch_toggle"
        )],
    ]
    if my_on:
        rows.append([InlineKeyboardButton("Проверить мои матчи сейчас", callback_data="my_watch_now")])
    rows += [
        [InlineKeyboardButton("Статус и параметры", callback_data="show_status")],
        [InlineKeyboardButton("Сменить аккаунт Playtomic", callback_data="reset_id")],
    ]
    return InlineKeyboardMarkup(rows)

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    # Set stop flag in user settings
    u = get_user(uid)
    u["monitoring_active"] = False
    set_user(uid, u)
    # Remove scheduled jobs
    for job in context.job_queue.get_jobs_by_name(f"watch_{uid}"):
        job.schedule_removal()
    await update.message.reply_text("⏹ Мониторинг остановлен.")

# ─── My schedule ───────────────────────────────────────────────────
def playtomic_user_matches(playtomic_user_id):
    """Get all matches a user is registered in. Returns up to 100 matches."""
    url = f"{BASE}/matches?sport_id=PADEL&user_id={playtomic_user_id}&size=100"
    data = api_get(url)
    return data if isinstance(data, list) else []

# ─── Calendar / Maps deep-links ───
import urllib.parse

def _match_local_dt(m):
    """Get start/end datetimes as UTC-aware. Playtomic API returns naive datetime in UTC."""
    sd = m.get("start_date")
    ed = m.get("end_date")
    if not sd:
        return None, None
    try:
        utc = ZoneInfo("UTC")
        start_utc = datetime.strptime(sd[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=utc)
        end_utc = (datetime.strptime(ed[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=utc)
                   if ed else start_utc + timedelta(minutes=90))
        return start_utc, end_utc
    except Exception:
        return None, None

def gcal_link(m):
    """Google Calendar deep-link to add the match as event."""
    start, end = _match_local_dt(m)
    if not start:
        return None
    fmt = "%Y%m%dT%H%M%SZ"
    s_utc = start.astimezone(ZoneInfo("UTC")).strftime(fmt)
    e_utc = end.astimezone(ZoneInfo("UTC")).strftime(fmt)
    title = f"Padel — {m.get('location') or 'Match'}"
    mid = m.get("match_id", "")
    addr_obj = ((m.get("location_info") or {}).get("address")
                or (m.get("tenant") or {}).get("address") or {})
    addr = ", ".join(filter(None, [addr_obj.get("street"), addr_obj.get("city"), addr_obj.get("country")])) or m.get("location", "")
    details = f"Playtomic match: https://app.playtomic.io/matches/{mid}?product_type=open_match"
    params = {
        "action": "TEMPLATE",
        "text": title,
        "dates": f"{s_utc}/{e_utc}",
        "details": details,
        "location": addr,
    }
    return "https://calendar.google.com/calendar/render?" + urllib.parse.urlencode(params)

def gmaps_link(m):
    """Google Maps directions link based on venue name + address."""
    addr_obj = ((m.get("location_info") or {}).get("address")
                or (m.get("tenant") or {}).get("address") or {})
    coord = addr_obj.get("coordinate") or {}
    if coord.get("lat") and coord.get("lon"):
        q = f"{coord['lat']},{coord['lon']}"
    else:
        q = ", ".join(filter(None, [m.get("location"), addr_obj.get("street"), addr_obj.get("city")])) or m.get("location", "")
    return "https://www.google.com/maps/search/?" + urllib.parse.urlencode({"api": "1", "query": q})

def _ics_escape(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")

def build_ics(matches, pt_id, start_d=None, end_d=None):
    """Build an .ics calendar file with the user's matches.
    Filters: only future matches the user is in (or has pending join request); excludes CANCELED/EXPIRED/FINISHED.
    Optional date range (inclusive).
    """
    today = datetime.utcnow().date()
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//PadelMonitor//RU", "CALSCALE:GREGORIAN", "METHOD:PUBLISH"]
    cnt = 0
    for m in sorted(matches, key=lambda x: x.get("start_date", "")):
        if m.get("status") in ("CANCELED", "EXPIRED", "FINISHED"):
            continue
        sd = m.get("start_date", "")[:10]
        if not sd or sd < today.isoformat():
            continue
        try:
            d = datetime.strptime(sd, "%Y-%m-%d").date()
        except Exception:
            continue
        if start_d and d < start_d: continue
        if end_d and d > end_d: continue
        # only matches user is in or has request to
        join_info = m.get("join_requests_info") or {}
        my_req = next((r for r in join_info.get("requests", []) if r.get("user_id") == pt_id), None)
        in_team = any(p.get("user_id") == pt_id
                      for t in m.get("teams", []) for p in t.get("players", []))
        if not in_team and not my_req:
            continue
        start, end = _match_local_dt(m)
        if not start:
            continue
        fmt = "%Y%m%dT%H%M%SZ"
        dtstart = start.astimezone(ZoneInfo("UTC")).strftime(fmt)
        dtend = end.astimezone(ZoneInfo("UTC")).strftime(fmt)
        mid = m.get("match_id", "")
        addr_obj = ((m.get("location_info") or {}).get("address")
                    or (m.get("tenant") or {}).get("address") or {})
        addr = ", ".join(filter(None, [addr_obj.get("street"), addr_obj.get("city"), addr_obj.get("country")])) or m.get("location", "")
        url = f"https://app.playtomic.io/matches/{mid}?product_type=open_match"
        title = f"Padel — {m.get('location') or 'Match'}"
        cur = sum(len(t.get("players", [])) for t in m.get("teams", []))
        mx = sum(t.get("max_players", 0) for t in m.get("teams", []))
        desc = f"Playtomic match. Players: {cur}/{mx}. Status: {m.get('status')}. Open: {url}"
        lines += [
            "BEGIN:VEVENT",
            f"UID:{mid}@padel-monitor",
            f"DTSTAMP:{datetime.utcnow().strftime(fmt)}",
            f"DTSTART:{dtstart}",
            f"DTEND:{dtend}",
            f"SUMMARY:{_ics_escape(title)}",
            f"LOCATION:{_ics_escape(addr)}",
            f"DESCRIPTION:{_ics_escape(desc)}",
            f"URL:{url}",
            "END:VEVENT",
        ]
        cnt += 1
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines), cnt

def format_my_schedule(matches, playtomic_user_id):
    """Group user's matches by status and date."""
    today = datetime.utcnow().date().isoformat()
    confirmed = []   # CONFIRMED status
    open_full = []   # PENDING + Phil approved + 4/4 → ready/awaiting confirmation
    pending_join = []  # Phil's join request is PENDING
    open_partial = []  # PENDING + spots still open

    for m in matches:
        if m.get("start_date", "")[:10] < today:
            continue
        if m.get("status") in ("CANCELED", "EXPIRED", "FINISHED"):
            continue

        # Check Phil's join request status
        join_info = m.get("join_requests_info") or {}
        my_request = next((r for r in join_info.get("requests", [])
                           if r.get("user_id") == playtomic_user_id), None)

        # Player is in teams
        all_players = [p for team in m.get("teams", []) for p in team.get("players", [])]
        in_team = any(p.get("user_id") == playtomic_user_id for p in all_players)
        max_p = sum(t.get("max_players", 0) for t in m.get("teams", []))
        is_full = len(all_players) >= max_p

        if m.get("status") == "CONFIRMED":
            confirmed.append(m)
        elif my_request and my_request.get("status") == "PENDING":
            pending_join.append(m)
        elif in_team and is_full:
            open_full.append(m)
        elif in_team:
            open_partial.append(m)

    parts = ["<b>📅 Моё расписание</b>\n"]

    def render_section(title, matches_list):
        if not matches_list:
            return
        parts.append(f"\n\n<b>{title}</b> ({len(matches_list)})")
        for m in sorted(matches_list, key=lambda x: x.get("start_date", "")):
            dt = parse_dt(m.get("start_date"))
            # Use match location — don't know which user location "city" is
            dt_str = dt.strftime("%a %d.%m %H:%M") if dt else "?"
            for en, ru in DAY_NAMES_RU.items():
                dt_str = dt_str.replace(en, ru)
            club = m.get("location", "?")
            mid = m.get("match_id", "")
            link = f"https://app.playtomic.io/matches/{mid}?product_type=open_match"
            cur = sum(len(t.get("players", [])) for t in m.get("teams", []))
            mx = sum(t.get("max_players", 0) for t in m.get("teams", []))
            parts.append(f"\n  • <b>{dt_str}</b> — {club} ({cur}/{mx}) — <a href=\"{link}\">Открыть</a>")

    render_section("✅ Подтверждённые (оплачены)", confirmed)
    render_section("🎯 Состав собран (ждём подтверждения/оплаты)", open_full)
    render_section("📝 Моя заявка на рассмотрении", pending_join)
    render_section("⚡ Открытые (ищут игроков)", open_partial)

    if len(parts) == 1:
        parts.append("\nНичего не запланировано.")

    return "".join(parts)

def render_calendar_pdf(matches, pt_id, start_date, end_date, output_path, location_label="", added_set=None):
    """Generate a Google-Calendar-style PDF.
    start_date, end_date: date objects (inclusive). Up to ~14 days fits well in landscape A4.
    """
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    import urllib.request

    FONTS_DIR = "/tmp/fonts"
    os.makedirs(FONTS_DIR, exist_ok=True)

    def fetch_font(url, name):
        path = os.path.join(FONTS_DIR, name)
        if not os.path.exists(path):
            try:
                urllib.request.urlretrieve(url, path)
            except Exception:
                return None
        return path

    base = "https://github.com/googlefonts/dm-fonts/raw/main/Sans/Exports/"
    fonts_ok = True
    for filename, fontname in [("DMSans-Regular.ttf", "DM"), ("DMSans-Medium.ttf", "DM-Med"), ("DMSans-Bold.ttf", "DM-Bold")]:
        p = fetch_font(base + filename, filename)
        if p:
            try:
                pdfmetrics.registerFont(TTFont(fontname, p))
            except Exception:
                fonts_ok = False
        else:
            fonts_ok = False
    F_REG = "DM" if fonts_ok else "Helvetica"
    F_MED = "DM-Med" if fonts_ok else "Helvetica"
    F_BOLD = "DM-Bold" if fonts_ok else "Helvetica-Bold"

    # Filter & convert to local time
    events = []
    for m in matches:
        sd = m.get("start_date", "")
        if not sd or m.get("status") in ("CANCELED", "EXPIRED", "FINISHED"):
            continue
        try:
            dt_utc = datetime.strptime(sd[:16], "%Y-%m-%dT%H:%M").replace(tzinfo=ZoneInfo("UTC"))
        except Exception:
            continue
        # Use first matching location's tz, fallback to user's tz preference, else UTC
        loc_for_match = m.get("_location") or ""
        tz_name = LOCATIONS.get(loc_for_match, {}).get("tz")
        if not tz_name:
            # Try detecting from any preset matching tenant city, else default to user's wizard locations
            tz_name = LOCATIONS.get((list(LOCATIONS.keys()) or [""])[0], {}).get("tz", "UTC")
        try:
            dt = dt_utc.astimezone(ZoneInfo(tz_name)).replace(tzinfo=None)
        except Exception:
            dt = dt_utc.replace(tzinfo=None)
        if dt.date() < start_date or dt.date() > end_date:
            continue
        players = [p for team in m.get("teams", []) for p in team.get("players", [])]
        max_p = sum(t.get("max_players", 0) for t in m.get("teams", []))
        cur = len(players)
        join_info = m.get("join_requests_info") or {}
        my_req = next((r for r in join_info.get("requests", []) if r.get("user_id") == pt_id), None)
        is_full = max_p > 0 and cur >= max_p
        if my_req and my_req.get("status") == "PENDING":
            kind = "pending"
        elif is_full:
            kind = "full"
        else:
            kind = "open"
        end_dt = dt + timedelta(minutes=90)
        if m.get("end_date"):
            try:
                end_utc = datetime.strptime(m["end_date"][:16], "%Y-%m-%dT%H:%M").replace(tzinfo=ZoneInfo("UTC"))
                end_dt = end_utc.astimezone(ZoneInfo(tz_name)).replace(tzinfo=None)
            except Exception:
                pass
        events.append({
            "date": dt.date(), "start": dt, "end": end_dt,
            "title": m.get("location", "?"), "kind": kind,
            "cur": cur, "max": max_p, "match_id": m.get("match_id", ""),
            "added": bool(added_set and m.get("match_id") in added_set),
        })

    PAGE_W, PAGE_H = landscape(A4)
    MARGIN_L, MARGIN_R = 14*mm, 14*mm
    MARGIN_T, MARGIN_B = 22*mm, 12*mm
    DAYS = [(start_date + timedelta(days=i)) for i in range((end_date - start_date).days + 1)]
    HEADER_H = 16*mm
    TIME_COL_W = 14*mm
    START_HOUR, END_HOUR = 6, 23
    HOURS = END_HOUR - START_HOUR
    GRID_X = MARGIN_L + TIME_COL_W
    GRID_Y_TOP = PAGE_H - MARGIN_T - HEADER_H
    GRID_Y_BOTTOM = MARGIN_B
    GRID_W = PAGE_W - MARGIN_L - MARGIN_R - TIME_COL_W
    GRID_H = GRID_Y_TOP - GRID_Y_BOTTOM
    COL_W = GRID_W / max(len(DAYS), 1)
    ROW_H = GRID_H / HOURS

    BG = (0.98, 0.97, 0.94); INK = (0.16, 0.14, 0.11)
    INK_MUTED = (0.48, 0.47, 0.45); INK_FAINT = (0.73, 0.72, 0.70)
    GRID = (0.83, 0.82, 0.79); WEEKEND = (0.94, 0.93, 0.89)
    GREEN_FILL = (0.74, 0.88, 0.66); GREEN_BORDER = (0.27, 0.48, 0.13); GREEN_TEXT = (0.16, 0.30, 0.05)
    YELLOW_FILL = (1.00, 0.93, 0.65); YELLOW_BORDER = (0.74, 0.55, 0.06); YELLOW_TEXT = (0.40, 0.28, 0.02)
    BLUE_FILL = (0.74, 0.86, 0.95); BLUE_BORDER = (0.20, 0.45, 0.66); BLUE_TEXT = (0.07, 0.24, 0.40)

    c = canvas.Canvas(output_path, pagesize=landscape(A4))
    c.setTitle(f"Padel Calendar {start_date} — {end_date}")
    c.setAuthor("Perplexity Computer")
    c.setFillColorRGB(*BG); c.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    title_y = PAGE_H - 10*mm
    c.setFillColorRGB(*INK); c.setFont(F_BOLD, 16)
    c.drawString(MARGIN_L, title_y, "Padel Calendar")
    c.setFont(F_REG, 10); c.setFillColorRGB(*INK_MUTED)
    sub = f"{start_date.strftime('%d %b')} — {end_date.strftime('%d %b %Y')}"
    if location_label:
        sub += f" · {location_label}"
    c.drawString(MARGIN_L, title_y - 14, sub)

    legend_y = title_y - 4; legend_x = PAGE_W - MARGIN_R
    c.setFont(F_REG, 9)
    for fill, border, label in [
        (BLUE_FILL, BLUE_BORDER, "My request"),
        (YELLOW_FILL, YELLOW_BORDER, "Open"),
        (GREEN_FILL, GREEN_BORDER, "Team complete"),
    ]:
        text_w = c.stringWidth(label, F_REG, 9)
        legend_x -= text_w + 9 + 14
        c.setFillColorRGB(*fill); c.setStrokeColorRGB(*border); c.setLineWidth(0.7)
        c.roundRect(legend_x, legend_y - 2, 9, 7, 1.5, fill=1, stroke=1)
        c.setFillColorRGB(*INK)
        c.drawString(legend_x + 13, legend_y, label)

    DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    for i, d in enumerate(DAYS):
        cx = GRID_X + i * COL_W
        if d.weekday() >= 5:
            c.setFillColorRGB(*WEEKEND)
            c.rect(cx, GRID_Y_BOTTOM, COL_W, GRID_H, fill=1, stroke=0)
        c.setFillColorRGB(0.99, 0.99, 0.97)
        c.rect(cx, GRID_Y_TOP, COL_W, HEADER_H, fill=1, stroke=0)
        c.setFillColorRGB(*INK_MUTED); c.setFont(F_MED, 8.5)
        c.drawString(cx + 4, GRID_Y_TOP + HEADER_H - 11, DAY_NAMES[d.weekday()].upper())
        c.setFillColorRGB(*INK); c.setFont(F_BOLD, 18)
        c.drawString(cx + 4, GRID_Y_TOP + 4, str(d.day))

    for i in range(HOURS + 1):
        y = GRID_Y_BOTTOM + (HOURS - i) * ROW_H
        c.setStrokeColorRGB(*GRID); c.setLineWidth(0.5)
        if i < HOURS:
            c.line(MARGIN_L, y, PAGE_W - MARGIN_R, y)
        hour = START_HOUR + i
        c.setFillColorRGB(*INK_FAINT); c.setFont(F_REG, 7.5)
        c.drawRightString(GRID_X - 4, y - 3, f"{hour:02d}:00")

    c.setStrokeColorRGB(*GRID)
    c.line(MARGIN_L, GRID_Y_TOP, PAGE_W - MARGIN_R, GRID_Y_TOP)
    c.line(MARGIN_L, GRID_Y_TOP + HEADER_H, PAGE_W - MARGIN_R, GRID_Y_TOP + HEADER_H)
    for i in range(len(DAYS) + 1):
        cx = GRID_X + i * COL_W
        c.line(cx, GRID_Y_BOTTOM, cx, GRID_Y_TOP + HEADER_H)

    def hour_to_y(h_float):
        return GRID_Y_TOP - (h_float - START_HOUR) * ROW_H

    for ev in sorted(events, key=lambda e: e["start"]):
        day_idx = (ev["date"] - start_date).days
        if day_idx < 0 or day_idx >= len(DAYS):
            continue
        cx = GRID_X + day_idx * COL_W
        sh = ev["start"].hour + ev["start"].minute / 60.0
        eh = ev["end"].hour + ev["end"].minute / 60.0
        if eh <= sh: eh = sh + 1.5
        y_top = hour_to_y(sh); y_bot = hour_to_y(eh)
        h = max(y_top - y_bot, 14)
        bx = cx + 2; bw = COL_W - 4
        if ev["kind"] == "full":
            fill, border, txt = GREEN_FILL, GREEN_BORDER, GREEN_TEXT
        elif ev["kind"] == "pending":
            fill, border, txt = BLUE_FILL, BLUE_BORDER, BLUE_TEXT
        else:
            fill, border, txt = YELLOW_FILL, YELLOW_BORDER, YELLOW_TEXT
        c.setFillColorRGB(*fill); c.setStrokeColorRGB(*border); c.setLineWidth(0.8)
        c.roundRect(bx, y_bot, bw, h, 2, fill=1, stroke=1)
        c.setFillColorRGB(*border); c.rect(bx, y_bot, 2, h, fill=1, stroke=0)
        c.setFillColorRGB(*txt); c.setFont(F_BOLD, 8)
        prefix = "✓ " if ev.get("added") else ""
        c.drawString(bx + 5, y_top - 9, f"{prefix}{ev['start'].strftime('%H:%M')} · {ev['cur']}/{ev['max']}")
        if h >= 22:
            c.setFont(F_MED, 7.5)
            title = ev["title"]
            max_chars = max(int((bw - 8) / 4.2), 4)
            if len(title) > max_chars:
                title = title[:max_chars - 1] + "…"
            c.drawString(bx + 5, y_top - 19, title)
        link = f"https://app.playtomic.io/matches/{ev['match_id']}?product_type=open_match"
        c.linkURL(link, (bx, y_bot, bx + bw, y_top), relative=0)

    c.setFillColorRGB(*INK_FAINT); c.setFont(F_REG, 7)
    c.drawString(MARGIN_L, MARGIN_B / 2,
                 f"Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} · Tap any event to open in Playtomic")
    c.showPage(); c.save()
    return len(events)

def format_my_calendar(matches, pt_id, days_ahead=11, added_set=None):
    """Visual calendar grid: days x time-of-day blocks.
    Green emoji = full team (4/4), yellow = open spots, blue = my pending request.
    Matches in added_set are prefixed with ✅."""
    today = datetime.utcnow().date()
    end = today + timedelta(days=days_ahead)

    # Filter & group matches by date
    by_date = {}
    for m in matches:
        sd = m.get("start_date", "")
        if not sd:
            continue
        if m.get("status") in ("CANCELED", "EXPIRED", "FINISHED"):
            continue
        try:
            dt = datetime.strptime(sd[:16], "%Y-%m-%dT%H:%M")
        except ValueError:
            continue
        if dt.date() < today or dt.date() > end:
            continue
        by_date.setdefault(dt.date(), []).append((dt, m))

    if not by_date:
        return "На ближайшие дни ничего не запланировано."

    parts = [f"<b>🗓 Календарь {today.strftime('%d.%m')} — {end.strftime('%d.%m')}</b>\n"]
    parts.append("🟢 Состав собран · 🟡 Ищут игроков · 🔵 Моя заявка\n")

    for d in sorted(by_date.keys()):
        day_str = d.strftime("%d.%m")
        wd = DAY_NAMES_RU_SHORT.get(d.weekday(), "?")
        parts.append(f"\n<b>📅 {wd} {day_str}</b>")

        for dt, m in sorted(by_date[d], key=lambda x: x[0]):
            players = [p for team in m.get("teams", []) for p in team.get("players", [])]
            max_p = sum(t.get("max_players", 0) for t in m.get("teams", []))
            cur = len(players)
            join_info = m.get("join_requests_info") or {}
            my_req = next((r for r in join_info.get("requests", []) if r.get("user_id") == pt_id), None)

            if my_req and my_req.get("status") == "PENDING":
                icon = "🔵"
            elif max_p > 0 and cur >= max_p:
                icon = "🟢"
            else:
                icon = "🟡"

            time_str = dt.strftime("%H:%M")
            club = m.get("location", "?")
            mid = m.get("match_id", "")
            link = f"https://app.playtomic.io/matches/{mid}?product_type=open_match"
            check = "✅" if added_set and mid in added_set else ""
            parts.append(
                f"  {icon} <code>{time_str}</code> {cur}/{max_p} — "
                f'<a href="{link}">{club}</a> {check}'
            )

    return "\n".join(parts)

async def _send_pdf_calendar(uid, chat_id, context, start_date, end_date, label_extra=""):
    """Build PDF and send to chat."""
    u = get_user(uid)
    pt_id = u.get("playtomic_user_id")
    if not pt_id:
        await context.bot.send_message(chat_id, NEED_LINK_TEXT,
            parse_mode="HTML", disable_web_page_preview=True)
        return
    matches = playtomic_user_matches(pt_id)
    # Determine location label from user's wizard or first match
    loc_label = ""
    w = u.get("wizard") or {}
    locs = w.get("locations") or []
    if locs:
        loc_label = ", ".join(locs)
    out_path = f"{DATA_DIR}/calendar_{uid}_{start_date}_{end_date}.pdf"
    try:
        n = render_calendar_pdf(matches, pt_id, start_date, end_date, out_path,
                                location_label=loc_label, added_set=get_added(uid))
    except Exception as e:
        log.exception("PDF render failed")
        await context.bot.send_message(chat_id, f"Ошибка создания PDF: {e}")
        return
    if n == 0:
        await context.bot.send_message(chat_id,
            f"В диапазоне {start_date.strftime('%d.%m')}–{end_date.strftime('%d.%m')} нет матчей.")
        os.remove(out_path) if os.path.exists(out_path) else None
        return
    caption = f"🗓 Календарь {start_date.strftime('%d.%m')}–{end_date.strftime('%d.%m.%Y')}\n{n} матчей{label_extra}"
    with open(out_path, "rb") as f:
        await context.bot.send_document(chat_id, document=f, filename=os.path.basename(out_path), caption=caption)
    try:
        os.remove(out_path)
    except Exception:
        pass

async def cmd_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send PDF calendar. Usage: /pdf  (next 14 days)  |  /pdf 2026-05-05 2026-05-15"""
    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    args = context.args or []
    today = datetime.utcnow().date()
    if len(args) >= 2:
        try:
            start_date = datetime.strptime(args[0], "%Y-%m-%d").date()
            end_date = datetime.strptime(args[1], "%Y-%m-%d").date()
        except ValueError:
            await update.message.reply_text(
                "Не получилось разобрать даты. Открой меню и выбери «PDF календарь»."
            )
            return
        if end_date < start_date:
            await update.message.reply_text("Конечная дата раньше начальной. Открой меню кнопкой «Menu» внизу.")
            return
        days = (end_date - start_date).days + 1
        if days > 21:
            await update.message.reply_text(f"Диапазон слишком большой ({days} дн.). Максимум 21 день.")
            return
    else:
        start_date = today
        end_date = today + timedelta(days=13)
    await update.message.reply_text("🔄 Строю PDF...")
    await _send_pdf_calendar(uid, chat_id, context, start_date, end_date)

async def cmd_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Visual calendar of upcoming matches."""
    uid = update.effective_user.id
    u = get_user(uid)
    pt_id = u.get("playtomic_user_id")
    if not pt_id:
        await _need_link(update.message)
        return
    await update.message.reply_text("Строю календарь...")
    matches = playtomic_user_matches(pt_id)
    text = format_my_calendar(matches, pt_id, added_set=get_added(uid))
    for chunk in split_message(text):
        await update.message.reply_text(chunk, parse_mode="HTML", disable_web_page_preview=True)

async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's upcoming matches grouped by status."""
    uid = update.effective_user.id
    u = get_user(uid)
    pt_id = u.get("playtomic_user_id")
    if not pt_id:
        await _need_link(update.message)
        return
    await update.message.reply_text("🔄 Загружаю расписание...")
    matches = playtomic_user_matches(pt_id)
    text = format_my_schedule(matches, pt_id)
    for chunk in split_message(text):
        await update.message.reply_text(chunk, parse_mode="HTML", disable_web_page_preview=True)

_PROFILE_RE = re.compile(r"playtomic\.io/profile/user/(\d+)")

NEED_LINK_TEXT = (
    "Пришли ссылку на свой профиль Playtomic. В приложении: "
    "Профиль → Делиться → Telegram. Пример ссылки:\n"
    "<code>https://app.playtomic.io/profile/user/9436699</code>"
)

async def _need_link(target):
    """Reply asking the user to share their Playtomic profile link."""
    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(NEED_LINK_TEXT, parse_mode="HTML", disable_web_page_preview=True)
    else:
        await target.reply_text(NEED_LINK_TEXT, parse_mode="HTML", disable_web_page_preview=True)

def parse_playtomic_id(text: str) -> str | None:
    """Accepts a numeric ID or a profile share link, returns numeric user_id."""
    if not text:
        return None
    text = text.strip()
    m = _PROFILE_RE.search(text)
    if m:
        return m.group(1)
    if text.isdigit():
        return text
    return None

async def cmd_setid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save Playtomic user_id (accepts numeric ID or profile share link)."""
    uid = update.effective_user.id
    raw = " ".join(context.args) if context.args else ""
    pt_id = parse_playtomic_id(raw)
    if not pt_id:
        await _need_link(update.message)
        return
    u = get_user(uid)
    u["playtomic_user_id"] = pt_id
    set_user(uid, u)
    await update.message.reply_text(
        f"Playtomic ID сохранён: <code>{pt_id}</code>\n\nАккаунт привязан. Открой меню кнопкой «Menu» внизу.",
        parse_mode="HTML"
    )

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Catch profile links pasted directly into chat."""
    if not update.message or not update.message.text:
        return
    text = update.message.text
    pt_id = parse_playtomic_id(text)
    if not pt_id or "playtomic.io/profile/user/" not in text:
        return  # оставляем визарду/другим обработчикам разобраться с обычным текстом
    uid = update.effective_user.id
    u = get_user(uid)
    u["playtomic_user_id"] = pt_id
    set_user(uid, u)
    await update.message.reply_text(
        f"Playtomic ID сохранён: <code>{pt_id}</code>\n\nАккаунт привязан. Открой меню кнопкой «Menu» внизу.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Открыть меню", callback_data="back_main")],
        ])
    )

async def cmd_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Edit settings without restarting monitoring or losing seen events."""
    uid = update.effective_user.id
    u = get_user(uid)
    w = u.get("wizard")
    if not w:
        await update.message.reply_text("Настроек пока нет. Открой меню кнопкой «Menu» и выбери «Настроить поиск игр».")
        return
    # Mark as editing — keeps seen_events, lets user change any param
    w["editing"] = True
    w["step"] = "location"
    set_user(uid, u)
    await show_step(update, uid, context)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = get_user(uid)
    w = u.get("wizard")
    if not w:
        await update.message.reply_text("Настройки не заданы. Открой меню кнопкой «Menu».")
        return
    text = summary_text(w)
    active = bool(context.job_queue.get_jobs_by_name(f"watch_{uid}"))
    text += f"\n\n{'✅ Мониторинг активен' if active else '⏸ Мониторинг неактивен'}"
    await update.message.reply_text(text, parse_mode="HTML")

# ─── Callback router ───────────────────────────────────────────────
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id
    data = q.data
    u = wiz(uid)
    w = u["wizard"]

    # ── Show status ──
    if data == "show_status":
        pt_id = u.get("playtomic_user_id") or "не задан"
        active_search = bool(context.job_queue.get_jobs_by_name(f"watch_{uid}"))
        active_my = bool(context.job_queue.get_jobs_by_name(f"my_watch_{uid}"))
        wz = u.get("wizard") or {}
        text = (
            f"<b>Статус</b>\n\n"
            f"Playtomic ID: <code>{pt_id}</code>\n"
            f"Поиск новых игр: {'включён' if active_search else 'выключен'}\n"
            f"Мониторинг моих матчей: {'включён' if active_my else 'выключен'}"
        )
        if wz:
            text += "\n\n" + summary_text(wz)
        await q.edit_message_text(text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← В меню", callback_data="back_main")]]))
        return

    # ── Stop monitoring (search) ──
    if data == "stop_monitoring":
        u["monitoring_active"] = False
        set_user(uid, u)
        for job in context.job_queue.get_jobs_by_name(f"watch_{uid}"):
            job.schedule_removal()
        await q.edit_message_text(
            "Мониторинг поиска остановлен.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← В меню", callback_data="back_main")]]))
        return

    # ── Reset Playtomic ID ──
    if data == "reset_id":
        u["playtomic_user_id"] = None
        u["my_account_active"] = False
        set_user(uid, u)
        for job in context.job_queue.get_jobs_by_name(f"my_watch_{uid}"):
            job.schedule_removal()
        await q.edit_message_text(
            "Аккаунт Playtomic отвязан. Пришли ссылку на новый профиль или нажми “В меню”.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← В меню", callback_data="back_main")]]))
        return

    # ── Back to main menu ──
    if data == "back_main":
        pt_id = u.get("playtomic_user_id")
        if not pt_id:
            await q.edit_message_text(NEED_LINK_TEXT, parse_mode="HTML", disable_web_page_preview=True)
            return
        await q.edit_message_text(_main_menu_text(u, context, uid),
            parse_mode="HTML", disable_web_page_preview=True,
            reply_markup=_main_menu_kb(u, context, uid))
        return

    # ── Ручная проверка моих матчей ──
    if data == "my_watch_now":
        pt_id = u.get("playtomic_user_id")
        if not pt_id:
            await _need_link(q); return
        chat_id = q.message.chat_id
        await q.edit_message_text("Проверяю изменения...")
        matches = playtomic_user_matches(pt_id)
        prev_states = u.get("my_match_states", {})
        new_states = {m["match_id"]: _my_match_state(m, pt_id) for m in matches if m.get("match_id")}
        events = _diff_my_matches(prev_states, matches, pt_id)
        u["my_match_states"] = new_states
        u["chat_id"] = chat_id
        set_user(uid, u)
        if events:
            text = "<b>Изменения в моих матчах:</b>\n\n" + "\n\n".join(events)
            for chunk in split_message(text):
                await context.bot.send_message(chat_id, chunk, parse_mode="HTML", disable_web_page_preview=True)
        else:
            await context.bot.send_message(chat_id, "Ничего не изменилось.")
        await context.bot.send_message(chat_id, "—",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← В меню", callback_data="back_main")]]))
        return

    # ── Resume search monitoring (without reset) ──
    if data == "resume_search":
        w = u.get("wizard")
        if not w:
            await q.edit_message_text("Настройки поиска не найдены. Нажми «Настроить поиск игр».",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← В меню", callback_data="back_main")]]))
            return
        chat_id = q.message.chat_id
        u["monitoring_active"] = True
        u["chat_id"] = chat_id
        set_user(uid, u)
        for job in context.job_queue.get_jobs_by_name(f"watch_{uid}"):
            job.schedule_removal()
        freq_sec = w.get("frequency", 60) * 60
        context.job_queue.run_repeating(
            watch_tick, interval=freq_sec, first=10,
            name=f"watch_{uid}", data={"uid": uid, "chat_id": chat_id},
        )
        await q.edit_message_text(
            f"Поиск возобновлён. Проверка каждые {w.get('frequency', 60)} мин.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← В меню", callback_data="back_main")]]))
        return

    # ── My account watch toggle ──
    if data == "my_watch_toggle":
        pt_id = u.get("playtomic_user_id")
        if not pt_id:
            await q.edit_message_text(
                NEED_LINK_TEXT, parse_mode="HTML", disable_web_page_preview=True)
            return
        if u.get("my_account_active"):
            u["my_account_active"] = False
            set_user(uid, u)
            for job in context.job_queue.get_jobs_by_name(f"my_watch_{uid}"):
                job.schedule_removal()
            await q.edit_message_text("⏹ Мониторинг моего аккаунта остановлен.")
            return
        matches_my = playtomic_user_matches(pt_id)
        new_states = {m["match_id"]: _my_match_state(m, pt_id)
                      for m in matches_my if m.get("match_id")}
        u["my_match_states"] = new_states
        u["my_account_active"] = True
        u["chat_id"] = q.message.chat_id
        set_user(uid, u)
        for job in context.job_queue.get_jobs_by_name(f"my_watch_{uid}"):
            job.schedule_removal()
        context.job_queue.run_repeating(
            watch_my_account, interval=180, first=15,
            name=f"my_watch_{uid}",
            data={"uid": uid, "chat_id": q.message.chat_id},
        )
        await q.edit_message_text(
            f"✅ Мониторинг моего аккаунта включён.\n\n"
            f"Отслеживаю {len(new_states)} матчей, проверка каждые 3 мин.\n\n"
            f"Буду сообщать о:\n"
            f"• Статусе матча (CONFIRMED/CANCELED)\n"
            f"• Входе/выходе игроков\n"
            f"• Заполнении состава\n"
            f"• Одобрении/отклонении заявок\n\n"
            f"Отключить: повторно нажми «Уведомления об изменениях» в меню.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← В меню", callback_data="back_main")]])
        )
        return

    # ── PDF menu ──
    if data == "pdf_menu":
        await q.edit_message_text(
            "<b>📄 PDF календарь</b>\n\n"
            "Выбери диапазон:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📆 7 дней", callback_data="pdf_7"),
                 InlineKeyboardButton("📆 14 дней", callback_data="pdf_14"),
                 InlineKeyboardButton("📆 21 день", callback_data="pdf_21")],
                [InlineKeyboardButton("🎯 Все даты с матчами", callback_data="pdf_all")],
                [InlineKeyboardButton("✏️ Произвольный диапазон", callback_data="pdf_custom")],
            ]))
        return

    if data and data.startswith("pdfr_"):
        # custom range from preset buttons
        try:
            _, s, e = data.split("_", 2)
            start_date = datetime.strptime(s, "%Y-%m-%d").date()
            end_date = datetime.strptime(e, "%Y-%m-%d").date()
        except Exception:
            return
        chat_id = q.message.chat_id
        await q.edit_message_text("Строю PDF...")
        await _send_pdf_calendar(uid, chat_id, context, start_date, end_date)
        return

    if data and data.startswith("pdf_"):
        action = data[4:]
        chat_id = q.message.chat_id
        today = datetime.utcnow().date()
        if action == "custom":
            # Вместо ввода вручную — выбор пресетов кнопками
            today = datetime.utcnow().date()
            from calendar import monthrange
            this_m_end = today.replace(day=monthrange(today.year, today.month)[1])
            next_w_start = today + timedelta(days=(7 - today.weekday()) % 7 or 7)
            next_w_end = next_w_start + timedelta(days=6)
            await q.edit_message_text(
                "<b>Выбери период:</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"До конца месяца ({this_m_end.strftime('%d.%m')})", callback_data=f"pdfr_{today.isoformat()}_{this_m_end.isoformat()}")],
                    [InlineKeyboardButton(f"Следующая неделя ({next_w_start.strftime('%d.%m')}–{next_w_end.strftime('%d.%m')})", callback_data=f"pdfr_{next_w_start.isoformat()}_{next_w_end.isoformat()}")],
                    [InlineKeyboardButton("+30 дней", callback_data=f"pdfr_{today.isoformat()}_{(today + timedelta(days=29)).isoformat()}")],
                    [InlineKeyboardButton("← Назад", callback_data="pdf_menu")],
                ]))
            return
        if action == "all":
            pt_id = u.get("playtomic_user_id")
            if not pt_id:
                await _need_link(q)
                return
            await q.edit_message_text("🔄 Ищу матчи...")
            matches = playtomic_user_matches(pt_id)
            future_dates = []
            for m in matches:
                if m.get("status") in ("CANCELED", "EXPIRED", "FINISHED"):
                    continue
                sd = m.get("start_date", "")[:10]
                if sd >= today.isoformat():
                    future_dates.append(sd)
            if not future_dates:
                await context.bot.send_message(chat_id, "Будущих матчей нет.")
                return
            start_date = datetime.strptime(min(future_dates), "%Y-%m-%d").date()
            end_date = datetime.strptime(max(future_dates), "%Y-%m-%d").date()
            # Cap span
            if (end_date - start_date).days > 30:
                end_date = start_date + timedelta(days=30)
                await context.bot.send_message(chat_id,
                    f"⚠️ Диапазон обрезан до 30 дней ({start_date}–{end_date}).")
            await _send_pdf_calendar(uid, chat_id, context, start_date, end_date,
                                     label_extra=" · все будущие")
            return
        # numeric range: 7 / 14 / 21 days
        try:
            days = int(action)
        except ValueError:
            return
        start_date = today
        end_date = today + timedelta(days=days - 1)
        await q.edit_message_text("🔄 Строю PDF...")
        await _send_pdf_calendar(uid, chat_id, context, start_date, end_date)
        return

    # ── My calendar button ──
    if data == "my_calendar":
        pt_id = u.get("playtomic_user_id")
        if not pt_id:
            await _need_link(q)
            return
        await q.edit_message_text("Строю календарь...")
        matches = playtomic_user_matches(pt_id)
        text = format_my_calendar(matches, pt_id, added_set=get_added(uid))
        chat_id = q.message.chat_id
        for chunk in split_message(text):
            await context.bot.send_message(chat_id, chunk, parse_mode="HTML", disable_web_page_preview=True)
        return

    # ── My schedule button ──
    if data == "my_schedule":
        pt_id = u.get("playtomic_user_id")
        if not pt_id:
            await _need_link(q)
            return
        await q.edit_message_text("Загружаю расписание...")
        matches = playtomic_user_matches(pt_id)
        chat_id = q.message.chat_id
        # Группируем будущие матчи юзера
        today = datetime.utcnow().date().isoformat()
        upcoming = []
        for m in matches:
            if m.get("status") in ("CANCELED", "EXPIRED", "FINISHED"):
                continue
            if m.get("start_date", "")[:10] < today:
                continue
            join_info = m.get("join_requests_info") or {}
            my_req = next((r for r in join_info.get("requests", []) if r.get("user_id") == pt_id), None)
            in_team = any(p.get("user_id") == pt_id for t in m.get("teams", []) for p in t.get("players", []))
            if in_team or my_req:
                upcoming.append(m)
        upcoming.sort(key=lambda x: x.get("start_date", ""))

        if not upcoming:
            await context.bot.send_message(chat_id, "Будущих матчей нет.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← В меню", callback_data="back_main")]]))
            return

        added = get_added(uid)
        # Сверху — быстрый экспорт всего и диапазоны
        await context.bot.send_message(chat_id,
            f"<b>Расписание — {len(upcoming)} матчей</b>\n\n"
            "<b>Добавить все игры в календарь</b> — бот пришлёт один файл. "
            "Нажми на него — и все матчи добавятся в Google/Apple/Outlook календарь одним нажатием.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Добавить все игры в календарь", callback_data="ics_all")],
                [InlineKeyboardButton("Только неделя", callback_data="ics_w"),
                 InlineKeyboardButton("2 недели", callback_data="ics_2w"),
                 InlineKeyboardButton("Месяц", callback_data="ics_m")],
            ]))
        for m in upcoming:
            dt = parse_dt(m.get("start_date"))
            tz_str = (((m.get("location_info") or {}).get("address") or {}).get("timezone")
                      or ((m.get("tenant") or {}).get("address") or {}).get("timezone") or "UTC")
            try:
                # Playtomic returns naive UTC — attach UTC then convert to venue local
                local = dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo(tz_str)) if dt else None
                dt_str = local.strftime("%a %d.%m %H:%M") if local else "?"
            except Exception:
                dt_str = dt.strftime("%a %d.%m %H:%M") if dt else "?"
            for en, ru in DAY_NAMES_RU.items():
                dt_str = dt_str.replace(en, ru)
            club = m.get("location", "?")
            mid = m.get("match_id", "")
            cur = sum(len(t.get("players", [])) for t in m.get("teams", []))
            mx = sum(t.get("max_players", 0) for t in m.get("teams", []))
            status = m.get("status", "")
            join_info = m.get("join_requests_info") or {}
            my_req = next((r for r in join_info.get("requests", []) if r.get("user_id") == pt_id), None)
            label = "Подтверждён" if status == "CONFIRMED" else ("Заявка ожидает" if my_req and my_req.get("status") == "PENDING" else f"Игроков: {cur}/{mx}")
            link_match = f"https://app.playtomic.io/matches/{mid}?product_type=open_match"
            check = "✅ " if mid in added else ""
            text = (f"{check}<b>{dt_str}</b> — {club}\n{label}")
            buttons = []
            gc = gcal_link(m)
            gm = gmaps_link(m)
            if gc:
                buttons.append([InlineKeyboardButton("Добавить в Google Calendar", url=gc)])
            buttons.append([InlineKeyboardButton("Добавить в Apple/Outlook календарь", callback_data=f"ics1_{mid}")])
            row2 = [InlineKeyboardButton("Открыть Playtomic", url=link_match)]
            if gm: row2.append(InlineKeyboardButton("Маршрут", url=gm))
            buttons.append(row2)
            mark_label = "✅ Добавлено — снять отметку" if mid in added else "Отметить как добавленное"
            buttons.append([InlineKeyboardButton(mark_label, callback_data=f"mark_{mid}")])
            await context.bot.send_message(chat_id, text, parse_mode="HTML",
                disable_web_page_preview=True, reply_markup=InlineKeyboardMarkup(buttons))
        return

    # ── Toggle "added" mark ──
    if data and data.startswith("mark_"):
        match_id = data[5:]
        new_state = toggle_added(uid, match_id)
        # Обновим текст и кнопку этого сообщения
        try:
            old_text = q.message.text_html or q.message.text or ""
            if new_state and not old_text.startswith("✅ "):
                new_text = "✅ " + old_text
            elif not new_state and old_text.startswith("✅ "):
                new_text = old_text[2:]
            else:
                new_text = old_text
            # Собираем новые кнопки на основе старых, подменив лейбл mark
            new_kb = []
            for row in (q.message.reply_markup.inline_keyboard if q.message.reply_markup else []):
                new_row = []
                for b in row:
                    if b.callback_data == data:
                        lbl = "✅ Добавлено — снять отметку" if new_state else "Отметить как добавленное"
                        new_row.append(InlineKeyboardButton(lbl, callback_data=data))
                    else:
                        new_row.append(b)
                new_kb.append(new_row)
            await q.edit_message_text(new_text, parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup(new_kb))
        except Exception as e:
            log.warning("mark toggle redraw failed: %s", e)
        return

    # ── ICS export (single match) ──
    if data and data.startswith("ics1_"):
        match_id = data[5:]
        pt_id = u.get("playtomic_user_id")
        if not pt_id:
            await _need_link(q); return
        chat_id = q.message.chat_id
        await context.bot.send_chat_action(chat_id=chat_id, action="upload_document")
        matches = playtomic_user_matches(pt_id)
        m = next((x for x in matches if x.get("match_id") == match_id), None)
        if not m:
            await context.bot.send_message(chat_id, "Матч не найден.")
            return
        ics, n = build_ics([m], pt_id)
        if n == 0:
            await context.bot.send_message(chat_id, "Матч в прошедшем или вы не в составе.")
            return
        path = f"{DATA_DIR}/match_{match_id}.ics"
        with open(path, "w") as f: f.write(ics)
        with open(path, "rb") as f:
            await context.bot.send_document(chat_id, document=f, filename=f"padel_match.ics",
                caption="Открой файл — добавится в Apple/Google/Outlook календарь.")
        try: os.remove(path)
        except Exception: pass
        # Автоматически помечаем как добавленный
        mark_added(uid, match_id)
        return

    # ── ICS export (range) ──
    if data in ("ics_all", "ics_w", "ics_2w", "ics_m"):
        pt_id = u.get("playtomic_user_id")
        if not pt_id:
            await _need_link(q); return
        chat_id = q.message.chat_id
        today = datetime.utcnow().date()
        end_d = None
        if data == "ics_w": end_d = today + timedelta(days=6)
        elif data == "ics_2w": end_d = today + timedelta(days=13)
        elif data == "ics_m":
            from calendar import monthrange
            end_d = today.replace(day=monthrange(today.year, today.month)[1])
        await context.bot.send_chat_action(chat_id=chat_id, action="upload_document")
        matches = playtomic_user_matches(pt_id)
        ics, n = build_ics(matches, pt_id, start_d=today, end_d=end_d)
        if n == 0:
            await context.bot.send_message(chat_id, "Нет матчей в этом диапазоне.")
            return
        path = f"{DATA_DIR}/schedule_{uid}_{data}.ics"
        with open(path, "w") as f: f.write(ics)
        with open(path, "rb") as f:
            await context.bot.send_document(chat_id, document=f, filename="padel_schedule.ics",
                caption=f"{n} матчей. Открой в календаре — добавятся все события.")
        try: os.remove(path)
        except Exception: pass
        # Помечаем все экспортированные как добавленные
        u_now = get_user(uid)
        added_set = set(u_now.get("calendar_added", []))
        for m in matches:
            mid_check = m.get("match_id")
            sd = m.get("start_date", "")[:10]
            if not mid_check or not sd: continue
            if m.get("status") in ("CANCELED","EXPIRED","FINISHED"): continue
            try:
                d = datetime.strptime(sd, "%Y-%m-%d").date()
            except Exception: continue
            if d < today: continue
            if end_d and d > end_d: continue
            join_info = m.get("join_requests_info") or {}
            my_req = next((r for r in join_info.get("requests", []) if r.get("user_id") == pt_id), None)
            in_team = any(p.get("user_id") == pt_id for t in m.get("teams", []) for p in t.get("players", []))
            if in_team or my_req:
                added_set.add(mid_check)
        u_now["calendar_added"] = list(added_set)
        set_user(uid, u_now)
        return

    # ── Wizard start/restart ──
    # wiz_begin: мягкий — если визард уже есть, режим редактирования, история сохраняется.
    # wiz_restart: полный сброс (явный выбор «Перенастроить с нуля»).
    if data == "wiz_restart":
        u["wizard"] = None
        u["seen_events"] = {}
        set_user(uid, u)
        u = wiz(uid)
        await show_step(q, uid, context)
        return
    if data == "wiz_begin":
        if u.get("wizard"):
            u["wizard"]["editing"] = True
            u["wizard"]["step"] = "location"
            set_user(uid, u)
        else:
            u = wiz(uid)
        await show_step(q, uid, context)
        return

    # ── Stop button ──
    if data == "cmd_stop_btn":
        u_stop = get_user(uid)
        u_stop["monitoring_active"] = False
        set_user(uid, u_stop)
        for job in context.job_queue.get_jobs_by_name(f"watch_{uid}"):
            job.schedule_removal()
        await q.edit_message_text(
            "Мониторинг остановлен.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← В меню", callback_data="back_main")]]))
        return

    # ── Location ──
    if data.startswith("loc_"):
        choice = data[4:]
        w["locations"] = ["Lahti", "Limassol"] if choice == "both" else [choice]
        w["step"] = "radius"
        set_user(uid, u)
        await show_step(q, uid, context)
        return

    # ── Radius ──
    if data.startswith("rad_"):
        cmd = data[4:]
        if cmd == "noop":
            return  # индикатор — не кликабельный
        if cmd == "ok":
            w["step"] = "dates"
        else:
            w["radius_km"] = max(1, min(200, w["radius_km"] + int(cmd)))
        set_user(uid, u)
        await show_step(q, uid, context)
        return

    # ── Dates ──
    if data.startswith("date_"):
        _, loc, phase, iso = data.split("_", 3)
        if loc not in w.get("loc_dates", {}):
            w["loc_dates"][loc] = {}
        w["loc_dates"][loc][phase] = iso
        set_user(uid, u)
        if phase == "from":
            await q.edit_message_text(
                f"📅 <b>Шаг 3/8 — Дата окончания для {loc}:</b>",
                reply_markup=kb_dates(loc, "to"), parse_mode="HTML")
        else:
            remaining = [l for l in w["locations"] if l not in w["loc_dates"] or "to" not in w["loc_dates"].get(l, {})]
            if remaining:
                w["dates_sub"] = remaining[0]
                set_user(uid, u)
            else:
                w["step"] = "min_players_m"
                set_user(uid, u)
            await show_step(q, uid, context)
        return

    # ── Min players match ──
    if data.startswith("mpm_"):
        w["min_players_match"] = int(data[4:])
        w["step"] = "min_players_t"
        set_user(uid, u)
        await show_step(q, uid, context)
        return

    # ── Min players tournament ──
    if data.startswith("mpt_"):
        cmd = data[4:]
        if cmd == "noop":
            return
        if cmd == "ok":
            w["step"] = "level"
            w["level_phase"] = "min"
            if w.get("level_min") is None: w["level_min"] = 2.0
            if w.get("level_max") is None: w["level_max"] = 4.0
        else:
            w["min_players_tourn"] = max(0, min(32, w["min_players_tourn"] + int(cmd)))
        set_user(uid, u)
        await show_step(q, uid, context)
        return

    # ── Level ──
    if data.startswith("lvl_"):
        if data == "lvl_noop":
            return
        if data == "lvl_any":
            w["level_min"] = None
            w["level_max"] = None
            w["step"] = "time_from"
            set_user(uid, u)
            await show_step(q, uid, context)
            return
        parts = data.split("_", 2)
        phase = parts[1]
        action = parts[2]
        if action == "ok":
            if phase == "min":
                w["level_phase"] = "max"
            else:
                w["step"] = "time_from"
            set_user(uid, u)
            await show_step(q, uid, context)
            return
        key = f"level_{phase}"
        current = w.get(key) or (2.0 if phase == "min" else 4.0)
        w[key] = max(0.0, min(10.0, current + float(action)))
        set_user(uid, u)
        await show_step(q, uid, context)
        return

    # ── Time spinners (0:00 – 23:30) ──
    if data.startswith("t_"):
        if data == "t_noop":
            return
        parts = data.split("_", 2)
        phase = parts[1]  # from / to
        action = parts[2]

        h_key = f"time_{phase}_h"
        m_key = f"time_{phase}_m"

        if action == "ok":
            w[f"time_{phase}"] = fmt_time(w.get(h_key, 0), w.get(m_key, 0))
            w["step"] = "time_to" if phase == "from" else "frequency"
            set_user(uid, u)
            await show_step(q, uid, context)
            return

        if action == "any":
            w[f"time_{phase}"] = None
            w["step"] = "time_to" if phase == "from" else "frequency"
            set_user(uid, u)
            await show_step(q, uid, context)
            return

        h = w.get(h_key, 0)
        m = w.get(m_key, 0)

        if action == "h-1":
            h = (h - 1) % 24
        elif action == "h+1":
            h = (h + 1) % 24
        elif action == "m-30":
            m -= 30
            if m < 0:
                m = 30
                h = (h - 1) % 24
        elif action == "m+30":
            m += 30
            if m >= 60:
                m = 0
                h = (h + 1) % 24

        w[h_key] = h
        w[m_key] = m
        set_user(uid, u)
        await show_step(q, uid, context)
        return

    # ── Frequency ──
    if data.startswith("freq_"):
        cmd = data[5:]
        if cmd == "noop":
            return
        if cmd == "ok":
            w["step"] = "confirm"
        else:
            w["frequency"] = max(1, min(120, w["frequency"] + int(cmd)))
        set_user(uid, u)
        await show_step(q, uid, context)
        return

    # ── Apply changes without restart ──
    if data == "wiz_apply":
        # Save settings, keep seen_events. Reschedule with new frequency.
        w["editing"] = False
        set_user(uid, u)
        chat_id = q.message.chat_id

        # Reschedule the watch job with new frequency (keeps existing seen_events)
        job_name = f"watch_{uid}"
        for job in context.job_queue.get_jobs_by_name(job_name):
            job.schedule_removal()

        u["monitoring_active"] = True
        set_user(uid, u)
        freq_sec = w.get("frequency", 60) * 60
        context.job_queue.run_repeating(
            watch_tick, interval=freq_sec, first=freq_sec,
            name=job_name, data={"uid": uid, "chat_id": chat_id},
        )
        await q.edit_message_text(
            "✅ Настройки обновлены.\n"
            "Придут только реально новые события, проходящие по новым фильтрам.\n\n"
            f"🔄 Проверка каждые {w.get('frequency', 60)} мин",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✏️ Изменить ещё", callback_data="wiz_edit_again")],
                [InlineKeyboardButton("⏹ Остановить", callback_data="cmd_stop_btn")],
            ]),
        )
        return

    if data == "wiz_edit_again":
        u["wizard"]["editing"] = True
        u["wizard"]["step"] = "location"
        set_user(uid, u)
        await show_step(q, uid, context)
        return

    # ── Confirm & Launch ──
    if data == "wiz_go":
        await launch_monitoring(q, uid, context, w)
        return

    if data == "wiz_search":
        chat_id = q.message.chat_id
        await q.edit_message_text("🔍 Ищу по всем платформам...", parse_mode="HTML")
        matches, tournaments, matchi = do_search(w)
        text = format_results(matches, tournaments, matchi, "📊 Результаты поиска")
        seen = {}
        for m in matches: seen[event_key(m)] = True
        for t in tournaments: seen[event_key(t)] = True
        for mc in matchi: seen[event_key(mc)] = True
        u["seen_events"] = seen
        set_user(uid, u)
        for chunk in split_message(text):
            await context.bot.send_message(chat_id, chunk, parse_mode="HTML", disable_web_page_preview=True)
        await context.bot.send_message(chat_id, "Готово.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⚙️ Перенастроить", callback_data="wiz_restart")],
            ]), parse_mode="HTML")
        return

# ─── Monitoring ─────────────────────────────────────────────────────
async def launch_monitoring(q, uid, context, w):
    job_name = f"watch_{uid}"
    for job in context.job_queue.get_jobs_by_name(job_name):
        job.schedule_removal()

    chat_id = q.message.chat_id
    await q.edit_message_text("🔍 Первый поиск...", parse_mode="HTML")

    matches, tournaments, matchi = do_search(w)
    text = format_results(matches, tournaments, matchi, "📊 Начальный отчёт — все подходящие матчи и турниры")

    u = get_user(uid)
    seen = {}
    for m in matches: seen[event_key(m)] = True
    for t in tournaments: seen[event_key(t)] = True
    for mc in matchi: seen[event_key(mc)] = True
    u["seen_events"] = seen
    u["monitoring_active"] = True
    u["chat_id"] = chat_id  # store for restart recovery
    set_user(uid, u)

    for chunk in split_message(text):
        await context.bot.send_message(chat_id, chunk, parse_mode="HTML", disable_web_page_preview=True)

    freq_sec = w.get("frequency", 60) * 60
    context.job_queue.run_repeating(
        watch_tick, interval=freq_sec, first=freq_sec,
        name=job_name, data={"uid": uid, "chat_id": chat_id},
    )

    await context.bot.send_message(
        chat_id,
        f"✅ Мониторинг запущен — проверка каждые {w.get('frequency', 60)} мин.\n"
        f"Буду присылать только <b>новые</b> события.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⚙️ Перенастроить", callback_data="wiz_restart")],
            [InlineKeyboardButton("⏹ Остановить", callback_data="cmd_stop_btn")],
        ]),
    )

def _my_match_state(m, pt_id):
    """Snapshot user-relevant state of a match for diff monitoring."""
    players = [p for team in m.get("teams", []) for p in team.get("players", [])]
    max_p = sum(t.get("max_players", 0) for t in m.get("teams", []))
    join_info = m.get("join_requests_info") or {}
    my_req = next((r for r in join_info.get("requests", []) if r.get("user_id") == pt_id), None)
    return {
        "status": m.get("status"),
        "player_ids": sorted([p.get("user_id") for p in players]),
        "player_count": len(players),
        "max_players": max_p,
        "is_full": len(players) >= max_p if max_p else False,
        "my_request_status": my_req.get("status") if my_req else None,
    }

def _diff_my_matches(prev_states, current_matches, pt_id):
    """Compare previous snapshot vs current. Returns list of human-readable change events."""
    events = []
    today = datetime.utcnow().date().isoformat()
    current_by_id = {m["match_id"]: m for m in current_matches
                     if m.get("start_date", "")[:10] >= today
                     and m.get("status") not in ("FINISHED",)}

    for mid, m in current_by_id.items():
        cur = _my_match_state(m, pt_id)
        prev = prev_states.get(mid)

        # Format match label
        dt = parse_dt(m.get("start_date"))
        when = dt.strftime("%a %d.%m %H:%M") if dt else "?"
        for en, ru in DAY_NAMES_RU.items():
            when = when.replace(en, ru)
        club = m.get("location", "?")
        link = f"https://app.playtomic.io/matches/{mid}?product_type=open_match"
        label = f'<b>{when}</b> — {club} <a href="{link}">»</a>'

        if prev is None:
            # New match this user is involved in
            if pt_id in cur["player_ids"] or cur["my_request_status"]:
                if cur["my_request_status"] == "PENDING":
                    events.append(f"📝 Новая заявка: {label}")
                elif cur["my_request_status"] == "APPROVED":
                    events.append(f"✅ Заявка одобрена: {label}")
                else:
                    events.append(f"➕ Добавлен в матч: {label}")
            continue

        # Status changed
        if cur["status"] != prev["status"]:
            if cur["status"] == "CONFIRMED":
                events.append(f"✅ Матч ПОДТВЕРЖДЁН: {label}")
            elif cur["status"] == "CANCELED":
                events.append(f"❌ Матч ОТМЕНЁН: {label}")
            elif cur["status"] == "EXPIRED":
                events.append(f"⚠️ Матч истёк: {label}")
            else:
                events.append(f"🔄 Статус изменён ({prev['status']} → {cur['status']}): {label}")

        # Players composition changed
        joined = set(cur["player_ids"]) - set(prev["player_ids"])
        left = set(prev["player_ids"]) - set(cur["player_ids"])
        if joined:
            jnames = []
            for p in [p for team in m.get("teams", []) for p in team.get("players", [])]:
                if p.get("user_id") in joined:
                    n = (p.get("full_name") or p.get("name") or "?")
                    lvl = p.get("level_value")
                    jnames.append(f'{n}{f" ({lvl:.1f})" if lvl is not None else ""}')
            events.append(f"➕ Игрок вошёл ({', '.join(jnames)}): {label}")
        if left:
            events.append(f"➖ Игрок вышел: {label}")

        # Full / no longer full
        if cur["is_full"] and not prev["is_full"]:
            events.append(f"🎯 Состав ПОЛНЫЙ ({cur['player_count']}/{cur['max_players']}): {label}")
        elif not cur["is_full"] and prev["is_full"]:
            events.append(f"🟡 Освободилось место ({cur['player_count']}/{cur['max_players']}): {label}")

        # Join request status change
        if cur["my_request_status"] != prev["my_request_status"]:
            if cur["my_request_status"] == "APPROVED":
                events.append(f"✅ Твоя заявка ОДОБРЕНА: {label}")
            elif cur["my_request_status"] == "REJECTED":
                events.append(f"❌ Твоя заявка ОТКЛОНЕНА: {label}")
            elif cur["my_request_status"] == "PENDING":
                events.append(f"📝 Отправлена заявка: {label}")

    # ── Исчезнувшие матчи (удалены/отменены/я вышел) ──
    for mid, prev in prev_states.items():
        if mid in current_by_id:
            continue
        # Было состояние, сейчас матч не возвращается API.
        link = f"https://app.playtomic.io/matches/{mid}?product_type=open_match"
        events.append(f"❌ Матч удалён или вы больше не в составе: <a href=\"{link}\">открыть</a>")

    return events

async def watch_my_account(context: ContextTypes.DEFAULT_TYPE):
    """Monitor changes in user's own Playtomic matches."""
    uid = context.job.data["uid"]
    chat_id = context.job.data["chat_id"]
    u = get_user(uid)
    if not u.get("my_account_active"):
        context.job.schedule_removal()
        return
    pt_id = u.get("playtomic_user_id")
    if not pt_id:
        return
    matches = playtomic_user_matches(pt_id)
    prev_states = u.get("my_match_states", {})
    new_states = {m["match_id"]: _my_match_state(m, pt_id)
                  for m in matches if m.get("match_id")}
    events = _diff_my_matches(prev_states, matches, pt_id)
    u["my_match_states"] = new_states
    set_user(uid, u)
    if events:
        text = "<b>📅 Изменения в моём расписании:</b>\n\n" + "\n\n".join(events)
        for chunk in split_message(text):
            await context.bot.send_message(chat_id, chunk, parse_mode="HTML", disable_web_page_preview=True)

async def cmd_my_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle monitoring of user's own matches."""
    uid = update.effective_user.id
    u = get_user(uid)
    pt_id = u.get("playtomic_user_id")
    if not pt_id:
        await _need_link(update.message)
        return

    if u.get("my_account_active"):
        u["my_account_active"] = False
        set_user(uid, u)
        for job in context.job_queue.get_jobs_by_name(f"my_watch_{uid}"):
            job.schedule_removal()
        await update.message.reply_text("⏹ Мониторинг моего аккаунта остановлен.")
        return

    # Initialize — take a snapshot, no notifications on first run
    matches = playtomic_user_matches(pt_id)
    new_states = {m["match_id"]: _my_match_state(m, pt_id)
                  for m in matches if m.get("match_id")}
    u["my_match_states"] = new_states
    u["my_account_active"] = True
    u["chat_id"] = update.effective_chat.id
    set_user(uid, u)

    chat_id = update.effective_chat.id
    for job in context.job_queue.get_jobs_by_name(f"my_watch_{uid}"):
        job.schedule_removal()
    context.job_queue.run_repeating(
        watch_my_account, interval=180, first=15,
        name=f"my_watch_{uid}", data={"uid": uid, "chat_id": chat_id},
    )
    await update.message.reply_text(
        f"✅ Мониторинг моего аккаунта включён (проверка каждые 3 мин).\n\n"
        f"Отслеживаю: {len(new_states)} матчей\n\n"
        f"Буду сообщать о:\n"
        f"  • Изменении статуса матча (CONFIRMED/CANCELED)\n"
        f"  • Входе/выходе игроков\n"
        f"  • Заполнении состава (4/4)\n"
        f"  • Одобрении/отклонении твоей заявки\n\n"
        f"Отключить: повторно нажми «Уведомления об изменениях» в меню."
    )

async def watch_tick(context: ContextTypes.DEFAULT_TYPE):
    uid = context.job.data["uid"]
    chat_id = context.job.data["chat_id"]
    u = get_user(uid)
    # Check stop flag — if stopped, cancel this job and exit
    if not u.get("monitoring_active", False):
        context.job.schedule_removal()
        return
    w = u.get("wizard")
    if not w:
        return

    matches, tournaments, matchi = do_search(w)
    seen = u.get("seen_events", {})

    new_m = [m for m in matches if event_key(m) not in seen]
    new_t = [t for t in tournaments if event_key(t) not in seen]
    new_mc = [mc for mc in matchi if event_key(mc) not in seen]

    if not new_m and not new_t and not new_mc:
        return

    for m in new_m: seen[event_key(m)] = True
    for t in new_t: seen[event_key(t)] = True
    for mc in new_mc: seen[event_key(mc)] = True
    u["seen_events"] = seen
    set_user(uid, u)

    text = format_results(new_m, new_t, new_mc, "🆕 Новые события")
    for chunk in split_message(text):
        await context.bot.send_message(chat_id, chunk, parse_mode="HTML", disable_web_page_preview=True)

# ─── Main ───────────────────────────────────────────────────────────
async def post_init(application):
    """Restore active monitoring jobs after restart and set bot commands."""
    # Единственная видимая команда — /start, всё остальное через кнопки
    try:
        await application.bot.set_my_commands([BotCommand("start", "Открыть меню")])
        await application.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        await application.bot.set_my_description(
            "Мониторинг открытых матчей и турниров Playtomic. "
            "Уведомления о новых слотах, личное расписание и PDF-календарь."
        )
        await application.bot.set_my_short_description(
            "Открытые матчи Playtomic, уведомления, расписание."
        )
    except Exception as e:
        log.warning("set_my_commands failed: %s", e)
    all_settings = load_all_settings()
    restored, my_restored = 0, 0
    for uid_str, cfg in all_settings.items():
        try:
            uid = int(uid_str)
        except ValueError:
            continue
        chat_id = cfg.get("chat_id", uid)
        # Search monitoring
        if cfg.get("monitoring_active") and cfg.get("wizard"):
            freq_sec = cfg["wizard"].get("frequency", 60) * 60
            application.job_queue.run_repeating(
                watch_tick, interval=freq_sec, first=freq_sec,
                name=f"watch_{uid}", data={"uid": uid, "chat_id": chat_id},
            )
            restored += 1
        # My-account monitoring
        if cfg.get("my_account_active") and cfg.get("playtomic_user_id"):
            application.job_queue.run_repeating(
                watch_my_account, interval=180, first=30,
                name=f"my_watch_{uid}", data={"uid": uid, "chat_id": chat_id},
            )
            my_restored += 1
    log.info(f"Restored {restored} search job(s), {my_restored} my-account job(s)")

def main():
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("edit", cmd_edit))
    app.add_handler(CommandHandler("schedule", cmd_schedule))
    app.add_handler(CommandHandler("setid", cmd_setid))
    app.add_handler(CommandHandler("mywatch", cmd_my_watch))
    app.add_handler(CommandHandler("calendar", cmd_calendar))
    app.add_handler(CommandHandler("pdf", cmd_pdf))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(CallbackQueryHandler(on_callback))
    log.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
