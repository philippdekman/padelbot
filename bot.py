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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    print("ERROR: TELEGRAM_BOT_TOKEN environment variable is not set!")
    print("Set it in Railway: Variables tab -> New Variable")
    exit(1)
SETTINGS_FILE = "user_settings.json"

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
         InlineKeyboardButton(f"📏 {km} км", callback_data="rad_noop"),
         InlineKeyboardButton("+5 ▶", callback_data="rad_+5")],
        [InlineKeyboardButton("◀ -1", callback_data="rad_-1"),
         InlineKeyboardButton("+1 ▶", callback_data="rad_+1")],
        [InlineKeyboardButton("✅ Подтвердить", callback_data="rad_ok")],
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
         InlineKeyboardButton(f"👥 {val}", callback_data="mpt_noop"),
         InlineKeyboardButton("+1 ▶", callback_data="mpt_+1")],
        [InlineKeyboardButton("◀ -4", callback_data="mpt_-4"),
         InlineKeyboardButton("+4 ▶", callback_data="mpt_+4")],
        [InlineKeyboardButton("✅ Подтвердить", callback_data="mpt_ok")],
    ])

def kb_level(phase, val):
    label = f"{'Мин' if phase == 'min' else 'Макс'}: {val:.1f}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ -0.5", callback_data=f"lvl_{phase}_-0.5"),
         InlineKeyboardButton(label, callback_data="lvl_noop"),
         InlineKeyboardButton("+0.5 ▶", callback_data=f"lvl_{phase}_+0.5")],
        [InlineKeyboardButton("◀ -1.0", callback_data=f"lvl_{phase}_-1"),
         InlineKeyboardButton("+1.0 ▶", callback_data=f"lvl_{phase}_+1")],
        [InlineKeyboardButton("✅ Подтвердить", callback_data=f"lvl_{phase}_ok")],
        [InlineKeyboardButton("Любой уровень", callback_data="lvl_any")],
    ])

def fmt_time(h, m):
    return f"{h:02d}:{m:02d}"

def kb_time_spinner(phase, h, m):
    """Time spinner 0:00–23:30, step ±1h / ±30min. phase = 'from' or 'to'."""
    time_label = fmt_time(h, m)
    title = "🕐 Время ОТ" if phase == "from" else "🕐 Время ДО"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{title}: {time_label}", callback_data="t_noop")],
        [InlineKeyboardButton("◀ -1ч", callback_data=f"t_{phase}_h-1"),
         InlineKeyboardButton("⏰", callback_data="t_noop"),
         InlineKeyboardButton("+1ч ▶", callback_data=f"t_{phase}_h+1")],
        [InlineKeyboardButton("◀ -30м", callback_data=f"t_{phase}_m-30"),
         InlineKeyboardButton("+30м ▶", callback_data=f"t_{phase}_m+30")],
        [InlineKeyboardButton("✅ Подтвердить", callback_data=f"t_{phase}_ok")],
        [InlineKeyboardButton("⏭ Любое время", callback_data=f"t_{phase}_any")],
    ])

def kb_frequency(val):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ -10", callback_data="freq_-10"),
         InlineKeyboardButton(f"🔄 {val} мин", callback_data="freq_noop"),
         InlineKeyboardButton("+10 ▶", callback_data="freq_+10")],
        [InlineKeyboardButton("◀ -1", callback_data="freq_-1"),
         InlineKeyboardButton("+1 ▶", callback_data="freq_+1")],
        [InlineKeyboardButton("✅ Подтвердить", callback_data="freq_ok")],
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
        await send(f"🎯 <b>Шаг 6/8 — Уровень ({label}):</b>", kb_level(phase, val))

    elif step == "time_from":
        await send(
            "🕐 <b>Шаг 7/8 — Время начала события ОТ:</b>\n\n"
            "Фильтр по фактическому времени начала матча/турнира.\n"
            "Диапазон: 00:00 – 23:30",
            kb_time_spinner("from", w.get("time_from_h", 0), w.get("time_from_m", 0))
        )

    elif step == "time_to":
        await send(
            "🕐 <b>Шаг 7/8 — Время начала события ДО:</b>\n\n"
            "Фильтр по фактическому времени начала матча/турнира.\n"
            "Диапазон: 00:00 – 23:30",
            kb_time_spinner("to", w.get("time_to_h", 23), w.get("time_to_m", 30))
        )

    elif step == "frequency":
        await send(f"🔄 <b>Шаг 8/8 — Частота обновления:</b>\n\nТекущая: каждые {w['frequency']} мин", kb_frequency(w["frequency"]))

    elif step == "confirm":
        modifying = bool(w.get("editing"))
        await send(summary_text(w), kb_confirm(modifying=modifying))

# ─── Handlers ───────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    # Stop any existing monitoring — prevents duplicate notifications
    for job in context.job_queue.get_jobs_by_name(f"watch_{uid}"):
        job.schedule_removal()
    u = get_user(uid)
    u["wizard"] = None
    u["seen_events"] = {}
    u["monitoring_active"] = False
    set_user(uid, u)

    await update.message.reply_text(
        "👋 <b>Padel Monitor Bot</b>\n\n"
        "Мониторинг матчей и турниров на Playtomic\n\n"
        "Нажми кнопку ниже чтобы настроить параметры:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⚙️ Настроить мониторинг", callback_data="wiz_begin")],
        ])
    )

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

async def cmd_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Edit settings without restarting monitoring or losing seen events."""
    uid = update.effective_user.id
    u = get_user(uid)
    w = u.get("wizard")
    if not w:
        await update.message.reply_text("Нет настроек. Отправь /start чтобы начать.")
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
        await update.message.reply_text("Настройки не заданы. /start")
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

    # ── Wizard start/restart (full reset) ──
    if data in ("wiz_begin", "wiz_restart"):
        u["wizard"] = None
        u["seen_events"] = {}  # full reset clears history
        set_user(uid, u)
        u = wiz(uid)
        w = u["wizard"]
        await show_step(q, uid, context)
        return

    # ── Stop button ──
    if data == "cmd_stop_btn":
        u_stop = get_user(uid)
        u_stop["monitoring_active"] = False
        set_user(uid, u_stop)
        for job in context.job_queue.get_jobs_by_name(f"watch_{uid}"):
            job.schedule_removal()
        await q.edit_message_text("⏹ Мониторинг остановлен.\n\n/start — начать заново")
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
        if cmd == "ok":
            w["step"] = "dates"
        elif cmd != "noop":
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
        if cmd == "ok":
            w["step"] = "level"
            w["level_phase"] = "min"
            if w.get("level_min") is None: w["level_min"] = 2.0
            if w.get("level_max") is None: w["level_max"] = 4.0
        elif cmd != "noop":
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
        if cmd == "ok":
            w["step"] = "confirm"
        elif cmd != "noop":
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
        f"Буду присылать только <b>новые</b> события.\n\nОстановить: /stop",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⚙️ Перенастроить", callback_data="wiz_restart")],
            [InlineKeyboardButton("⏹ Остановить", callback_data="cmd_stop_btn")],
        ]),
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
def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("edit", cmd_edit))
    app.add_handler(CallbackQueryHandler(on_callback))
    log.info("Bot starting...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
