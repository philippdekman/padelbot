"""Модуль мониторинга свободных кортов (Playtomic /v1/availability).

Архитектура:
- Пресет хранится в user_settings под ключом `court_presets` (список dict).
- Активный мониторинг хранится под ключом `court_watch` (один на пользователя).
- Job выполняется каждые 10 минут, делает diff: новые свободные слоты → пуш.
- Авто-стоп: когда все указанные временные окна в прошлом.

Структура preset:
{
  "name": "Limassol вечера",
  "tenant_ids": ["..."],
  "tenant_names": {"id": "name"},
  "loc_name": "Limassol",          # для timezone
  "date_from": "2026-05-05",
  "date_to": "2026-05-15",
  "windows": [{"from": "09:00", "to": "12:00"}, {"from": "18:00", "to": "21:00"}],
  "min_duration": 90,              # 60/90/120
}
"""
from __future__ import annotations
import json, urllib.request, logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional

log = logging.getLogger(__name__)

PT_BASE = "https://api.playtomic.io/v1"


# ─── Playtomic availability ────────────────────────────────────────
def fetch_availability(tenant_id: str, day: date) -> list:
    """Returns list of resources with slots for that day. Empty on error."""
    url = (f"{PT_BASE}/availability?sport_id=PADEL&tenant_id={tenant_id}"
           f"&start_min={day.isoformat()}T00:00:00"
           f"&start_max={day.isoformat()}T23:59:59")
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning("availability fetch failed for %s %s: %s", tenant_id, day, e)
        return []


def book_url(tenant_id: str, day: date, start_time: str, duration: int) -> str:
    """Deep-link to Playtomic booking flow with pre-filled slot."""
    # Базовый URL — открывает страницу клуба на нужной дате; Playtomic web сам
    # покажет доступные слоты. Точный deep-link на конкретный слот в публичном
    # API не документирован.
    return (f"https://app.playtomic.io/clubs/{tenant_id}"
            f"?date={day.isoformat()}&duration={duration}&time={start_time[:5]}")


# ─── Slot filtering ────────────────────────────────────────────────
def slot_matches_windows(start_time: str, windows: list) -> bool:
    """Check if slot start time falls in any of the configured windows."""
    if not windows:
        return True
    hh, mm = map(int, start_time.split(":")[:2])
    sm = hh * 60 + mm
    for w in windows:
        f_h, f_m = map(int, w["from"].split(":"))
        t_h, t_m = map(int, w["to"].split(":"))
        if f_h * 60 + f_m <= sm <= t_h * 60 + t_m:
            return True
    return False


def filter_slots(availability: list, preset: dict, day: date, tz: ZoneInfo,
                 now_local: Optional[datetime] = None) -> list:
    """Returns list of slot dicts matching preset filters.
    Each slot: {tenant_id, resource_id, day, start, duration, price, key}
    """
    out = []
    min_dur = preset.get("min_duration", 60)
    windows = preset.get("windows", [])
    now = now_local or datetime.now(tz).replace(tzinfo=None)
    for resource in availability:
        rid = resource.get("resource_id", "")
        for s in resource.get("slots", []):
            dur = s.get("duration", 0)
            if dur < min_dur:
                continue
            st = s.get("start_time", "")
            if not st:
                continue
            if not slot_matches_windows(st, windows):
                continue
            # Skip past slots
            try:
                hh, mm, ss = map(int, st.split(":"))
                slot_dt = datetime.combine(day, datetime.min.time()).replace(hour=hh, minute=mm)
                if slot_dt <= now:
                    continue
            except Exception:
                pass
            out.append({
                "resource_id": rid,
                "day": day.isoformat(),
                "start": st[:5],
                "duration": dur,
                "price": s.get("price", ""),
                "key": f"{rid}|{day.isoformat()}|{st[:5]}|{dur}",
            })
    return out


def collect_slots(preset: dict, tz: ZoneInfo) -> list:
    """Iterate every day in preset range and every tenant, return all matching slots."""
    try:
        d_from = datetime.strptime(preset["date_from"], "%Y-%m-%d").date()
        d_to = datetime.strptime(preset["date_to"], "%Y-%m-%d").date()
    except Exception:
        return []
    today = datetime.now(tz).date()
    if d_to < today:
        return []
    if d_from < today:
        d_from = today
    all_slots = []
    cur = d_from
    while cur <= d_to:
        for tid in preset.get("tenant_ids", []):
            avail = fetch_availability(tid, cur)
            slots = filter_slots(avail, preset, cur, tz)
            for s in slots:
                s["tenant_id"] = tid
            all_slots.extend(slots)
        cur += timedelta(days=1)
    return all_slots


def is_preset_expired(preset: dict, tz: ZoneInfo) -> bool:
    """Return True if all windows are in the past."""
    try:
        d_to = datetime.strptime(preset["date_to"], "%Y-%m-%d").date()
    except Exception:
        return True
    today = datetime.now(tz).date()
    if d_to < today:
        return True
    if d_to > today:
        return False
    # d_to == today: check last window end-time
    windows = preset.get("windows", []) or [{"from": "00:00", "to": "23:59"}]
    last_end = max(((int(w["to"].split(":")[0]) * 60 + int(w["to"].split(":")[1]))
                    for w in windows), default=0)
    now = datetime.now(tz)
    now_min = now.hour * 60 + now.minute
    return now_min > last_end


# ─── Formatting ────────────────────────────────────────────────────
DAY_RU = {0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс"}


def format_new_slots(new_slots: list, tenant_names: dict) -> str:
    """Group by club, then by day."""
    if not new_slots:
        return ""
    by_club = {}
    for s in new_slots:
        by_club.setdefault(s["tenant_id"], []).append(s)
    lines = ["<b>Новые свободные корты</b>\n"]
    for tid, slots in by_club.items():
        name = tenant_names.get(tid, tid[:8])
        lines.append(f"\n<b>{name}</b>")
        slots.sort(key=lambda x: (x["day"], x["start"]))
        for s in slots:
            try:
                d = datetime.strptime(s["day"], "%Y-%m-%d").date()
                wd = DAY_RU.get(d.weekday(), "?")
                day_str = f"{wd} {d.strftime('%d.%m')}"
            except Exception:
                day_str = s["day"]
            link = book_url(tid, datetime.strptime(s["day"], "%Y-%m-%d").date(),
                            s["start"], s["duration"])
            lines.append(
                f'  {day_str} {s["start"]} · {s["duration"]} мин · {s["price"]} '
                f'<a href="{link}">забронировать</a>'
            )
    return "\n".join(lines)


def format_preset_summary(preset: dict) -> str:
    names = ", ".join(preset.get("tenant_names", {}).values()) or "—"
    windows = preset.get("windows", [])
    win_str = ", ".join(f'{w["from"]}–{w["to"]}' for w in windows) if windows else "любое"
    return (
        f"<b>{preset.get('name', 'Без названия')}</b>\n"
        f"Клубы: {names}\n"
        f"Даты: {preset.get('date_from', '?')} — {preset.get('date_to', '?')}\n"
        f"Окна: {win_str}\n"
        f"Мин. длительность: {preset.get('min_duration', 60)} мин"
    )
