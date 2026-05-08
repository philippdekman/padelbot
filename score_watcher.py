"""Score Watcher — independent service.

Polls Playtomic for the user's matches and sends a shareable score image
the moment a score is published. Independent state, independent job.

Adaptive polling per match:
  • 0–2 h after match end_date  → check every 10 min
  • 2–48 h after match end_date → check every 3 h
  • > 48 h with no score         → give up (mark as final, never notify)

The job itself ticks every 10 min; per-match `next_check_at` timestamps in
user settings throttle when each match is actually examined.

State (in user settings):
  notified_scores:   list[match_id]      — already announced, skip forever
  score_check_state: { match_id: {"next_check_at": iso} }

Pulls scores in any status (incl. VALIDATING) — no need to wait for CONFIRMED.
"""
from __future__ import annotations
import logging
from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

log = logging.getLogger(__name__)

JOB_INTERVAL_SEC = 10 * 60          # tick every 10 min
FAST_WINDOW = timedelta(hours=2)    # 0-2h after end → 10 min cadence
SLOW_WINDOW = timedelta(hours=48)   # 2-48h after end → 3h cadence
SLOW_GAP = timedelta(hours=3)
FAST_GAP = timedelta(minutes=10)


def has_published_score(match: dict) -> bool:
    """True if any set has at least one numeric score.

    Does NOT filter by match.status — Playtomic publishes the score in
    VALIDATING (one team has submitted, the other hasn't confirmed yet).
    We tag the score the moment it appears, no waiting for CONFIRMED.
    """
    for s in match.get("results", []) or []:
        for entry in s.get("scores") or []:
            if isinstance(entry, dict) and entry.get("score") is not None:
                return True
    return False


def _parse_iso(s: str | None):
    if not s:
        return None
    try:
        return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def _build_caption(match: dict, my_user_id: str) -> str:
    teams = match.get("teams", [])
    my_team_id = None
    for t in teams:
        if any(p.get("user_id") == my_user_id for p in t.get("players", [])):
            my_team_id = t.get("team_id"); break

    sets_strs, my_total, opp_total = [], 0, 0
    for s in match.get("results", []) or []:
        vals = {}
        for e in s.get("scores") or []:
            if isinstance(e, dict) and e.get("score") is not None:
                vals[str(e.get("team_id"))] = e.get("score")
        if not vals:
            continue
        my = vals.get(str(my_team_id))
        opp = next((v for k, v in vals.items() if k != str(my_team_id)), None)
        if my is None or opp is None:
            continue
        my_total += int(my); opp_total += int(opp)
        sets_strs.append(f"{my}–{opp}")

    outcome = ("Победа" if my_total > opp_total
               else "Поражение" if my_total < opp_total else "Ничья")
    club = match.get("location", "?")
    sets_line = " · ".join(sets_strs) if sets_strs else "—"
    return (f"🏆 <b>Счёт матча — {outcome}</b>\n"
            f"{club}\n"
            f"<b>{sets_line}</b>")


async def watch_scores(context: ContextTypes.DEFAULT_TYPE):
    """Job tick: examine matches whose next_check_at is due."""
    # Lazy imports — avoid circular at module load time
    from bot import (get_user, set_user, playtomic_user_matches,
                     _is_quiet, DATA_DIR)
    from score_image import render_score_image

    uid = context.job.data["uid"]
    chat_id = context.job.data["chat_id"]

    if _is_quiet(uid):
        return

    u = get_user(uid)
    if not u.get("my_account_active"):
        # Mirrors the my-account watcher's lifecycle
        context.job.schedule_removal()
        return

    pt_id = u.get("playtomic_user_id")
    if not pt_id:
        return

    init_at = (u.get("my_watch_init_at") or "")[:19]
    notified = set(u.get("notified_scores") or [])
    state = dict(u.get("score_check_state") or {})
    now = datetime.utcnow()

    # Prefilter: do we have any match due now? Build a list from the matches API.
    try:
        matches = playtomic_user_matches(pt_id)
    except Exception as e:
        log.warning("score_watcher: matches fetch failed: %s", e)
        return

    sent = 0
    state_changed = False

    for m in matches:
        mid = m.get("match_id")
        if not mid or mid in notified:
            continue
        if m.get("status") == "CANCELED":
            continue

        sd_raw = m.get("start_date") or ""
        ed_raw = m.get("end_date") or sd_raw
        if not sd_raw:
            continue
        # Only matches that started AFTER monitoring was first enabled
        if init_at and sd_raw[:19] < init_at:
            continue
        end_dt = _parse_iso(ed_raw)
        if not end_dt or now < end_dt:
            continue  # not finished yet

        elapsed = now - end_dt
        # Throttle by per-match next_check_at
        per = state.get(mid) or {}
        nxt = _parse_iso(per.get("next_check_at"))
        if nxt and now < nxt:
            continue

        # Examine this match now
        if has_published_score(m):
            caption = _build_caption(m, pt_id)
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(
                "Открыть матч в Playtomic",
                url=f"https://app.playtomic.io/matches/{mid}?product_type=open_match")]])
            try:
                img_path = f"{DATA_DIR}/score_{mid}.png"
                render_score_image(m, pt_id, img_path)
                with open(img_path, "rb") as f:
                    await context.bot.send_photo(chat_id, photo=f, caption=caption,
                                                 parse_mode="HTML", reply_markup=kb)
            except Exception as e:
                log.warning("score_watcher: image send failed for %s: %s", mid, e)
                try:
                    await context.bot.send_message(chat_id, caption,
                                                   parse_mode="HTML",
                                                   disable_web_page_preview=True,
                                                   reply_markup=kb)
                except Exception:
                    continue
            notified.add(mid)
            state.pop(mid, None)
            state_changed = True
            sent += 1
        else:
            # Schedule next check based on adaptive cadence
            if elapsed < FAST_WINDOW:
                nxt_dt = now + FAST_GAP
            elif elapsed < SLOW_WINDOW:
                nxt_dt = now + SLOW_GAP
            else:
                # Give up — mark as final to prevent rechecking forever
                notified.add(mid)
                state.pop(mid, None)
                state_changed = True
                log.info("score_watcher: giving up on %s (>48h, no score)", mid)
                continue
            state[mid] = {"next_check_at": nxt_dt.isoformat()}
            state_changed = True

    if state_changed:
        # Cap notified list to last 500 to keep settings file small
        u["notified_scores"] = list(notified)[-500:]
        u["score_check_state"] = state
        set_user(uid, u)

    if sent:
        log.info("score_watcher: sent %d new score(s) for uid=%s", sent, uid)
