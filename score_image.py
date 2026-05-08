"""Generate a shareable match score image (PNG) for social media.

Layout: 1080x1080 (square — universal for Instagram, VK, Telegram, Twitter).
Header with club and status. Big score in middle. Two team blocks with avatars,
names, levels. Footer with date and Playtomic mark.
"""
from __future__ import annotations
import io, urllib.request, logging, hashlib
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont, ImageOps

log = logging.getLogger(__name__)

W, H = 1080, 1080
BG = (18, 22, 28)
ACCENT = (105, 230, 165)         # win green
ACCENT_RED = (240, 90, 110)
TEXT = (240, 240, 245)
DIM = (160, 165, 175)
CARD = (32, 38, 48)
CARD_HI = (40, 48, 60)

DAY_RU = {0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс"}
MONTH_RU = ["", "января", "февраля", "марта", "апреля", "мая", "июня",
            "июля", "августа", "сентября", "октября", "ноября", "декабря"]


def _font(size, bold=False):
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ]
    for p in paths:
        try: return ImageFont.truetype(p, size)
        except Exception: pass
    return ImageFont.load_default()


def _fetch_avatar(url: str, size: int):
    if not url:
        return None
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = r.read()
        img = Image.open(io.BytesIO(data)).convert("RGBA")
        img = ImageOps.fit(img, (size, size), Image.LANCZOS)
        mask = Image.new("L", (size, size), 0)
        ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
        img.putalpha(mask)
        return img
    except Exception as e:
        log.warning("avatar fetch failed: %s", e)
        return None


def _initials_avatar(name: str, size: int):
    parts = (name or "?").split()[:2]
    initials = "".join(p[0].upper() for p in parts) or "?"
    h = int(hashlib.md5(name.encode()).hexdigest()[:6], 16)
    r, g, b = (h >> 16) & 255, (h >> 8) & 255, h & 255
    r, g, b = (r + 60) // 2, (g + 80) // 2, (b + 100) // 2
    img = Image.new("RGBA", (size, size), (r, g, b, 255))
    d = ImageDraw.Draw(img)
    f = _font(int(size * 0.42), bold=True)
    bbox = d.textbbox((0, 0), initials, font=f)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    d.text(((size - tw) / 2 - bbox[0], (size - th) / 2 - bbox[1]), initials, fill=TEXT, font=f)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
    img.putalpha(mask)
    return img


def _avatar_circle(p, size):
    name = p.get("full_name") or p.get("name") or "?"
    return _fetch_avatar(p.get("picture", ""), size) or _initials_avatar(name, size)


def _truncate(text, font, max_w, draw):
    if not text:
        return text
    if draw.textlength(text, font=font) <= max_w:
        return text
    while text and draw.textlength(text + "…", font=font) > max_w:
        text = text[:-1]
    return text + "…"


def render_score_image(match: dict, my_user_id: str, output_path: str):
    """Generate a square 1080×1080 PNG of match score, ready for social sharing."""
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)

    # Determine teams (my team first)
    teams = match.get("teams", [])
    my_team_idx = 0
    for i, t in enumerate(teams):
        if any(p.get("user_id") == my_user_id for p in t.get("players", [])):
            my_team_idx = i; break
    team_my = teams[my_team_idx] if teams else {}
    team_opp = teams[1 - my_team_idx] if len(teams) > 1 else {}

    # Compute totals + sets
    my_team_id = team_my.get("team_id")
    sets = []
    my_total = opp_total = 0
    for s in match.get("results", []) or []:
        scores = s.get("scores") or []
        vals = {str(e.get("team_id")): e.get("score") for e in scores
                if isinstance(e, dict) and e.get("score") is not None}
        if not vals: continue
        my = vals.get(str(my_team_id))
        opp = next((v for k, v in vals.items() if k != str(my_team_id)), None)
        if my is None or opp is None: continue
        sets.append((int(my), int(opp)))
        my_total += int(my); opp_total += int(opp)

    won = my_total > opp_total
    draw_d = my_total == opp_total
    accent = ACCENT if won and not draw_d else (DIM if draw_d else ACCENT_RED)

    # ── Header ──
    header_h = 100
    d.rectangle([0, 0, W, header_h], fill=CARD)
    f_brand = _font(34, bold=True)
    d.text((50, 33), "PADEL", fill=accent, font=f_brand)
    f_status = _font(30, bold=True)
    status_text = "ПОБЕДА" if won else ("ПОРАЖЕНИЕ" if my_total < opp_total else "НИЧЬЯ")
    bbox = d.textbbox((0, 0), status_text, font=f_status)
    d.text((W - 50 - (bbox[2] - bbox[0]), 35), status_text, fill=accent, font=f_status)

    # ── Big score in center ──
    score_y = 145
    score_text = "  ".join(f"{a}-{b}" for a, b in sets) or "—"
    f_big = _font(126, bold=True)
    bbox = d.textbbox((0, 0), score_text, font=f_big)
    d.text(((W - (bbox[2] - bbox[0])) // 2, score_y), score_text, fill=TEXT, font=f_big)

    # Sets label below score
    f_label = _font(22)
    if sets:
        sets_str = "   ".join(f"Сет {i+1}" for i in range(len(sets)))
        bbox = d.textbbox((0, 0), sets_str, font=f_label)
        d.text(((W - (bbox[2] - bbox[0])) // 2, score_y + 155), sets_str, fill=DIM, font=f_label)

    # ── Two team blocks ──
    block_y = 360
    block_h = 470
    pad = 40
    block_w = (W - pad * 3) // 2
    avatar_size = 96

    def draw_team(x, team, label_color, label, is_winner):
        # Card with subtle highlight for winner
        d.rounded_rectangle([x, block_y, x + block_w, block_y + block_h],
                            radius=28, fill=(CARD_HI if is_winner else CARD))
        # Top label
        f_lbl = _font(22, bold=True)
        d.text((x + 30, block_y + 26), label, fill=label_color, font=f_lbl)

        # Sets won counter on the right
        if team is team_my:
            n_sets = sum(1 for a, b in sets if a > b)
        else:
            n_sets = sum(1 for a, b in sets if b > a)
        f_sets = _font(36, bold=True)
        sets_lbl = f"{n_sets}"
        bb = d.textbbox((0, 0), sets_lbl, font=f_sets)
        d.text((x + block_w - 30 - (bb[2] - bb[0]), block_y + 22), sets_lbl,
               fill=label_color, font=f_sets)

        # Players (up to 2)
        players = (team or {}).get("players", [])[:2]
        f_name = _font(30, bold=True)
        f_lvl = _font(22)
        rows_top = block_y + 90
        row_h = 128
        for i, p in enumerate(players):
            ay = rows_top + i * row_h
            ax = x + 30
            avatar = _avatar_circle(p, avatar_size)
            img.paste(avatar, (ax, ay), avatar)
            tx = ax + avatar_size + 22
            name = p.get("full_name") or p.get("name") or "?"
            name_short = _truncate(name, f_name, block_w - (tx - x) - 24, d)
            d.text((tx, ay + 18), name_short, fill=TEXT, font=f_name)
            lvl = p.get("level_value")
            lvl_str = f"Рейтинг {lvl:.2f}" if lvl is not None else "Без рейтинга"
            d.text((tx, ay + 58), lvl_str, fill=DIM, font=f_lvl)

        # Bottom strip — average level
        levels = [p.get("level_value") for p in players if p.get("level_value") is not None]
        if levels:
            avg = sum(levels) / len(levels)
            f_avg = _font(22)
            avg_label = "Средний уровень"
            avg_val = f"{avg:.2f}"
            sep_y = block_y + block_h - 56
            d.line([(x + 30, sep_y), (x + block_w - 30, sep_y)], fill=(60, 68, 80), width=2)
            d.text((x + 30, sep_y + 14), avg_label, fill=DIM, font=f_avg)
            f_avg_b = _font(24, bold=True)
            bb = d.textbbox((0, 0), avg_val, font=f_avg_b)
            d.text((x + block_w - 30 - (bb[2] - bb[0]), sep_y + 12), avg_val,
                   fill=label_color, font=f_avg_b)

    draw_team(pad, team_my, accent, "ТВОЯ КОМАНДА", won and not draw_d)
    draw_team(pad * 2 + block_w, team_opp, DIM,
              "СОПЕРНИКИ", (my_total < opp_total) and not draw_d)

    # ── Footer: club + date ──
    foot_y = 880
    club = match.get("location") or "?"
    f_club = _font(32, bold=True)
    club_short = _truncate(club, f_club, W - 100, d)
    bbox = d.textbbox((0, 0), club_short, font=f_club)
    d.text(((W - (bbox[2] - bbox[0])) // 2, foot_y), club_short, fill=TEXT, font=f_club)

    sd = match.get("start_date", "")
    when_str = ""
    try:
        if sd:
            dt = datetime.strptime(sd[:19], "%Y-%m-%dT%H:%M:%S")
            when_str = f"{DAY_RU[dt.weekday()]}, {dt.day} {MONTH_RU[dt.month]} {dt.year}"
    except Exception:
        pass
    if when_str:
        f_d = _font(24)
        bbox = d.textbbox((0, 0), when_str, font=f_d)
        d.text(((W - (bbox[2] - bbox[0])) // 2, foot_y + 50), when_str, fill=DIM, font=f_d)

    img.save(output_path, "PNG", optimize=True)
    return output_path
