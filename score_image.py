"""Generate a shareable match score image (PNG) for social media.

Layout: 1080×1080 square (universal Instagram / VK / Telegram / Twitter).
Style inspired by tennis broadcast scoreboards — column-per-set with the
winning set underlined in the team's accent color.
"""
from __future__ import annotations
import io, urllib.request, logging, hashlib
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageFilter

log = logging.getLogger(__name__)

W, H = 1080, 1080

# Padel-inspired palette
BG_TOP = (12, 30, 22)        # very dark green
BG_BOT = (8, 14, 12)
ACCENT = (0, 230, 122)       # padel neon green
ACCENT_DIM = (0, 140, 80)
GOLD = (240, 200, 90)
RED = (240, 90, 110)
TEXT = (245, 248, 245)
DIM = (150, 168, 158)
CARD = (24, 36, 30)
LINE = (60, 78, 70)

DAY_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
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


def _vertical_gradient(size, top, bottom):
    img = Image.new("RGB", size, top)
    d = ImageDraw.Draw(img)
    for y in range(size[1]):
        t = y / max(1, size[1] - 1)
        r = int(top[0] * (1 - t) + bottom[0] * t)
        g = int(top[1] * (1 - t) + bottom[1] * t)
        b = int(top[2] * (1 - t) + bottom[2] * t)
        d.line([(0, y), (size[0], y)], fill=(r, g, b))
    return img


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
    r, g, b = (r + 30) // 2, (g + 70) // 2, (b + 50) // 2
    img = Image.new("RGBA", (size, size), (r, g, b, 255))
    d = ImageDraw.Draw(img)
    f = _font(int(size * 0.42), bold=True)
    bbox = d.textbbox((0, 0), initials, font=f)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    d.text(((size - tw) / 2 - bbox[0], (size - th) / 2 - bbox[1]),
           initials, fill=TEXT, font=f)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
    img.putalpha(mask)
    return img


def _avatar_circle(p, size):
    name = p.get("full_name") or p.get("name") or "?"
    return _fetch_avatar(p.get("picture", ""), size) or _initials_avatar(name, size)


def _ring(img, x, y, size, color, width=4):
    """Draw a colored ring border around an avatar slot."""
    d = ImageDraw.Draw(img)
    d.ellipse([x - width, y - width, x + size + width, y + size + width],
              outline=color, width=width)


def _truncate(text, font, max_w, draw):
    if not text: return text
    if draw.textlength(text, font=font) <= max_w: return text
    while text and draw.textlength(text + "…", font=font) > max_w:
        text = text[:-1]
    return (text + "…") if text else ""


def _diagonal_band(img, y0, y1, color, slope=80):
    """Decorative diagonal band across the canvas."""
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(overlay)
    pts = [(0, y0), (img.size[0], y0 - slope),
           (img.size[0], y1 - slope), (0, y1)]
    d.polygon(pts, fill=color)
    img.alpha_composite(overlay) if img.mode == "RGBA" else img.paste(
        Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB"))


def render_score_image(match: dict, my_user_id: str, output_path: str):
    """Generate a 1080×1080 PNG of the match score, ready for social posts."""
    base = _vertical_gradient((W, H), BG_TOP, BG_BOT).convert("RGBA")

    # Decorative geometric stripes (subtle)
    deco = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    dd = ImageDraw.Draw(deco)
    # top-right thin neon slashes
    for i, alpha in enumerate([90, 60, 35]):
        off = i * 26
        dd.polygon([(W - 220 - off, 0), (W - 80 - off, 0),
                    (W - 60 - off, 140), (W - 200 - off, 140)],
                   fill=ACCENT + (alpha,))
    # bottom-left thin gold slashes
    for i, alpha in enumerate([70, 45, 25]):
        off = i * 22
        dd.polygon([(40 + off, H), (200 + off, H),
                    (220 + off, H - 110), (60 + off, H - 110)],
                   fill=GOLD + (alpha,))
    base = Image.alpha_composite(base, deco)

    img = base
    d = ImageDraw.Draw(img)

    # ── Determine teams (my team first) ──
    teams = match.get("teams", [])
    my_team_idx = 0
    for i, t in enumerate(teams):
        if any(p.get("user_id") == my_user_id for p in t.get("players", [])):
            my_team_idx = i; break
    team_my = teams[my_team_idx] if teams else {}
    team_opp = teams[1 - my_team_idx] if len(teams) > 1 else {}

    # ── Compute sets ──
    my_team_id = team_my.get("team_id")
    sets = []           # list of (my, opp)
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
    accent = ACCENT if won and not draw_d else (GOLD if draw_d else RED)

    # ── Top header ──
    f_brand = _font(36, bold=True)
    f_meta = _font(22, bold=True)
    f_status = _font(38, bold=True)

    # PADEL wordmark with neon dot
    d.ellipse([54, 60, 74, 80], fill=accent)
    d.text((90, 56), "PADEL", fill=TEXT, font=f_brand)

    # Status pill (top-right)
    status_text = "ПОБЕДА" if won else ("ПОРАЖЕНИЕ" if my_total < opp_total else "НИЧЬЯ")
    bb = d.textbbox((0, 0), status_text, font=f_status)
    pill_w = (bb[2] - bb[0]) + 60
    pill_h = 64
    pill_x = W - 60 - pill_w
    pill_y = 50
    d.rounded_rectangle([pill_x, pill_y, pill_x + pill_w, pill_y + pill_h],
                        radius=pill_h // 2, fill=accent)
    d.text((pill_x + 30, pill_y + 10), status_text, fill=BG_TOP, font=f_status)

    # ── Tournament-style scoreboard ──
    sb_top = 200
    sb_h = 620
    sb_x = 60
    sb_w = W - 120
    sb_bottom = sb_top + sb_h
    d.rounded_rectangle([sb_x, sb_top, sb_x + sb_w, sb_bottom],
                        radius=32, fill=CARD)

    # Inner column layout: name area | set columns | total
    name_pad = 32
    name_x = sb_x + name_pad
    n_sets = max(1, len(sets))
    # Right side: set columns + total column
    total_col_w = 96
    # Set columns width adapts to number of sets
    if n_sets <= 2:
        set_col_w = 110
    elif n_sets == 3:
        set_col_w = 100
    else:
        set_col_w = 88
    set_area_w = set_col_w * n_sets
    total_x = sb_x + sb_w - name_pad - total_col_w
    set_area_x = total_x - 14 - set_area_w

    # Set headers row
    f_hdr = _font(22, bold=True)
    hdr_y = sb_top + 24
    for i in range(n_sets):
        cx = set_area_x + i * set_col_w + set_col_w // 2
        lbl = f"СЕТ {i + 1}"
        bb = d.textbbox((0, 0), lbl, font=f_hdr)
        d.text((cx - (bb[2] - bb[0]) // 2, hdr_y), lbl, fill=DIM, font=f_hdr)
    bb = d.textbbox((0, 0), "ИТОГ", font=f_hdr)
    d.text((total_x + total_col_w // 2 - (bb[2] - bb[0]) // 2, hdr_y),
           "ИТОГ", fill=accent, font=f_hdr)

    # Two rows: my team (top), opp (bottom)
    row_top_y = sb_top + 90
    row_h = (sb_h - 90 - 60) // 2   # 60 reserved for footer strip
    avatar_size = 84
    row_centers = [row_top_y + row_h // 2, row_top_y + row_h + row_h // 2]

    # Mid-row separator
    d.line([(sb_x + name_pad, row_top_y + row_h),
            (sb_x + sb_w - name_pad, row_top_y + row_h)],
           fill=LINE, width=2)

    f_name = _font(26, bold=True)
    f_lvl = _font(20)
    f_team_lbl = _font(18, bold=True)
    f_score = _font(64, bold=True)
    f_total = _font(64, bold=True)

    def draw_team_row(team, is_mine, row_idx):
        cy = row_centers[row_idx]
        players = (team or {}).get("players", [])[:2]

        # Pair of avatars side-by-side, slightly overlapping
        ax = name_x
        overlap = 22
        avatars_w = avatar_size + (avatar_size - overlap) * (max(1, len(players)) - 1)
        ring_color = accent if is_mine else DIM
        for i, p in enumerate(players):
            px = ax + i * (avatar_size - overlap)
            py = cy - avatar_size // 2
            av = _avatar_circle(p, avatar_size)
            img.paste(av, (px, py), av)
            _ring(img, px, py, avatar_size, ring_color, width=3)

        # Names + levels block (right of avatars)
        text_x = ax + avatars_w + 22
        text_max_w = (set_area_x - 16) - text_x

        # Team label
        team_lbl = "ТВОЯ КОМАНДА" if is_mine else "СОПЕРНИКИ"
        d.text((text_x, cy - avatar_size // 2 - 2), team_lbl,
               fill=accent if is_mine else DIM, font=f_team_lbl)

        # Names: stacked vertically (max 2)
        for i, p in enumerate(players):
            name = p.get("full_name") or p.get("name") or "?"
            lvl = p.get("level_value")
            name_short = _truncate(name, f_name, text_max_w - 80, d)
            d.text((text_x, cy - avatar_size // 2 + 26 + i * 36), name_short,
                   fill=TEXT, font=f_name)
            if lvl is not None:
                lvl_str = f"{lvl:.2f}"
                bb_l = d.textbbox((0, 0), lvl_str, font=f_lvl)
                d.text((text_x + text_max_w - (bb_l[2] - bb_l[0]),
                        cy - avatar_size // 2 + 30 + i * 36),
                       lvl_str, fill=DIM, font=f_lvl)

    draw_team_row(team_my, True, 0)
    draw_team_row(team_opp, False, 1)

    # Set scores per row
    for i, (a, b) in enumerate(sets):
        cx = set_area_x + i * set_col_w + set_col_w // 2
        # MY team set
        my_color = accent if a > b else TEXT
        s_text = str(a)
        bb = d.textbbox((0, 0), s_text, font=f_score)
        sx = cx - (bb[2] - bb[0]) // 2
        sy = row_centers[0] - (bb[3] - bb[1]) // 2 - bb[1]
        d.text((sx, sy), s_text, fill=my_color, font=f_score)
        if a > b:
            # Underline winning set
            ul_w = max(36, (bb[2] - bb[0]) + 20)
            d.rounded_rectangle([cx - ul_w // 2, sy + (bb[3] - bb[1]) + 14,
                                 cx + ul_w // 2, sy + (bb[3] - bb[1]) + 20],
                                radius=3, fill=accent)
        # OPP team set
        opp_color = TEXT if a > b else (DIM if a == b else TEXT)
        s_text = str(b)
        bb = d.textbbox((0, 0), s_text, font=f_score)
        sx = cx - (bb[2] - bb[0]) // 2
        sy = row_centers[1] - (bb[3] - bb[1]) // 2 - bb[1]
        d.text((sx, sy), s_text, fill=opp_color, font=f_score)
        if b > a:
            ul_w = max(36, (bb[2] - bb[0]) + 20)
            d.rounded_rectangle([cx - ul_w // 2, sy + (bb[3] - bb[1]) + 14,
                                 cx + ul_w // 2, sy + (bb[3] - bb[1]) + 20],
                                radius=3, fill=DIM)

    # Total column — number of sets won
    my_sets_won = sum(1 for a, b in sets if a > b)
    opp_sets_won = sum(1 for a, b in sets if b > a)

    # Vertical separator before total
    d.line([(total_x - 8, row_top_y + 10),
            (total_x - 8, sb_top + sb_h - 80)], fill=LINE, width=2)

    cx = total_x + total_col_w // 2
    for row_idx, (val, is_mine) in enumerate([(my_sets_won, True),
                                              (opp_sets_won, False)]):
        s = str(val)
        bb = d.textbbox((0, 0), s, font=f_total)
        sx = cx - (bb[2] - bb[0]) // 2
        sy = row_centers[row_idx] - (bb[3] - bb[1]) // 2 - bb[1]
        col = accent if is_mine and won else (RED if not is_mine and not won and not draw_d else (DIM if draw_d else TEXT))
        d.text((sx, sy), s, fill=col, font=f_total)

    # ── Footer strip inside the card: club + average levels ──
    f_avg_lbl = _font(18)
    f_avg_val = _font(22, bold=True)
    foot_y = sb_top + sb_h - 56

    def avg_level(team):
        lvls = [p.get("level_value") for p in (team or {}).get("players", [])
                if p.get("level_value") is not None]
        return (sum(lvls) / len(lvls)) if lvls else None

    a_my = avg_level(team_my)
    a_opp = avg_level(team_opp)
    line_y = foot_y - 8
    d.line([(sb_x + name_pad, line_y), (sb_x + sb_w - name_pad, line_y)],
           fill=LINE, width=2)
    if a_my is not None:
        d.text((sb_x + name_pad, foot_y + 4),
               f"Средний уровень твоей команды", fill=DIM, font=f_avg_lbl)
        d.text((sb_x + name_pad + 320, foot_y),
               f"{a_my:.2f}", fill=accent, font=f_avg_val)
    if a_opp is not None:
        right_lbl = "Средний уровень соперников"
        bb1 = d.textbbox((0, 0), right_lbl, font=f_avg_lbl)
        val_str = f"{a_opp:.2f}"
        bb2 = d.textbbox((0, 0), val_str, font=f_avg_val)
        block_w = (bb1[2] - bb1[0]) + 12 + (bb2[2] - bb2[0])
        rx = sb_x + sb_w - name_pad - block_w
        d.text((rx, foot_y + 4), right_lbl, fill=DIM, font=f_avg_lbl)
        d.text((rx + (bb1[2] - bb1[0]) + 12, foot_y),
               val_str, fill=DIM, font=f_avg_val)

    # ── Bottom: club + date ──
    bottom_y = sb_bottom + 30
    club = match.get("location") or "?"
    f_club = _font(34, bold=True)
    club_short = _truncate(club, f_club, W - 120, d)
    bb = d.textbbox((0, 0), club_short, font=f_club)
    d.text(((W - (bb[2] - bb[0])) // 2, bottom_y), club_short,
           fill=TEXT, font=f_club)

    sd = match.get("start_date", "")
    when_str = ""
    try:
        if sd:
            dt = datetime.strptime(sd[:19], "%Y-%m-%dT%H:%M:%S")
            when_str = f"{DAY_RU[dt.weekday()]} · {dt.day} {MONTH_RU[dt.month]} {dt.year}"
    except Exception:
        pass
    if when_str:
        f_d = _font(22)
        bb = d.textbbox((0, 0), when_str, font=f_d)
        d.text(((W - (bb[2] - bb[0])) // 2, bottom_y + 50),
               when_str, fill=DIM, font=f_d)

    img.convert("RGB").save(output_path, "PNG", optimize=True)
    return output_path
