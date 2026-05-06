"""Модуль мониторинга рейтинга и построения графика динамики.

Источник данных: Playtomic /v1/matches?user_id=...
В каждом матче в массиве teams[].players[] для user_id=... поле level_value содержит
рейтинг игрока на момент матча. Берём по дате старта.
"""
from __future__ import annotations
import urllib.request, urllib.error, json, logging
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)

PT_BASE = "https://api.playtomic.io/v1"


def fetch_user_matches(pt_id: str) -> list:
    url = f"{PT_BASE}/matches?sport_id=PADEL&user_id={pt_id}&size=100"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning("fetch_user_matches failed: %s", e)
        return []


def history_from_matches(matches: list, pt_id: str) -> list:
    """Возвращает [(date, level_value), ...] отсортированный по дате (asc)."""
    items = []
    for m in matches:
        sd = m.get("start_date", "")[:10]
        if not sd:
            continue
        for team in m.get("teams", []):
            for p in team.get("players", []):
                if p.get("user_id") == pt_id and p.get("level_value") is not None:
                    items.append((sd, float(p["level_value"])))
                    break
    items.sort()
    # Убираем дубли в один день — берём последний рейтинг
    by_day = {}
    for d, lv in items:
        by_day[d] = lv
    return sorted(by_day.items())


def current_level(history: list) -> Optional[float]:
    """Самое свежее значение."""
    if not history:
        return None
    return history[-1][1]


def render_rating_pdf(history: list, pt_id: str, output_path: str, name: str = "Player"):
    """Простой график динамики рейтинга. Возвращает len(history)."""
    if not history:
        return 0
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas

    PAGE_W, PAGE_H = landscape(A4)
    M = 18 * mm
    c = canvas.Canvas(output_path, pagesize=landscape(A4))

    # Заголовок
    c.setFillColorRGB(0.1, 0.1, 0.15)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(M, PAGE_H - M, f"Рейтинг — {name}")
    cur = history[-1][1]
    first = history[0][1]
    delta = cur - first
    c.setFont("Helvetica", 11)
    c.setFillColorRGB(0.4, 0.4, 0.5)
    period = f"{history[0][0]} … {history[-1][0]}  ·  {len(history)} точек"
    delta_str = f"{'+' if delta >= 0 else ''}{delta:.2f}"
    c.drawString(M, PAGE_H - M - 16, f"{period}  ·  Текущий: {cur:.2f}  ·  Изменение: {delta_str}")

    # График
    chart_top = PAGE_H - M - 40
    chart_bottom = M + 30
    chart_left = M + 30
    chart_right = PAGE_W - M
    chart_w = chart_right - chart_left
    chart_h = chart_top - chart_bottom

    # Оси
    levels = [lv for _, lv in history]
    lv_min, lv_max = min(levels), max(levels)
    if lv_max - lv_min < 0.5:
        pad = 0.25
    else:
        pad = (lv_max - lv_min) * 0.15
    y_min = max(0, lv_min - pad)
    y_max = lv_max + pad

    dates = [datetime.strptime(d, "%Y-%m-%d") for d, _ in history]
    x_min = dates[0].timestamp()
    x_max = dates[-1].timestamp()
    if x_max == x_min:
        x_max = x_min + 86400

    def to_xy(dt, lv):
        x = chart_left + (dt.timestamp() - x_min) / (x_max - x_min) * chart_w
        y = chart_bottom + (lv - y_min) / (y_max - y_min) * chart_h
        return x, y

    # Сетка
    c.setStrokeColorRGB(0.85, 0.85, 0.9)
    c.setLineWidth(0.5)
    c.setFont("Helvetica", 8)
    c.setFillColorRGB(0.5, 0.5, 0.6)
    n_y_ticks = 5
    for i in range(n_y_ticks + 1):
        v = y_min + (y_max - y_min) * i / n_y_ticks
        y = chart_bottom + chart_h * i / n_y_ticks
        c.line(chart_left, y, chart_right, y)
        c.drawRightString(chart_left - 4, y - 3, f"{v:.1f}")

    # Линия тренда
    c.setStrokeColorRGB(0.10, 0.40, 0.85)
    c.setLineWidth(1.8)
    p = c.beginPath()
    for i, (d, lv) in enumerate(history):
        dt = datetime.strptime(d, "%Y-%m-%d")
        x, y = to_xy(dt, lv)
        if i == 0:
            p.moveTo(x, y)
        else:
            p.lineTo(x, y)
    c.drawPath(p, stroke=1, fill=0)

    # Точки
    c.setFillColorRGB(0.10, 0.40, 0.85)
    for d, lv in history:
        dt = datetime.strptime(d, "%Y-%m-%d")
        x, y = to_xy(dt, lv)
        c.circle(x, y, 1.8, stroke=0, fill=1)

    # Подписи дат — несколько ключевых точек
    c.setFillColorRGB(0.45, 0.45, 0.55)
    c.setFont("Helvetica", 7.5)
    n_labels = min(8, len(history))
    step = max(1, len(history) // n_labels)
    for i in range(0, len(history), step):
        d, lv = history[i]
        dt = datetime.strptime(d, "%Y-%m-%d")
        x, y = to_xy(dt, lv)
        c.drawCentredString(x, chart_bottom - 12, dt.strftime("%d.%m.%y"))

    # Подсветка max/min
    max_idx = max(range(len(history)), key=lambda i: history[i][1])
    min_idx = min(range(len(history)), key=lambda i: history[i][1])
    for idx, color, lbl in [(max_idx, (0.20, 0.65, 0.30), "max"), (min_idx, (0.85, 0.30, 0.30), "min")]:
        d, lv = history[idx]
        dt = datetime.strptime(d, "%Y-%m-%d")
        x, y = to_xy(dt, lv)
        c.setFillColorRGB(*color)
        c.circle(x, y, 3.2, stroke=0, fill=1)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(x + 5, y + 5, f"{lbl} {lv:.2f}")

    c.setFillColorRGB(0.7, 0.7, 0.75)
    c.setFont("Helvetica", 7)
    c.drawCentredString(PAGE_W / 2, M - 5,
                        f"Источник: Playtomic. Обновлено {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    c.showPage(); c.save()
    return len(history)
