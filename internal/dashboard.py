# /mnt/data/dashboard.py
# -*- coding: utf-8 -*-
"""
Dashboard TV (Flet) - Enhanced Edition v2.

Mejoras visuales implementadas:
  IDEA 1: Carrusel de últimas 5 exhibiciones con cross-fade
  IDEA 2: Flechas animadas de cambio de posición en ranking
  IDEA 3: Efecto "Nuevo Líder" con confetti y banner
  + Flash minimalista, loop 60 FPS, lógica ranking APR/DEST separadas.
"""

import asyncio
import base64
import inspect
import os
import sys
import threading
import time
import random
import math
from datetime import datetime
from queue import SimpleQueue, Empty
from typing import Any, Dict, List, Optional, Sequence

import flet as ft


# -------------------------
# PATHS / IMPORTS SEGUROS
# -------------------------
def _setup_sys_path() -> None:
    if getattr(sys, "frozen", False):
        base = os.path.dirname(sys.executable)
    else:
        here = os.path.dirname(os.path.abspath(__file__))
        base = os.path.dirname(here) if os.path.basename(here).lower() == "src" else here

    candidates = [
        base,
        os.path.join(base, "src"),
        os.path.join(base, "CONFIG_GLOBAL"),
        os.path.join(base, "..", "internal"),
        os.path.join(base, "..", "internal", "src"),
        os.path.join(base, "..", "internal", "CONFIG_GLOBAL"),
    ]
    for p in candidates:
        if p and os.path.exists(p) and p not in sys.path:
            sys.path.insert(0, p)


_setup_sys_path()

try:
    from sheets_manager import SheetsManager
    from config_manager import ConfigManager
except Exception:
    try:
        from src.sheets_manager import SheetsManager
        from CONFIG_GLOBAL.config_manager import ConfigManager
    except Exception:
        SheetsManager = ConfigManager = None

try:
    from logger_config import get_logger

    logger = get_logger(__name__)
except Exception:
    import logging

    logger = logging.getLogger("Dashboard")


# -------------------------
# HELPERS DE COMPAT
# -------------------------
def _sig_params(obj: Any) -> set[str]:
    try:
        return set(inspect.signature(obj).parameters.keys())
    except Exception:
        try:
            return set(inspect.signature(obj.__init__).parameters.keys())
        except Exception:
            return set()


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _fw_bold() -> Any:
    FW = _safe_getattr(ft, "FontWeight")
    if FW and hasattr(FW, "BOLD"):
        return FW.BOLD
    return "bold"


def _fw_w600() -> Any:
    FW = _safe_getattr(ft, "FontWeight")
    for name in ("W_600", "W600", "SEMIBOLD"):
        if FW and hasattr(FW, name):
            return getattr(FW, name)
    return "bold"


def _align_center() -> Any:
    al = _safe_getattr(ft, "alignment")
    if al and hasattr(al, "center"):
        return al.center
    return ft.Alignment(0, 0)


def _align_center_left() -> Any:
    al = _safe_getattr(ft, "alignment")
    if al and hasattr(al, "center_left"):
        return al.center_left
    return ft.Alignment(-1, 0)


def _image_fit_contain() -> Any:
    IF = _safe_getattr(ft, "ImageFit")
    if IF and hasattr(IF, "CONTAIN"):
        return IF.CONTAIN
    return "contain"


def _scroll_auto() -> Any:
    SM = _safe_getattr(ft, "ScrollMode")
    if SM and hasattr(SM, "AUTO"):
        return SM.AUTO
    return "auto"


def _padding_symmetric(horizontal: float, vertical: float) -> Any:
    Padding = _safe_getattr(ft, "Padding")
    if Padding is not None and hasattr(Padding, "symmetric"):
        try:
            return Padding.symmetric(horizontal=horizontal, vertical=vertical)
        except Exception:
            try:
                return Padding.symmetric(horizontal, vertical)
            except Exception:
                pass

    padding_mod = _safe_getattr(ft, "padding")
    if padding_mod is not None and hasattr(padding_mod, "symmetric"):
        try:
            return padding_mod.symmetric(horizontal, vertical)
        except Exception:
            try:
                return padding_mod.symmetric(horizontal=horizontal, vertical=vertical)
            except Exception:
                pass

    return None


def _border_all(width: float, color: Any) -> Any:
    Border = _safe_getattr(ft, "Border")
    if Border is not None and hasattr(Border, "all"):
        try:
            return Border.all(width=width, color=color)
        except Exception:
            try:
                return Border.all(width, color)
            except Exception:
                pass

    border_mod = _safe_getattr(ft, "border")
    if border_mod is not None and hasattr(border_mod, "all"):
        try:
            return border_mod.all(width, color)
        except Exception:
            try:
                return border_mod.all(width=width, color=color)
            except Exception:
                pass

    return None


# -------------------------
# OPACIDAD ROBUSTA
# -------------------------
def _hex_with_opacity(opacity: float, hex_color: str) -> str:
    c = (hex_color or "").strip()
    if not c.startswith("#"):
        return hex_color
    h = c[1:]
    if len(h) == 6:
        try:
            a = max(0, min(255, int(round(opacity * 255))))
            return f"#{a:02X}{h.upper()}"
        except Exception:
            return hex_color
    if len(h) == 8:
        try:
            a = max(0, min(255, int(round(opacity * 255))))
            return f"#{a:02X}{h[2:].upper()}"
        except Exception:
            return hex_color
    return hex_color


def _op(opacity: float, color: str) -> str:
    try:
        if hasattr(ft, "colors") and hasattr(ft.colors, "with_opacity"):
            v = ft.colors.with_opacity(opacity, color)
            if isinstance(v, str) and v:
                return v
    except Exception:
        pass

    if isinstance(color, str) and color.startswith("#"):
        return _hex_with_opacity(opacity, color)

    return color


# -------------------------
# ICONOS
# -------------------------
def _icons_namespace() -> Any:
    return _safe_getattr(ft, "icons") or _safe_getattr(ft, "Icons")


_ICONS_NS = _icons_namespace()

_ICON_CANDIDATES: Dict[str, Sequence[str]] = {
    "FULLSCREEN": ("FULLSCREEN", "OPEN_IN_FULL", "ASPECT_RATIO"),
    "CHECK_CIRCLE": ("CHECK_CIRCLE", "CHECK_CIRCLE_OUTLINED", "CHECK", "DONE"),
    "WHATSHOT": ("WHATSHOT", "LOCAL_FIRE_DEPARTMENT", "FIREPLACE"),
    "CANCEL": ("CANCEL", "HIGHLIGHT_OFF", "CLOSE"),
    "TODAY": ("TODAY", "EVENT", "DATE_RANGE"),
    "CALENDAR_MONTH": ("CALENDAR_MONTH", "CALENDAR_TODAY", "EVENT_NOTE"),
    "HISTORY": ("HISTORY", "SCHEDULE", "ACCESS_TIME"),
    "EMOJI_EVENTS": ("EMOJI_EVENTS", "WORKSPACE_PREMIUM", "MILITARY_TECH", "STAR"),
    "FORMAT_QUOTE": ("FORMAT_QUOTE", "FORMAT_QUOTE_OUTLINED"),
    "STORE": ("STORE", "STORE_MALL_DIRECTORY", "SHOP"),
    "PERSON": ("PERSON", "ACCOUNT_CIRCLE", "PERSON_OUTLINE"),
    "ACCESS_TIME": ("ACCESS_TIME", "SCHEDULE", "WATCH_LATER"),
}

_ICON_EMOJI: Dict[str, str] = {
    "FULLSCREEN": "⛶",
    "CHECK_CIRCLE": "✓",
    "WHATSHOT": "🔥",
    "CANCEL": "✖",
    "TODAY": "📅",
    "CALENDAR_MONTH": "🗓️",
    "HISTORY": "🕘",
    "EMOJI_EVENTS": "🏆",
    "FORMAT_QUOTE": "❝",
    "STORE": "🏪",
    "PERSON": "👤",
    "ACCESS_TIME": "⏰",
}


def _icon_data(*names: str) -> Optional[Any]:
    ns = _ICONS_NS
    if not ns:
        return None
    for n in names:
        if not n:
            continue
        try:
            if hasattr(ns, n):
                return getattr(ns, n)
        except Exception:
            continue
    return None


def _icon_control(
    key: str,
    *,
    color: Optional[str] = None,
    size: Optional[int] = None,
) -> Optional[ft.Control]:
    candidates = _ICON_CANDIDATES.get(key, (key,))
    icon = _icon_data(*candidates)
    if icon is None:
        return None

    params = _sig_params(ft.Icon)
    kwargs: Dict[str, Any] = {}
    if "color" in params and color is not None:
        kwargs["color"] = color
    if "size" in params and size is not None:
        kwargs["size"] = size
    try:
        return ft.Icon(icon, **kwargs)
    except Exception:
        return None


def _guaranteed_icon(
    key: str,
    *,
    color: str,
    size: int,
    fallback_text: str,
) -> ft.Control:
    icon = _icon_control(key, color=color, size=size)
    if icon is not None:
        return icon
    return ft.Text(fallback_text, size=size, color=color, weight=_fw_bold())


# -------------------------
# ASSET LOADING
# -------------------------
_ASSET_CACHE: Dict[str, Optional[str]] = {}


def _load_asset_b64(name: str) -> Optional[str]:
    """Load a PNG from assets directories, return base64 or None."""
    if name in _ASSET_CACHE:
        return _ASSET_CACHE[name]

    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "assets", name),
        os.path.join(here, "..", "assets", name),
        os.path.join(here, "..", "internal", "assets", name),
    ]
    if getattr(sys, "frozen", False):
        exe_dir = os.path.dirname(sys.executable)
        candidates.insert(0, os.path.join(exe_dir, "assets", name))
        candidates.insert(1, os.path.join(exe_dir, "internal", "assets", name))

    for p in candidates:
        try:
            if os.path.exists(p):
                with open(p, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                _ASSET_CACHE[name] = b64
                return b64
        except Exception:
            continue

    _ASSET_CACHE[name] = None
    return None


def _asset_image(name: str, width: int, height: int) -> Optional[ft.Image]:
    """Create an ft.Image from an asset PNG. Returns None if not found."""
    b64 = _load_asset_b64(name)
    if not b64:
        return None
    return _make_image_for_b64(b64, width=width, height=height)


def _asset_or_fallback(
    name: str,
    width: int,
    height: int,
    fallback_text: str,
    fallback_color: str,
) -> ft.Control:
    """Load asset image or return text fallback."""
    img = _asset_image(name, width, height)
    if img is not None:
        return img
    return ft.Text(
        fallback_text,
        size=min(width, height),
        color=fallback_color,
        weight=_fw_bold(),
    )


# -------------------------
# UI HELPERS
# -------------------------
BG = "#0B1220"
PANEL = "#121B2E"
PANEL2 = "#0F172A"
BORDER = "#22304C"
TXT = "#F8FAFC"
MUTED = "#94A3B8"
GREEN = "#22C55E"
CYAN = "#22D3EE"
RED = "#EF4444"
BLUE = "#3B82F6"
AMBER = "#FCD34D"
PURPLE = "#8B5CF6"
MAGENTA = "#EC4899"
BRONZE = "#F97316"
GOLD_GLOW = "#F59E0B"

TOP = 15
REFRESH = 60
TICK_MS = 16
CAROUSEL_INTERVAL = 12.0
CAROUSEL_FADE_DUR = 0.8
CONFETTI_COUNT = 70
CONFETTI_DURATION = 3.0
LEADER_BANNER_DUR = 5.0
ARROW_BOUNCE_DUR = 0.4


def _vsep(*, h: int = 18) -> ft.Control:
    return ft.Container(width=1, height=h, bgcolor=_op(0.18, "#FFFFFF"))


# -------------------------
# IMAGE HELPERS
# -------------------------
def _pad_b64(s: str) -> str:
    s = s.strip()
    if not s:
        return s
    return s + "=" * (-len(s) % 4)


def _b64_to_data_url(b64: str) -> str:
    b64 = (b64 or "").strip()
    if not b64:
        return ""
    if b64.startswith("data:"):
        return b64
    mime = "image/jpeg"
    try:
        head_b64 = _pad_b64(b64[:120])
        head = base64.b64decode(head_b64, validate=False)
        if head.startswith(b"\x89PNG\r\n\x1a\n"):
            mime = "image/png"
        elif head.startswith(b"\xff\xd8\xff"):
            mime = "image/jpeg"
        elif head.startswith(b"GIF87a") or head.startswith(b"GIF89a"):
            mime = "image/gif"
        elif head.startswith(b"RIFF") and b"WEBP" in head[8:16]:
            mime = "image/webp"
    except Exception:
        pass
    return f"data:{mime};base64,{b64}"


def _make_image_for_b64(b64: str, *, width: int, height: int) -> ft.Image:
    img_params = _sig_params(ft.Image)
    kwargs: Dict[str, Any] = {}
    if "width" in img_params:
        kwargs["width"] = width
    if "height" in img_params:
        kwargs["height"] = height
    if "fit" in img_params:
        kwargs["fit"] = _image_fit_contain()

    data_url = _b64_to_data_url(b64)

    if "src_base64" in img_params:
        kwargs["src_base64"] = b64
        return ft.Image(**kwargs)

    kwargs["src"] = data_url
    return ft.Image(**kwargs)


def _toggle_fullscreen(page: ft.Page) -> None:
    try:
        win = _safe_getattr(page, "window")
        if win is not None and hasattr(win, "full_screen"):
            current = bool(getattr(win, "full_screen", False))
            setattr(win, "full_screen", not current)
            return
    except Exception:
        pass

    try:
        current = bool(getattr(page, "window_full_screen", False))
        setattr(page, "window_full_screen", not current)
    except Exception:
        pass


def _make_fullscreen_button(page: ft.Page, *, icon_color: str) -> ft.Control:
    icon_data_val = _icon_data(*_ICON_CANDIDATES["FULLSCREEN"])
    on_click = lambda _: (_toggle_fullscreen(page), page.update())

    if icon_data_val is not None:
        params = _sig_params(ft.IconButton)
        kwargs: Dict[str, Any] = {}
        if "icon" in params:
            kwargs["icon"] = icon_data_val
        if "icon_color" in params:
            kwargs["icon_color"] = icon_color
        if "on_click" in params:
            kwargs["on_click"] = on_click
        try:
            return ft.IconButton(**kwargs)
        except Exception:
            pass

    return ft.Container(
        content=ft.Text(_ICON_EMOJI.get("FULLSCREEN", "⛶"), size=18, color=icon_color),
        border_radius=999,
        padding=_padding_symmetric(10, 6),
        alignment=_align_center(),
        on_click=on_click,
    )


# -------------------------
# DATA HELPERS
# -------------------------
def _parse_date(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s.strip(), "%d/%m/%Y") if s else None
    except Exception:
        return None


def _upper(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{str(k).upper().strip(): v for k, v in r.items()} for r in rows]


def _get(row: Dict[str, Any], *keys: str, d: str = "-") -> str:
    for k in keys:
        v = row.get(k)
        if v and str(v).strip():
            return str(v).strip()
    return d


def _month(d: datetime) -> tuple[int, int]:
    return (d.year, d.month)


def _last_ok(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for r in reversed(rows):
        st = str(r.get("ESTADO_AUDITORIA", "")).strip()
        if st in ("Aprobado", "Destacado"):
            return r
    return None


def _last_n_ok(rows: List[Dict[str, Any]], n: int = 5) -> List[Dict[str, Any]]:
    """Return last N approved/destacado rows (most recent last)."""
    ok_rows = []
    for r in rows:
        st = str(r.get("ESTADO_AUDITORIA", "")).strip()
        if st in ("Aprobado", "Destacado"):
            ok_rows.append(r)
    return ok_rows[-n:] if len(ok_rows) > n else ok_rows


def _resolve_image_src(
    row: Dict[str, Any],
    sheets_mgr: Any,
    img_cache: Dict[str, str],
) -> str:
    """Resolve the display-ready image src for a row. Safe for background thread."""
    uuid = _get(row, "UUID_REF")
    cached = img_cache.get(uuid)
    if cached:
        return cached

    link = _get(row, "LINK_FOTO", "FOTO", "URL_FOTO", d="").strip()

    if not link or not link.startswith("http"):
        return ""

    if "drive.google.com" in link and sheets_mgr:
        try:
            b64 = sheets_mgr.get_image_data_base64(link)
            if b64:
                src = _b64_to_data_url(b64)
                img_cache[uuid] = src
                return src
        except Exception:
            pass

        fid = ""
        if "/d/" in link:
            fid = link.split("/d/")[1].split("/")[0]
        elif "id=" in link:
            fid = link.split("id=")[1].split("&")[0]
        src = f"https://drive.google.com/uc?export=download&id={fid}" if fid else link
        img_cache[uuid] = src
        return src

    img_cache[uuid] = link
    return link


# ========================
# MAIN
# ========================
def main(page: ft.Page) -> None:
    page.title = "Dashboard TV"
    page.bgcolor = BG
    page.padding = 18
    try:
        page.theme_mode = "dark"
    except Exception:
        pass

    # -------------------------
    # STATE
    # -------------------------
    state: Dict[str, Any] = {
        "periodo": "mes",
        "uuid": None,
        "count": 0,
        "current_status": "Normal",
        "refresh_started": time.monotonic(),
        "last_cycle_pos": 0.0,
        "refresh_inflight": False,
        "pulse_started": None,
        "last_badge_second": None,
        "stop": False,
        # IDEA 1 - Carrusel
        "carousel_items": [],
        "carousel_index": 0,
        "carousel_last_switch": time.monotonic(),
        "carousel_fading": False,
        "carousel_fade_start": None,
        # IDEA 2 - Flechas de posición
        "prev_rank_map": {},
        "rank_changes": {},
        "known_vendors": set(),
        "arrow_anim_start": None,
        # IDEA 3 - Nuevo Líder
        "current_leader": None,
        "leader_celebration_start": None,
        "leader_name": "",
        "confetti_particles": [],
        "confetti_start": None,
    }

    cache: List[Dict[str, Any]] = []
    img_cache: Dict[str, str] = {}
    q_updates: SimpleQueue = SimpleQueue()

    cfg = ConfigManager() if ConfigManager else None
    sheets = None
    company = "DISTRIBUIDORA"
    if cfg:
        try:
            company = (cfg.get_identity().get("nombre") or company).upper()
        except Exception:
            pass

    # Logo
    logo_b64: Optional[str] = None
    if cfg:
        try:
            base = os.path.dirname(
                sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__)
            )
            for path in [
                f"{base}/CONFIG_GLOBAL/assets/logo_empresa.png",
                f"{base}/../CONFIG_GLOBAL/assets/logo_empresa.png",
            ]:
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        logo_b64 = base64.b64encode(f.read()).decode()
                    break
        except Exception:
            logo_b64 = None

    logo_img = _make_image_for_b64(logo_b64, width=54, height=54) if logo_b64 else None
    logo_cont = ft.Container(content=logo_img) if logo_img else ft.Container(width=0, height=0)

    # -------------------------
    # UI: HEADER / BADGES
    # -------------------------
    refresh_txt = ft.Text("⟳ #0 • --:--:--", size=12, color="#FFFFFF", weight=_fw_bold())
    refresh_badge = ft.Container(
        refresh_txt,
        padding=_padding_symmetric(12, 7),
        border_radius=999,
        bgcolor=_op(0.20, CYAN),
        border=_border_all(1.5, _op(0.55, CYAN)),
    )

    pb = ft.ProgressBar(value=0, color=CYAN, bgcolor=_op(0.18, "#FFFFFF"), height=8)
    pb_wrap = ft.Container(height=8, border_radius=999, content=pb, bgcolor=_op(0.10, "#FFFFFF"))

    btn_full = _make_fullscreen_button(page, icon_color=TXT)

    # -------------------------
    # UI: KPIs
    # -------------------------
    kpi_ok = ft.Text("0", size=36, weight=_fw_bold(), color=TXT)
    kpi_destacadas = ft.Text("0", size=36, weight=_fw_bold(), color=TXT)
    kpi_bad = ft.Text("0", size=36, weight=_fw_bold(), color=TXT)

    def kpi_card(title: str, val: ft.Text, icon_key: str, color: str, fallback_letter: str) -> ft.Container:
        guaranteed = _guaranteed_icon(icon_key, color=color, size=24, fallback_text=fallback_letter)
        return ft.Container(
            expand=True,
            padding=16,
            border_radius=16,
            bgcolor=PANEL,
            border=_border_all(1, BORDER),
            content=ft.Row(
                alignment="spaceBetween",
                controls=[
                    ft.Column(
                        spacing=3,
                        controls=[ft.Text(title, size=12, color=MUTED, weight=_fw_bold()), val],
                    ),
                    ft.Container(
                        width=48,
                        height=48,
                        border_radius=16,
                        bgcolor=_op(0.14, color),
                        border=_border_all(1, _op(0.30, color)),
                        alignment=_align_center(),
                        content=guaranteed,
                    ),
                ],
            ),
        )

    kpis = ft.Row(
        spacing=14,
        controls=[
            kpi_card("APROBADAS", kpi_ok, "CHECK_CIRCLE", GREEN, "A"),
            kpi_card("DESTACADAS", kpi_destacadas, "WHATSHOT", AMBER, "D"),
            kpi_card("RECHAZADAS", kpi_bad, "CANCEL", RED, "R"),
        ],
    )

    # -------------------------
    # UI: PERIOD SELECTOR
    # -------------------------
    def _pick_single_selected(raw: Any) -> Optional[str]:
        if raw is None:
            return None
        if isinstance(raw, str):
            return raw or None
        if isinstance(raw, (list, tuple)):
            return str(raw[0]) if raw else None
        if isinstance(raw, set):
            try:
                return str(sorted(raw)[0]) if raw else None
            except Exception:
                for v in raw:
                    return str(v)
        return str(raw) if raw else None

    def request_refresh(force: bool) -> None:
        if state["refresh_inflight"]:
            return
        state["refresh_inflight"] = True

        def worker():
            try:
                snap = compute_snapshot(force=force)
                q_updates.put(snap)
            finally:
                state["refresh_inflight"] = False

        page.run_thread(worker)

    def on_period_change(e) -> None:
        raw = getattr(getattr(e, "control", None), "selected", None)
        if raw is None:
            raw = getattr(e, "data", None)

        chosen = _pick_single_selected(raw)
        if not chosen:
            return

        state["periodo"] = chosen
        try:
            e.control.selected = [chosen]
        except Exception:
            try:
                e.control.selected = chosen
            except Exception:
                pass

        request_refresh(force=False)

    def build_period_control() -> ft.Control:
        if hasattr(ft, "SegmentedButton") and hasattr(ft, "Segment"):
            seg_btn_params = _sig_params(ft.SegmentedButton)

            def seg(value: str, label: str, icon_key: str):
                icon = _icon_control(icon_key, size=14, color=TXT)
                text = ft.Text(label) if icon else ft.Text(f"{_ICON_EMOJI.get(icon_key, '•')} {label}")
                kwargs = {}
                seg_params = _sig_params(ft.Segment)
                if "value" in seg_params:
                    kwargs["value"] = value
                if "label" in seg_params:
                    kwargs["label"] = text
                if "text" in seg_params and "label" not in kwargs:
                    kwargs["text"] = text
                if icon and "icon" in seg_params:
                    kwargs["icon"] = icon
                return ft.Segment(**kwargs)

            segments = [
                seg("hoy", "HOY", "TODAY"),
                seg("mes", "MES", "CALENDAR_MONTH"),
                seg("historico", "HISTÓRICO", "HISTORY"),
            ]
            base_kwargs: Dict[str, Any] = {}
            if "allow_multiple_selection" in seg_btn_params:
                base_kwargs["allow_multiple_selection"] = False
            if "on_change" in seg_btn_params:
                base_kwargs["on_change"] = on_period_change
            if "segments" in seg_btn_params:
                base_kwargs["segments"] = segments
            if "selected" in seg_btn_params:
                base_kwargs["selected"] = [state.get("periodo", "mes")]
            try:
                return ft.SegmentedButton(**base_kwargs)
            except Exception:
                try:
                    base_kwargs["selected"] = state.get("periodo", "mes")
                    return ft.SegmentedButton(**base_kwargs)
                except Exception:
                    pass

        return ft.Row(controls=[ft.Text("FILTRO: MES", color=RED)])

    period = build_period_control()
    rank_list = ft.Column(spacing=8)

    rank_panel = ft.Container(
        expand=True,
        padding=16,
        border_radius=20,
        bgcolor=PANEL,
        border=_border_all(1, BORDER),
        content=ft.Column(
            spacing=12,
            scroll=_scroll_auto(),
            controls=[
                ft.Row(
                    alignment="spaceBetween",
                    controls=[
                        ft.Row(
                            spacing=10,
                            controls=[
                                ft.Text("RANKING", size=15, weight=_fw_bold(), color=AMBER),
                                ft.Container(
                                    ft.Text(f"TOP {TOP}", size=12, color=BG, weight=_fw_bold()),
                                    padding=_padding_symmetric(12, 7),
                                    border_radius=999,
                                    bgcolor=_op(0.22, AMBER),
                                    border=_border_all(1, _op(0.35, AMBER)),
                                ),
                            ],
                        ),
                        _guaranteed_icon("EMOJI_EVENTS", color=AMBER, size=20, fallback_text="🏆"),
                    ],
                ),
                ft.Container(height=1, bgcolor=_op(0.10, "#FFFFFF")),
                rank_list,
            ],
        ),
    )

    # -------------------------
    # UI: LEFT COLUMN
    # -------------------------
    left = ft.Container(
        expand=1,
        padding=18,
        border_radius=24,
        bgcolor=PANEL2,
        border=_border_all(1, BORDER),
        content=ft.Column(
            spacing=16,
            expand=True,
            controls=[
                ft.Row(
                    alignment="spaceBetween",
                    controls=[
                        ft.Row(
                            spacing=14,
                            controls=[
                                logo_cont,
                                ft.Column(
                                    spacing=4,
                                    controls=[
                                        ft.Text(company, size=32, weight=_fw_bold(), color=TXT),
                                        ft.Text("TV DASHBOARD", size=12, color=_op(0.58, "#FFFFFF")),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                ft.Row(controls=[period]),
                kpis,
                rank_panel,
            ],
        ),
    )

    # -------------------------
    # UI: CAROUSEL (IDEA 1)
    # -------------------------
    img_a = ft.Image(src="", fit=_image_fit_contain())
    img_b = ft.Image(src="", fit=_image_fit_contain())

    img_a_wrap = ft.Container(
        content=img_a,
        expand=True,
        alignment=_align_center(),
        opacity=1.0,
    )
    img_b_wrap = ft.Container(
        content=img_b,
        expand=True,
        alignment=_align_center(),
        opacity=0.0,
    )

    # Badge "DESTACADA" flotante sobre la imagen
    carousel_star = _asset_or_fallback("star-burst.png", 28, 28, "🔥", AMBER)
    carousel_dest_badge = ft.Container(
        content=ft.Row(
            spacing=4,
            controls=[
                carousel_star,
                ft.Text("DESTACADA", size=11, weight=_fw_bold(), color=BG),
            ],
        ),
        padding=_padding_symmetric(10, 6),
        border_radius=8,
        bgcolor=_op(0.95, AMBER),
        visible=False,
        top=12,
        right=12,
    )

    img_stack = ft.Stack(
        expand=True,
        controls=[img_b_wrap, img_a_wrap, carousel_dest_badge],
    )

    img_area = ft.Container(
        expand=True,
        bgcolor=PANEL2,
        border_radius=16,
        border=_border_all(1, BORDER),
        padding=20,
        alignment=_align_center(),
        content=img_stack,
    )

    # Indicadores de punto (dots) del carrusel
    carousel_dots: List[ft.Container] = []
    for _ in range(5):
        carousel_dots.append(
            ft.Container(
                width=10,
                height=10,
                border_radius=5,
                bgcolor=_op(0.30, MUTED),
            )
        )
    dots_row = ft.Row(alignment="center", spacing=8, controls=carousel_dots, visible=False)

    # -------------------------
    # UI: FOOTER
    # -------------------------
    txt_cli = ft.Text("—", size=26, weight=_fw_bold(), color=TXT, expand=True)
    badge_status = ft.Container()
    txt_pdv = ft.Text("", size=13, color=CYAN, weight=_fw_bold())
    txt_ven = ft.Text("—", size=16, color=TXT)
    txt_dt = ft.Text("—", size=13, color=MUTED)

    icon_quote = _guaranteed_icon("FORMAT_QUOTE", color=MUTED, size=20, fallback_text="❝")
    txt_comment = ft.Text("", size=14, color=TXT, italic=True, weight=_fw_w600())
    comment_row = ft.Row(spacing=10, controls=[icon_quote, txt_comment], visible=False)

    icon_store = _icon_control("STORE", size=18, color=CYAN)
    icon_person = _icon_control("PERSON", size=18, color=MUTED)
    icon_time = _icon_control("ACCESS_TIME", size=18, color=MUTED)

    footer = ft.Container(
        padding=16,
        border_radius=20,
        bgcolor=PANEL,
        border=_border_all(1, BORDER),
        content=ft.Column(
            spacing=10,
            controls=[
                ft.Row(alignment="spaceBetween", controls=[txt_cli, badge_status]),
                comment_row,
                ft.Container(height=1, bgcolor=_op(0.10, "#FFFFFF")),
                ft.Row(
                    alignment="spaceBetween",
                    controls=[
                        ft.Row(spacing=10, controls=[c for c in [icon_store, txt_pdv] if c is not None]),
                        ft.Row(spacing=10, controls=[c for c in [icon_person, txt_ven] if c is not None]),
                        ft.Row(spacing=10, controls=[c for c in [icon_time, txt_dt] if c is not None]),
                    ],
                ),
            ],
        ),
    )

    # -------------------------
    # UI: NUEVO LÍDER OVERLAY (IDEA 3)
    # -------------------------
    leader_name_txt = ft.Text("", size=28, weight=_fw_bold(), color=BG)
    leader_crown = _asset_or_fallback("crown.png", 32, 32, "👑", GOLD_GLOW)
    leader_banner = ft.Container(
        content=ft.Row(
            alignment="center",
            spacing=14,
            controls=[
                leader_crown,
                ft.Column(
                    spacing=2,
                    horizontal_alignment="center",
                    controls=[
                        ft.Text("¡NUEVO LÍDER!", size=18, weight=_fw_bold(), color=BG),
                        leader_name_txt,
                    ],
                ),
                _asset_or_fallback("crown.png", 32, 32, "👑", GOLD_GLOW),
            ],
        ),
        padding=_padding_symmetric(30, 16),
        border_radius=16,
        bgcolor=AMBER,
        border=_border_all(3, GOLD_GLOW),
        alignment=_align_center(),
        visible=False,
        top=-100,
        left=0,
        right=0,
    )

    # Pool de confetti pre-allocado
    MAX_CONFETTI = CONFETTI_COUNT
    confetti_controls: List[ft.Container] = []
    for _ in range(MAX_CONFETTI):
        confetti_controls.append(
            ft.Container(
                width=8,
                height=5,
                border_radius=2,
                bgcolor=AMBER,
                visible=False,
                top=-20,
                left=0,
            )
        )

    confetti_layer = ft.Stack(
        controls=confetti_controls,
        expand=True,
        visible=False,
    )

    # -------------------------
    # UI: RIGHT COLUMN
    # -------------------------
    right = ft.Container(
        expand=1,
        padding=18,
        border_radius=24,
        bgcolor=PANEL2,
        border=_border_all(1, BORDER),
        content=ft.Column(
            spacing=16,
            expand=True,
            controls=[
                ft.Row(
                    alignment="spaceBetween",
                    controls=[
                        ft.Row(
                            spacing=10,
                            controls=[
                                ft.Text("ÚLTIMAS EXHIBICIONES", size=16, weight=_fw_bold(), color=CYAN),
                                refresh_badge,
                            ],
                        ),
                        btn_full,
                    ],
                ),
                pb_wrap,
                img_area,
                dots_row,
                footer,
            ],
        ),
    )

    # -------------------------
    # ROOT: Stack con overlays
    # -------------------------
    main_row = ft.Row(expand=True, spacing=16, controls=[left, right])
    root = ft.Stack(
        expand=True,
        controls=[
            ft.Container(expand=True, content=main_row),
            confetti_layer,
            leader_banner,
        ],
    )
    page.add(root)
    page.update()

    # -------------------------
    # DOWNLOAD / STATS
    # -------------------------
    def download() -> str:
        nonlocal sheets, cache
        if not SheetsManager:
            return "ERROR: No Manager"
        if not sheets:
            try:
                sheets = SheetsManager()
            except Exception as ex:
                logger.exception("SheetsManager init failed: %s", ex)
                return "ERROR: Connect"

        try:
            ws = sheets._get_ws("STATS")
            if not ws:
                return "ERROR: No STATS"
            raw = ws.get_all_records()
            if not raw:
                return "ERROR: Empty"
            cache = _upper(raw)
            return "OK"
        except Exception as ex:
            logger.exception("download failed: %s", ex)
            return "ERROR: Excep"

    def filter_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        valid_states = ("Aprobado", "Destacado", "Rechazado")

        if state["periodo"] == "hoy":
            today = datetime.now().date()
            out: List[Dict[str, Any]] = []
            for r in rows:
                st = str(r.get("ESTADO_AUDITORIA", "")).strip()
                d = _parse_date(_get(r, "FECHA"))
                if st in valid_states and d and d.date() == today:
                    out.append(r)
            return out

        if state["periodo"] == "mes":
            mon = _month(datetime.now())
            out = []
            for r in rows:
                st = str(r.get("ESTADO_AUDITORIA", "")).strip()
                d = _parse_date(_get(r, "FECHA"))
                if st in valid_states and d and _month(d) == mon:
                    out.append(r)
            return out

        return [r for r in rows if str(r.get("ESTADO_AUDITORIA", "")).strip() in valid_states]

    def calc_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        ok = dest = bad = 0
        vstats: Dict[str, Dict[str, int]] = {}

        for r in rows:
            st = str(r.get("ESTADO_AUDITORIA", "")).strip()
            v = _get(r, "VENDEDOR", d="SIN_VENDEDOR")
            if v not in vstats:
                vstats[v] = {"ap": 0, "dest": 0, "pts": 0, "env": 0}
            vstats[v]["env"] += 1

            if st == "Aprobado":
                ok += 1
                vstats[v]["ap"] += 1
                vstats[v]["pts"] += 1
            elif st == "Destacado":
                ok += 1
                dest += 1
                vstats[v]["dest"] += 1
                vstats[v]["pts"] += 2
            elif st == "Rechazado":
                bad += 1

        rank: List[Dict[str, Any]] = []
        for v, s in vstats.items():
            if s["pts"] > 0:
                rank.append({"v": v, "p": s["pts"], "ap": s["ap"], "dest": s["dest"]})

        rank.sort(key=lambda x: x["p"], reverse=True)
        return {"ok": ok, "dest": dest, "bad": bad, "rank": rank[:TOP]}

    # -------------------------
    # IDEA 2: RANK CHANGES
    # -------------------------
    def calc_rank_changes(new_rank: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Compare new ranking vs previous.
        Returns dict: vendor -> delta (positive=subió, negative=bajó, 0=igual, None=nuevo)
        """
        prev_map = state["prev_rank_map"]
        changes: Dict[str, Any] = {}
        new_map: Dict[str, int] = {}

        for i, entry in enumerate(new_rank):
            vendor = entry["v"]
            new_pos = i + 1
            new_map[vendor] = new_pos

            if not prev_map:
                changes[vendor] = 0
            elif vendor not in prev_map:
                if vendor in state["known_vendors"]:
                    changes[vendor] = 0
                else:
                    changes[vendor] = "new"
            else:
                old_pos = prev_map[vendor]
                delta = old_pos - new_pos
                changes[vendor] = delta

        state["prev_rank_map"] = new_map
        for entry in new_rank:
            state["known_vendors"].add(entry["v"])

        return changes

    # -------------------------
    # IDEA 3: LEADER DETECTION
    # -------------------------
    def check_new_leader(rank: List[Dict[str, Any]]) -> bool:
        """Check if there's a new #1. Returns True if leader changed."""
        if not rank:
            return False
        new_leader = rank[0]["v"]
        old_leader = state["current_leader"]
        state["current_leader"] = new_leader

        if old_leader is not None and new_leader != old_leader:
            return True
        return False

    def spawn_confetti(now_mono: float) -> None:
        """Initialize confetti particles for celebration."""
        particles = []
        colors = [AMBER, "#FFD700", BRONZE, "#FF8C00", "#FBBF24", "#F59E0B"]
        for i in range(MAX_CONFETTI):
            particles.append({
                "x": random.uniform(50, 750),
                "y": random.uniform(-80, -10),
                "vx": random.uniform(-3, 3),
                "vy": random.uniform(1.5, 5.0),
                "gravity": 0.15,
                "size_w": random.uniform(6, 12),
                "size_h": random.uniform(4, 8),
                "color": random.choice(colors),
                "opacity": 1.0,
            })
        state["confetti_particles"] = particles
        state["confetti_start"] = now_mono

        confetti_layer.visible = True
        for idx, p in enumerate(particles):
            if idx < len(confetti_controls):
                c = confetti_controls[idx]
                c.visible = True
                c.bgcolor = p["color"]
                c.width = p["size_w"]
                c.height = p["size_h"]
                c.top = p["y"]
                c.left = p["x"]
                c.opacity = 1.0

    def trigger_leader_celebration(leader_name: str, now_mono: float) -> None:
        """Start the new leader celebration sequence."""
        state["leader_celebration_start"] = now_mono
        state["leader_name"] = leader_name
        leader_name_txt.value = leader_name.upper()
        leader_banner.visible = True
        leader_banner.top = -100
        spawn_confetti(now_mono)

    # -------------------------
    # RANKING RENDER (IDEA 2 integrado)
    # -------------------------
    def _make_arrow_indicator(vendor: str) -> ft.Control:
        """Create position change indicator for a vendor."""
        change = state["rank_changes"].get(vendor, 0)

        if change == "new":
            badge_img = _asset_or_fallback("new-badge.png", 48, 32, "NEW", CYAN)
            return ft.Container(
                content=badge_img,
                padding=_padding_symmetric(4, 2),
                border_radius=6,
                bgcolor=_op(0.20, CYAN),
            )

        if isinstance(change, (int, float)) and change > 0:
            arrow = _asset_or_fallback("arrow-up.png", 24, 24, "▲", GREEN)
            return ft.Row(
                spacing=2,
                controls=[
                    arrow,
                    ft.Text(f"+{change}", size=10, color=GREEN, weight=_fw_bold()),
                ],
            )

        if isinstance(change, (int, float)) and change < 0:
            arrow = _asset_or_fallback("arrow-down.png", 24, 24, "▼", RED)
            return ft.Row(
                spacing=2,
                controls=[
                    arrow,
                    ft.Text(str(change), size=10, color=RED, weight=_fw_bold()),
                ],
            )

        equal = _asset_or_fallback("equals.png", 24, 24, "=", MUTED)
        return equal

    def render_rank(data: List[Dict[str, Any]]) -> None:
        rank_list.controls.clear()
        if not data:
            rank_list.controls.append(ft.Text("Sin datos", size=14, color=MUTED))
            return

        W_POS = 52
        W_ARROW = 48
        W_APR = 54
        W_DEST = 62
        W_PTS = 70

        icon_check = _guaranteed_icon("CHECK_CIRCLE", color=GREEN, size=12, fallback_text="✓")
        icon_fire = _guaranteed_icon("WHATSHOT", color=AMBER, size=12, fallback_text="🔥")
        icon_trophy = _guaranteed_icon("EMOJI_EVENTS", color=CYAN, size=12, fallback_text="🏆")

        header = ft.Container(
            padding=_padding_symmetric(12, 10),
            border_radius=12,
            bgcolor=_op(0.16, BLUE),
            border=_border_all(1, _op(0.25, BLUE)),
            content=ft.Row(
                alignment="spaceBetween",
                controls=[
                    ft.Container(
                        width=W_POS,
                        alignment=_align_center(),
                        content=ft.Row(
                            spacing=6,
                            alignment="center",
                            controls=[
                                _guaranteed_icon("EMOJI_EVENTS", color=AMBER, size=12, fallback_text="🏆"),
                                ft.Text("POS", size=11, color=TXT, weight=_fw_bold()),
                            ],
                        ),
                    ),
                    ft.Container(width=W_ARROW, alignment=_align_center()),
                    _vsep(h=18),
                    ft.Container(
                        expand=True,
                        alignment=_align_center_left(),
                        content=ft.Text("VENDEDOR", size=11, color=TXT, weight=_fw_bold()),
                    ),
                    _vsep(h=18),
                    ft.Container(
                        width=W_APR,
                        alignment=_align_center(),
                        content=ft.Row(
                            spacing=6,
                            alignment="center",
                            controls=[icon_check, ft.Text("APR", size=11, color=TXT, weight=_fw_bold())],
                        ),
                    ),
                    _vsep(h=18),
                    ft.Container(
                        width=W_DEST,
                        alignment=_align_center(),
                        content=ft.Row(
                            spacing=6,
                            alignment="center",
                            controls=[icon_fire, ft.Text("DEST", size=11, color=TXT, weight=_fw_bold())],
                        ),
                    ),
                    _vsep(h=18),
                    ft.Container(
                        width=W_PTS,
                        alignment=_align_center(),
                        content=ft.Row(
                            spacing=6,
                            alignment="center",
                            controls=[icon_trophy, ft.Text("PTS", size=11, color=TXT, weight=_fw_bold())],
                        ),
                    ),
                ],
            ),
        )
        rank_list.controls.append(header)

        for i, d in enumerate(data):
            if i == 0:
                pos_color = AMBER
                row_border = _border_all(1.6, _op(0.35, AMBER))
            elif i == 1:
                pos_color = CYAN
                row_border = _border_all(1.6, _op(0.30, CYAN))
            elif i == 2:
                pos_color = BRONZE
                row_border = _border_all(1.6, _op(0.30, BRONZE))
            else:
                pos_color = MUTED
                row_border = None

            medal = _guaranteed_icon("EMOJI_EVENTS", color=pos_color, size=14, fallback_text="🏆") if i < 3 else None
            arrow_indicator = _make_arrow_indicator(d["v"])

            rank_list.controls.append(
                ft.Container(
                    padding=_padding_symmetric(12, 10),
                    border_radius=12,
                    bgcolor="transparent",
                    border=row_border,
                    content=ft.Row(
                        alignment="spaceBetween",
                        controls=[
                            ft.Container(
                                width=W_POS,
                                alignment=_align_center(),
                                content=ft.Row(
                                    spacing=6,
                                    alignment="center",
                                    controls=[c for c in [
                                        medal,
                                        ft.Text(str(i + 1), size=14, color=pos_color, weight=_fw_bold()),
                                    ] if c is not None],
                                ),
                            ),
                            ft.Container(
                                width=W_ARROW,
                                alignment=_align_center(),
                                content=arrow_indicator,
                            ),
                            _vsep(h=18),
                            ft.Container(
                                expand=True,
                                alignment=_align_center_left(),
                                content=ft.Text(
                                    d["v"],
                                    size=14,
                                    color=TXT,
                                    weight=_fw_w600() if i < 3 else None,
                                    overflow="ellipsis",
                                ),
                            ),
                            _vsep(h=18),
                            ft.Container(
                                width=W_APR,
                                alignment=_align_center(),
                                content=ft.Text(str(d.get("ap", 0)), size=13, color=GREEN, weight=_fw_bold()),
                            ),
                            _vsep(h=18),
                            ft.Container(
                                width=W_DEST,
                                alignment=_align_center(),
                                content=ft.Text(str(d.get("dest", 0)), size=13, color=AMBER, weight=_fw_bold()),
                            ),
                            _vsep(h=18),
                            ft.Container(
                                width=W_PTS,
                                alignment=_align_center(),
                                padding=_padding_symmetric(8, 6),
                                border_radius=10,
                                bgcolor=_op(0.14, CYAN),
                                border=_border_all(1, _op(0.28, CYAN)),
                                content=ft.Text(str(d["p"]), size=13, color=TXT, weight=_fw_bold()),
                            ),
                        ],
                    ),
                )
            )

    # -------------------------
    # CAROUSEL + FOOTER UPDATE
    # -------------------------
    def update_footer_from_row(row: Optional[Dict[str, Any]]) -> None:
        """Update footer texts from a data row."""
        if not row:
            state["current_status"] = "Normal"
            return

        cli = _get(row, "CLIENTE", "NOMBRE_CLIENTE", d="-").strip()
        cli = cli if cli.startswith("#") else f"#{cli}"
        pdv = _get(row, "TIPO_PDV", "PDV", "PUNTO_DE_VENTA", d="").strip()
        ven = _get(row, "VENDEDOR", d="-")
        dt_str = f"{_get(row, 'FECHA', d='')} {_get(row, 'HORA', d='')}".strip()

        est = str(row.get("ESTADO_AUDITORIA", "")).strip()
        state["current_status"] = est

        if est == "Destacado":
            badge_text = "DESTACADA"
            badge_bg = AMBER
            badge_fg = BG
        else:
            badge_text = "APROBADA"
            badge_bg = GREEN
            badge_fg = BG

        badge_status.content = ft.Container(
            content=ft.Text(badge_text, size=12, weight=_fw_bold(), color=badge_fg),
            padding=_padding_symmetric(12, 8),
            border_radius=8,
            bgcolor=_op(0.95, badge_bg),
            border=_border_all(1.5, _op(0.80, badge_bg)),
        )

        raw_comment = str(row.get("COMENTARIOS", "")).strip()
        clean_comment = ""
        if "| Nota:" in raw_comment:
            clean_comment = raw_comment.split("| Nota:", 1)[1].strip()
        elif "| Motivo:" in raw_comment:
            clean_comment = raw_comment.split("| Motivo:", 1)[1].strip()
        elif "PC:" in raw_comment:
            parts = raw_comment.split("|")
            clean_comment = parts[-1].replace("Motivo:", "").strip() if len(parts) > 1 else raw_comment
        else:
            clean_comment = raw_comment

        if clean_comment and clean_comment.lower() not in ("-", "ok", "."):
            txt_comment.value = f'"{clean_comment}"'
            comment_row.visible = True
        else:
            comment_row.visible = False

        txt_cli.value = cli
        txt_pdv.value = pdv or "PDV Gral"
        txt_ven.value = ven
        txt_dt.value = dt_str

    def update_carousel_highlight(status: str) -> None:
        """Update image area border and badge for Destacada status."""
        if status == "Destacado":
            img_area.border = _border_all(4, _op(0.85, AMBER))
            carousel_dest_badge.visible = True
        else:
            img_area.border = _border_all(1, BORDER)
            carousel_dest_badge.visible = False

    def update_dots(active_index: int, total: int) -> None:
        """Update carousel dot indicators."""
        if total <= 1:
            dots_row.visible = False
            return
        dots_row.visible = True
        for i, dot in enumerate(carousel_dots):
            if i < total:
                dot.visible = True
                if i == active_index:
                    dot.bgcolor = CYAN
                    dot.width = 12
                    dot.height = 12
                    dot.border_radius = 6
                else:
                    dot.bgcolor = _op(0.30, MUTED)
                    dot.width = 10
                    dot.height = 10
                    dot.border_radius = 5
            else:
                dot.visible = False

    def set_carousel_image(item: Dict[str, Any]) -> None:
        """Set image source from a carousel item on img_a."""
        src = item.get("src", "")
        if src:
            img_a.src = src
        else:
            img_a.src = ""

    def apply_carousel(carousel_data: List[Dict[str, Any]], now_mono: float) -> None:
        """Initialize the carousel with fresh data."""
        items = state["carousel_items"]
        new_items = carousel_data if carousel_data else []
        state["carousel_items"] = new_items

        if not new_items:
            return

        idx = 0
        state["carousel_index"] = idx
        state["carousel_last_switch"] = now_mono
        state["carousel_fading"] = False

        first = new_items[idx]
        src = first.get("src", "")
        if src:
            img_a.src = src
        img_a_wrap.opacity = 1.0
        img_b_wrap.opacity = 0.0

        update_footer_from_row(first.get("row"))
        update_carousel_highlight(first.get("status", ""))
        update_dots(idx, len(new_items))

    # -------------------------
    # SNAPSHOT: compute / apply
    # -------------------------
    def compute_snapshot(force: bool) -> Dict[str, Any]:
        nonlocal cache
        if force:
            res = download()
            if res != "OK":
                return {"ok": False, "err": res}

        rows = filter_rows(cache)
        stats = calc_stats(rows)

        # IDEA 1: Get last 5 for carousel
        last5 = _last_n_ok(rows, 5)
        carousel_data: List[Dict[str, Any]] = []
        for r in last5:
            src = _resolve_image_src(r, sheets, img_cache)
            st = str(r.get("ESTADO_AUDITORIA", "")).strip()
            carousel_data.append({"row": r, "src": src, "status": st})

        # Trim image cache
        if len(img_cache) > 30:
            keys = list(img_cache.keys())
            for k in keys[: len(keys) - 20]:
                try:
                    del img_cache[k]
                except KeyError:
                    pass

        last = _last_ok(rows)
        return {"ok": True, "stats": stats, "last": last, "carousel": carousel_data}

    def apply_snapshot(snap: Dict[str, Any]) -> None:
        if not snap.get("ok"):
            refresh_txt.value = f"ERR ({snap.get('err', 'unknown')})"
            return

        now_mono = time.monotonic()
        stats = snap["stats"]

        kpi_ok.value = str(stats["ok"])
        kpi_destacadas.value = str(stats["dest"])
        kpi_bad.value = str(stats["bad"])

        # IDEA 2: Calculate rank changes before rendering
        rank_data = stats["rank"]
        state["rank_changes"] = calc_rank_changes(rank_data)
        state["arrow_anim_start"] = now_mono

        render_rank(rank_data)

        # IDEA 3: Check for new leader
        if check_new_leader(rank_data):
            trigger_leader_celebration(state["current_leader"], now_mono)

        # IDEA 1: Update carousel
        carousel_data = snap.get("carousel", [])
        if carousel_data:
            apply_carousel(carousel_data, now_mono)
        else:
            # Fallback: single image like before
            last = snap.get("last")
            if last:
                src = _resolve_image_src(last, sheets, img_cache)
                state["carousel_items"] = [{"row": last, "src": src, "status": str(last.get("ESTADO_AUDITORIA", "")).strip()}]
                state["carousel_index"] = 0
                img_a.src = src
                img_a_wrap.opacity = 1.0
                update_footer_from_row(last)
                update_carousel_highlight(str(last.get("ESTADO_AUDITORIA", "")).strip())
                dots_row.visible = False

        state["count"] += 1
        state["pulse_started"] = now_mono

    def update_badge_time() -> None:
        now = datetime.now()
        sec = now.strftime("%H:%M:%S")
        if sec == state.get("last_badge_second"):
            return
        state["last_badge_second"] = sec
        refresh_txt.value = f"⟳ #{state['count']} • {sec}"

    # -------------------------
    # ANIMATION: PULSE + PROGRESS
    # -------------------------
    def set_progress_color() -> None:
        st = state.get("current_status", "Normal")
        if st == "Destacado":
            pb.color = AMBER
        elif st == "Aprobado":
            pb.color = GREEN
        else:
            pb.color = CYAN

    def pulse_tick(now_mono: float) -> None:
        ps = state.get("pulse_started")
        if ps is None:
            return

        DURATION = 0.25
        dt = now_mono - ps

        if dt > DURATION:
            left.border = _border_all(1, BORDER)
            right.border = _border_all(1, BORDER)
            state["pulse_started"] = None
            return

        status = state.get("current_status", "Normal")
        if status == "Destacado":
            pulse_color = AMBER
            base_width = 3.0
        elif status == "Aprobado":
            pulse_color = GREEN
            base_width = 2.0
        else:
            pulse_color = CYAN
            base_width = 1.5

        progress = dt / DURATION
        factor = math.sin(progress * math.pi)

        width = base_width + (2.0 * factor)
        opacity = 0.3 + (0.7 * factor)

        b = _border_all(width, _op(opacity, pulse_color))

        left.border = b
        right.border = b

    # -------------------------
    # ANIMATION: CAROUSEL TICK (IDEA 1)
    # -------------------------
    def carousel_tick(now_mono: float) -> None:
        items = state["carousel_items"]
        if not items or len(items) <= 1:
            return

        if state["carousel_fading"]:
            fade_elapsed = now_mono - state["carousel_fade_start"]

            if fade_elapsed >= CAROUSEL_FADE_DUR:
                # Fade complete
                img_a_wrap.opacity = 1.0
                img_b_wrap.opacity = 0.0
                state["carousel_fading"] = False
            else:
                t = fade_elapsed / CAROUSEL_FADE_DUR
                # Smoothstep easing
                t = t * t * (3.0 - 2.0 * t)
                img_a_wrap.opacity = t
                img_b_wrap.opacity = 1.0 - t
            return

        elapsed = now_mono - state["carousel_last_switch"]
        if elapsed >= CAROUSEL_INTERVAL:
            old_idx = state["carousel_index"]
            new_idx = (old_idx + 1) % len(items)
            state["carousel_index"] = new_idx
            state["carousel_last_switch"] = now_mono

            new_item = items[new_idx]

            # img_b takes old (currently visible) image
            img_b.src = img_a.src
            img_b_wrap.opacity = 1.0

            # img_a gets new image
            new_src = new_item.get("src", "")
            if new_src:
                img_a.src = new_src
            img_a_wrap.opacity = 0.0

            state["carousel_fading"] = True
            state["carousel_fade_start"] = now_mono

            # Update footer and highlight for new image
            update_footer_from_row(new_item.get("row"))
            update_carousel_highlight(new_item.get("status", ""))
            update_dots(new_idx, len(items))

    # -------------------------
    # ANIMATION: CONFETTI TICK (IDEA 3)
    # -------------------------
    def confetti_tick(now_mono: float) -> None:
        start = state.get("confetti_start")
        if start is None:
            return

        elapsed = now_mono - start
        if elapsed > CONFETTI_DURATION:
            # Done - hide all
            confetti_layer.visible = False
            for c in confetti_controls:
                c.visible = False
            state["confetti_particles"] = []
            state["confetti_start"] = None
            return

        particles = state["confetti_particles"]
        dt = TICK_MS / 1000.0

        for idx, p in enumerate(particles):
            if idx >= len(confetti_controls):
                break

            p["vy"] += p["gravity"]
            p["x"] += p["vx"]
            p["y"] += p["vy"]

            # Fade out in last second
            if elapsed > CONFETTI_DURATION - 1.0:
                p["opacity"] = max(0, 1.0 - (elapsed - (CONFETTI_DURATION - 1.0)))

            c = confetti_controls[idx]
            c.top = p["y"]
            c.left = p["x"]
            c.opacity = p["opacity"]

    # -------------------------
    # ANIMATION: LEADER BANNER (IDEA 3)
    # -------------------------
    def leader_tick(now_mono: float) -> None:
        start = state.get("leader_celebration_start")
        if start is None:
            return

        elapsed = now_mono - start
        total_dur = LEADER_BANNER_DUR

        if elapsed > total_dur:
            # Hide banner
            leader_banner.visible = False
            leader_banner.top = -100
            state["leader_celebration_start"] = None
            return

        # Phase 1: Slide down with bounce (0-0.6s)
        if elapsed < 0.6:
            t = elapsed / 0.6
            # Bounce easing: overshoot then settle
            if t < 0.6:
                pos = (t / 0.6) * 1.2
            elif t < 0.8:
                pos = 1.2 - ((t - 0.6) / 0.2) * 0.3
            else:
                pos = 0.9 + ((t - 0.8) / 0.2) * 0.1
            target_y = 30
            leader_banner.top = -100 + (100 + target_y) * min(pos, 1.0)
            leader_banner.opacity = min(1.0, t * 2)

        # Phase 2: Visible (0.6s - 4.2s)
        elif elapsed < total_dur - 0.8:
            leader_banner.top = 30
            leader_banner.opacity = 1.0

        # Phase 3: Fade out upward (last 0.8s)
        else:
            fade_t = (elapsed - (total_dur - 0.8)) / 0.8
            leader_banner.top = 30 - 50 * fade_t
            leader_banner.opacity = 1.0 - fade_t

    # -------------------------
    # ANIMATION: ARROW BOUNCE (IDEA 2)
    # -------------------------
    # Arrow bounce is a visual-only effect. Since we rebuild the rank
    # controls each refresh, the bounce is implicit in the initial
    # scale of the arrow containers. For a real bounce we'd need
    # to track per-row scale, but the visual rebuild already gives
    # a "pop-in" feel. The ARROW_BOUNCE_DUR is reserved for future
    # per-element animation if needed.

    # -------------------------
    # TICKER ASYNC
    # -------------------------
    async def ticker_loop() -> None:
        while not state["stop"]:
            try:
                # Drain snapshots
                try:
                    while True:
                        snap = q_updates.get_nowait()
                        apply_snapshot(snap)
                except Empty:
                    pass

                update_badge_time()

                now_m = time.monotonic()
                elapsed = now_m - state["refresh_started"]

                # Progress bar
                pb.value = max(0.0, min(1.0, elapsed / REFRESH))

                set_progress_color()
                pulse_tick(now_m)

                # IDEA 1: Carousel rotation
                carousel_tick(now_m)

                # IDEA 3: Confetti + Leader banner
                confetti_tick(now_m)
                leader_tick(now_m)

                # Auto refresh
                if elapsed >= REFRESH:
                    state["refresh_started"] = now_m
                    request_refresh(force=True)

                page.update()

                await asyncio.sleep(TICK_MS / 1000.0)

            except Exception as ex:
                logger.exception("ticker_loop crashed: %s", ex)
                await asyncio.sleep(1)

    # First refresh
    request_refresh(force=True)

    # Launch ticker
    page.run_task(ticker_loop)


if __name__ == "__main__":
    ft.app(target=main)
