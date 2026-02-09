# -*- coding: utf-8 -*-
"""
file: visor.py
Visor de Fotos - Panel de Supervisión para evaluar exhibiciones.
"""

from __future__ import annotations

import base64
import re
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import flet as ft

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore


# =============================================================================
# PATHS / IMPORTS
# =============================================================================

def _setup_sys_path() -> None:
    here = Path(__file__).resolve().parent
    root = here.parent if here.name.lower() == "src" else here

    candidates = [
        str(root),
        str(root / "src"),
        str(root / "CONFIG_GLOBAL"),
        str(here),
        str(here / "CONFIG_GLOBAL"),
        str(root.parent / "internal"),
        str(root.parent / "internal" / "src"),
        str(root.parent / "internal" / "CONFIG_GLOBAL"),
    ]

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        candidates.extend(
            [
                str(exe_dir),
                str(exe_dir / "CONFIG_GLOBAL"),
                str(exe_dir / "internal"),
                str(exe_dir / "internal" / "src"),
                str(exe_dir / "internal" / "CONFIG_GLOBAL"),
            ]
        )

    for p in candidates:
        if p and p not in sys.path:
            sys.path.insert(0, p)


_setup_sys_path()

_IMPORT_ERROR = ""
try:
    try:
        from src.sheets_manager import SheetsManager
    except Exception:
        from sheets_manager import SheetsManager  # type: ignore
except Exception as e:
    SheetsManager = None  # type: ignore
    _IMPORT_ERROR = str(e)


# =============================================================================
# CONSTANTES / UTILIDADES
# =============================================================================

BATCH_SIZE = 10

STATUS_APPROVED = "Aprobado"
STATUS_HIGHLIGHTED = "Destacado"
STATUS_REJECTED = "Rechazado"

PLACEHOLDER_IMG = "https://upload.wikimedia.org/wikipedia/commons/1/14/No_Image_Available.jpg"

# Colores del tema
BG_MAIN = "#0f172a"
BG_PANEL = "#1e293b"
BG_CARD = "#1F2937"
BG_IMG = "#111827"
BG_INPUT = "#374151"
COLOR_GREEN = "#4ade80"
COLOR_AMBER = "#fb923c"
COLOR_RED = "#f87171"
COLOR_CYAN = "#22d3ee"
COLOR_MUTED = "white70"

_DRIVE_FILE_RE = re.compile(r"drive\.google\.com/file/d/([a-zA-Z0-9_-]+)")
_DRIVE_OPEN_RE = re.compile(r"drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)")
_DRIVE_UC_RE = re.compile(r"drive\.google\.com/uc\?export=(?:download|view)&id=([a-zA-Z0-9_-]+)")
_CONFIRM_RE = re.compile(r"[?&]confirm=([0-9A-Za-z_]+)")


def get_machine_id() -> str:
    try:
        import getpass
        return f"{getpass.getuser()}@{socket.gethostname()}"
    except Exception:
        return "unknown"


def safe_open_url(url: str) -> None:
    try:
        import webbrowser
        webbrowser.open(url)
    except Exception:
        pass


def drive_file_id(url: str) -> Optional[str]:
    u = (url or "").strip()
    if not u:
        return None
    for rx in (_DRIVE_FILE_RE, _DRIVE_OPEN_RE, _DRIVE_UC_RE):
        m = rx.search(u)
        if m:
            return m.group(1)
    return None


def drive_candidates(url: str) -> List[str]:
    u = (url or "").strip()
    fid = drive_file_id(u)
    if not fid:
        return [u]

    return [
        f"https://drive.google.com/uc?export=download&id={fid}",
        f"https://drive.google.com/uc?export=view&id={fid}",
        f"https://drive.google.com/thumbnail?id={fid}&sz=w2000",
        f"https://lh3.googleusercontent.com/d/{fid}",
        u,
    ]


def _extract_confirm_from_html(html: str) -> Optional[str]:
    if not html:
        return None
    m = _CONFIRM_RE.search(html)
    if m:
        return m.group(1)
    m2 = re.search(r"confirm=([0-9A-Za-z_]+)", html)
    if m2:
        return m2.group(1)
    return None


def _bytes_to_base64_src(data: bytes) -> str:
    """Convierte bytes de imagen a data URL base64 para ft.Image.src."""
    b64 = base64.b64encode(data).decode("utf-8")
    # Detectar MIME type
    mime = "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        mime = "image/png"
    elif data[:3] == b"\xff\xd8\xff":
        mime = "image/jpeg"
    elif data[:6] in (b"GIF87a", b"GIF89a"):
        mime = "image/gif"
    elif data[:4] == b"RIFF" and b"WEBP" in data[8:16]:
        mime = "image/webp"
    return f"data:{mime};base64,{b64}"


def fetch_image_bytes(url: str, timeout: float = 25.0) -> Tuple[Optional[bytes], str]:
    """Devuelve (bytes, reason)."""
    u = (url or "").strip()
    if not u or not u.startswith("http"):
        return None, "EMPTY"
    if requests is None:
        return None, "NO_REQUESTS"

    headers = {"User-Agent": "Mozilla/5.0"}
    session = requests.Session()

    try:
        for cand in drive_candidates(u):
            r = session.get(cand, headers=headers, timeout=timeout, allow_redirects=True)
            if r.status_code != 200:
                continue

            ctype = (r.headers.get("Content-Type") or "").lower()

            if "text/html" in ctype:
                if "drive.google.com/uc" not in cand:
                    continue

                confirm = None
                for k, v in session.cookies.items():
                    if k.startswith("download_warning"):
                        confirm = v
                        break
                if not confirm:
                    confirm = _extract_confirm_from_html(r.text)

                if not confirm:
                    continue

                fid = drive_file_id(cand) or drive_file_id(u)
                if not fid:
                    continue

                r2 = session.get(
                    f"https://drive.google.com/uc?export=download&confirm={confirm}&id={fid}",
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=True,
                )
                if r2.status_code != 200:
                    continue

                ctype2 = (r2.headers.get("Content-Type") or "").lower()
                if "text/html" in ctype2:
                    continue

                data = r2.content
            else:
                data = r.content

            if not data or len(data) < 128:
                continue

            return data, "OK"

        return None, "HTML"
    except Exception:
        return None, "EXC"
    finally:
        session.close()


# =============================================================================
# BATCH MANAGER
# =============================================================================

class BatchManager:
    def __init__(self):
        if SheetsManager is None:
            raise RuntimeError(f"No se pudo importar SheetsManager: {_IMPORT_ERROR}")
        self.sheets = SheetsManager()
        self.machine_id = get_machine_id()

        self.current_batch: List[Dict[str, Any]] = []
        self.current_index = 0
        self.batch_id = "-"

        self._lock = threading.Lock()

        self.reviewed = 0
        self.approved = 0
        self.highlighted = 0
        self.rejected = 0
        self.total_seconds_spent = 0.0
        self.last_photo_loaded_at: Optional[float] = None

    def load_new_batch(self) -> bool:
        with self._lock:
            try:
                pendientes = self.sheets.get_pending_evaluations()
            except Exception:
                pendientes = []

            if not pendientes:
                self.current_batch = []
                self.current_index = 0
                self.batch_id = "-"
                return False

            self.current_batch = pendientes[:BATCH_SIZE]
            self.current_index = 0
            self.batch_id = f"batch_{int(time.time())}"
            self.last_photo_loaded_at = time.time()
            return True

    def get_current_photo(self) -> Optional[Dict[str, Any]]:
        if 0 <= self.current_index < len(self.current_batch):
            return self.current_batch[self.current_index]
        return None

    def go_prev(self) -> bool:
        with self._lock:
            if self.current_index > 0:
                self.current_index -= 1
                self.last_photo_loaded_at = time.time()
                return True
            return False

    def go_next(self) -> bool:
        with self._lock:
            if self.current_index < len(self.current_batch) - 1:
                self.current_index += 1
                self.last_photo_loaded_at = time.time()
                return True
            return False

    def _mark_time_spent(self) -> None:
        now = time.time()
        if self.last_photo_loaded_at is None:
            self.last_photo_loaded_at = now
            return
        self.total_seconds_spent += max(0.0, now - self.last_photo_loaded_at)
        self.last_photo_loaded_at = now

    def evaluate_current(self, decision: str, comment_extra: str) -> Tuple[bool, str]:
        with self._lock:
            photo = self.get_current_photo()
            if not photo:
                return False, "ERROR"

            row_num = int(photo.get("row_num") or 0)
            if row_num <= 1:
                return False, "ERROR"

            self._mark_time_spent()

            extra = (comment_extra or "").strip()
            base = f"Evaluado por {self.machine_id}"
            comments = f"{base} | Nota: {extra}" if extra else base

            try:
                res = self.sheets.update_evaluation_status(
                    row_num=row_num,
                    new_status=decision,
                    comments=comments,
                )
            except Exception:
                res = "ERROR"

            if res != "OK":
                return False, res

            self.reviewed += 1
            if decision == STATUS_APPROVED:
                self.approved += 1
            elif decision == STATUS_HIGHLIGHTED:
                self.highlighted += 1
            elif decision == STATUS_REJECTED:
                self.rejected += 1

            return True, "OK"

    def get_stats(self) -> Dict[str, Any]:
        avg = (self.total_seconds_spent / self.reviewed) if self.reviewed else 0.0
        return {
            "reviewed": self.reviewed,
            "approved": self.approved,
            "highlighted": self.highlighted,
            "rejected": self.rejected,
            "avg_time": avg,
        }


# =============================================================================
# SNACKBAR HELPER
# =============================================================================

def _show_snackbar(page: ft.Page, message: str, bgcolor: str = "green") -> None:
    """Muestra un SnackBar usando la API correcta de Flet."""
    sb = ft.SnackBar(ft.Text(message), bgcolor=bgcolor)
    page.overlay.append(sb)
    sb.open = True
    page.update()


# =============================================================================
# UI
# =============================================================================

def main(page: ft.Page) -> None:
    if _IMPORT_ERROR:
        page.add(ft.Text(f"Error importando modulos: {_IMPORT_ERROR}", size=14))
        return

    manager = BatchManager()

    page.title = "Visor de Fotos - Supervision"
    page.window.min_width = 1200
    page.window.min_height = 800
    page.padding = 16
    page.bgcolor = BG_MAIN
    page.theme_mode = ft.ThemeMode.DARK

    state: Dict[str, Any] = {
        "is_loading": False,
        "is_empty": False,
        "is_evaluating": False,
        "is_typing": False,
    }
    last_img_reason = {"value": "-"}

    # =========================================================================
    # HEADER
    # =========================================================================
    txt_title = ft.Text("Visor de Fotos", size=26, weight="bold")
    txt_user = ft.Text(f"Supervisor: {manager.machine_id}", size=12, color=COLOR_MUTED)
    txt_batch = ft.Text("Batch: -", size=12, color=COLOR_MUTED)
    txt_progress = ft.Text("0/0", size=14, weight="bold")
    progress_bar = ft.ProgressBar(width=260, value=0, color=COLOR_CYAN)

    header = ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.Row([ft.Icon(ft.Icons.CAMERA_ALT, size=28, color=COLOR_CYAN), ft.Container(width=8), txt_title]),
                        ft.Row([txt_user]),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                ft.Row(
                    [
                        txt_batch,
                        ft.Container(width=12),
                        ft.Row([txt_progress, ft.Container(width=10), ft.Container(content=progress_bar, width=260)]),
                    ]
                ),
            ],
            spacing=6,
        ),
        bgcolor=BG_PANEL,
        padding=14,
        border_radius=12,
    )

    # =========================================================================
    # IMAGE VIEWER
    # =========================================================================
    img_control = ft.Image(src=PLACEHOLDER_IMG, fit=ft.BoxFit.CONTAIN, expand=True, border_radius=10)

    btn_prev = ft.IconButton(icon=ft.Icons.ARROW_BACK, icon_size=36, tooltip="Anterior", disabled=True)
    btn_next = ft.IconButton(icon=ft.Icons.ARROW_FORWARD, icon_size=36, tooltip="Siguiente", disabled=True)

    img_container = ft.Container(
        content=ft.Row(
            [btn_prev, ft.Container(content=img_control, expand=True), btn_next],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        ),
        bgcolor=BG_IMG,
        padding=10,
        border_radius=12,
        expand=True,
    )

    # =========================================================================
    # RIGHT PANEL: DETAILS
    # =========================================================================
    txt_cliente = ft.Text("Cliente: -", size=14)
    txt_vendedor = ft.Text("Vendedor: -", size=14)
    txt_tipo_pdv = ft.Text("Tipo PDV: -", size=14, color=COLOR_CYAN)
    txt_fecha = ft.Text("Fecha/Hora: -", size=12, color=COLOR_MUTED)
    txt_img_status = ft.Text("Imagen: -", size=12, color=COLOR_MUTED)
    btn_open_link = ft.OutlinedButton("Abrir link en navegador", icon=ft.Icons.OPEN_IN_NEW)

    details_panel = ft.Container(
        content=ft.Column(
            [
                ft.Row([ft.Icon(ft.Icons.DESCRIPTION, size=20, color=COLOR_CYAN), ft.Text("Detalles", size=18, weight="bold")]),
                ft.Container(height=4),
                ft.Row([ft.Icon(ft.Icons.STORE, size=16, color=COLOR_MUTED), ft.Container(width=4), txt_cliente]),
                ft.Row([ft.Icon(ft.Icons.PERSON, size=16, color=COLOR_MUTED), ft.Container(width=4), txt_vendedor]),
                ft.Row([ft.Icon(ft.Icons.PLACE, size=16, color=COLOR_MUTED), ft.Container(width=4), txt_tipo_pdv]),
                ft.Row([ft.Icon(ft.Icons.ACCESS_TIME, size=16, color=COLOR_MUTED), ft.Container(width=4), txt_fecha]),
                ft.Divider(height=1, color="white10"),
                txt_img_status,
                ft.Container(height=4),
                btn_open_link,
            ],
            spacing=6,
        ),
        bgcolor=BG_CARD,
        padding=16,
        border_radius=12,
    )

    # =========================================================================
    # RIGHT PANEL: STATS
    # =========================================================================
    stat_reviewed = ft.Text("0", size=18, weight="bold")
    stat_approved = ft.Text("0", size=18, weight="bold", color=COLOR_GREEN)
    stat_highlighted = ft.Text("0", size=18, weight="bold", color=COLOR_AMBER)
    stat_rejected = ft.Text("0", size=18, weight="bold", color=COLOR_RED)
    stat_avg_time = ft.Text("0.0s", size=18, weight="bold")

    stats_panel = ft.Container(
        content=ft.Column(
            [
                ft.Row([ft.Icon(ft.Icons.BAR_CHART, size=20, color=COLOR_CYAN), ft.Text("Estadisticas", size=18, weight="bold")]),
                ft.Container(height=8),
                ft.Row([ft.Text("Revisadas:", size=12), stat_reviewed]),
                ft.Row([ft.Icon(ft.Icons.CHECK_CIRCLE, color=COLOR_GREEN, size=20), ft.Text("Aprobadas:", size=12), stat_approved]),
                ft.Row([ft.Icon(ft.Icons.STAR, color=COLOR_AMBER, size=20), ft.Text("Destacadas:", size=12), stat_highlighted]),
                ft.Row([ft.Icon(ft.Icons.CLOSE, color=COLOR_RED, size=20), ft.Text("Rechazadas:", size=12), stat_rejected]),
                ft.Divider(height=1, color="white10"),
                ft.Row([ft.Icon(ft.Icons.ACCESS_TIME, size=20), ft.Text("Tiempo/foto:", size=12), stat_avg_time]),
            ],
            spacing=10,
        ),
        bgcolor=BG_CARD,
        padding=16,
        border_radius=12,
    )

    # =========================================================================
    # RIGHT PANEL: ATAJOS
    # =========================================================================
    shortcuts_panel = ft.Container(
        content=ft.Column(
            [
                ft.Text("Atajos de teclado", size=13, weight="bold", color=COLOR_MUTED),
                ft.Container(height=4),
                ft.Row([ft.Text("A", size=12, weight="bold", color=COLOR_GREEN), ft.Text("Aprobar", size=11, color=COLOR_MUTED)]),
                ft.Row([ft.Text("D", size=12, weight="bold", color=COLOR_AMBER), ft.Text("Destacar", size=11, color=COLOR_MUTED)]),
                ft.Row([ft.Text("R", size=12, weight="bold", color=COLOR_RED), ft.Text("Rechazar", size=11, color=COLOR_MUTED)]),
                ft.Row([ft.Text("<- ->", size=12, weight="bold"), ft.Text("Navegar fotos", size=11, color=COLOR_MUTED)]),
            ],
            spacing=4,
        ),
        bgcolor=BG_CARD,
        padding=12,
        border_radius=12,
    )

    right_panel = ft.Container(
        width=380,
        content=ft.Column(
            [details_panel, ft.Container(height=10), stats_panel, ft.Container(height=10), shortcuts_panel],
            spacing=0,
        ),
    )

    # =========================================================================
    # BOTTOM BAR: EVALUATION BUTTONS + COMMENT
    # =========================================================================
    txt_comment = ft.TextField(
        label="Comentario",
        hint_text="Opcional...",
        multiline=True,
        min_lines=2,
        max_lines=3,
        text_size=12,
        bgcolor=BG_INPUT,
        border_color="transparent",
        color="white",
        width=320,
        border_radius=12,
        on_focus=lambda e: state.update({"is_typing": True}),
        on_blur=lambda e: state.update({"is_typing": False}),
    )

    btn_approve = ft.ElevatedButton(
        content=ft.Text("APROBAR (A)", size=16, weight="bold"),
        icon=ft.Icons.CHECK_CIRCLE,
        bgcolor="green",
        color="white",
        height=60,
        expand=True,
        disabled=True,
    )
    btn_highlight = ft.ElevatedButton(
        content=ft.Text("DESTACAR (D)", size=16, weight="bold"),
        icon=ft.Icons.STAR,
        bgcolor="#F59E0B",
        color="white",
        height=60,
        expand=True,
        disabled=True,
    )
    btn_reject = ft.ElevatedButton(
        content=ft.Text("RECHAZAR (R)", size=16, weight="bold"),
        icon=ft.Icons.CLOSE,
        bgcolor="red",
        color="white",
        height=60,
        expand=True,
        disabled=True,
    )

    bottom_bar = ft.Container(
        content=ft.Row(
            [
                ft.Row([btn_approve, ft.Container(width=12), btn_highlight, ft.Container(width=12), btn_reject], expand=True),
                ft.Container(width=14),
                ft.Container(content=txt_comment, width=320),
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
        ),
        bgcolor=BG_PANEL,
        padding=12,
        border_radius=12,
    )

    # =========================================================================
    # OVERLAYS
    # =========================================================================
    loading_overlay = ft.Container(
        content=ft.Column(
            [
                ft.ProgressRing(width=70, height=70),
                ft.Container(height=16),
                ft.Text("Cargando pendientes...", size=18),
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        ),
        alignment=ft.Alignment.CENTER,
        expand=True,
        visible=False,
    )

    btn_reload = ft.ElevatedButton(
        content=ft.Text("RECARGAR"),
        icon=ft.Icons.REFRESH,
        bgcolor="blue",
        color="white",
        height=48,
    )

    empty_overlay = ft.Container(
        content=ft.Column(
            [
                ft.Icon(ft.Icons.EMOJI_EVENTS, size=110, color="#fbbf24"),
                ft.Container(height=14),
                ft.Text("Buen trabajo!", size=30, weight="bold"),
                ft.Container(height=8),
                ft.Text("No hay mas fotos pendientes en este momento", size=15, color=COLOR_MUTED),
                ft.Container(height=20),
                btn_reload,
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        ),
        alignment=ft.Alignment.CENTER,
        expand=True,
        visible=False,
    )

    main_area = ft.Row(
        [ft.Column([img_container], expand=True), ft.Container(width=14), right_panel],
        expand=True,
    )

    root = ft.Column(
        [header, ft.Container(height=10), main_area, ft.Container(height=10), bottom_bar],
        expand=True,
    )
    stack = ft.Stack([root, loading_overlay, empty_overlay], expand=True)
    page.add(stack)

    # =========================================================================
    # HELPERS
    # =========================================================================

    def set_loading(v: bool) -> None:
        state["is_loading"] = v
        loading_overlay.visible = v
        page.update()

    def set_empty(v: bool) -> None:
        state["is_empty"] = v
        empty_overlay.visible = v
        page.update()

    def set_buttons_enabled(enabled: bool) -> None:
        btn_approve.disabled = not enabled
        btn_highlight.disabled = not enabled
        btn_reject.disabled = not enabled
        page.update()

    def update_stats() -> None:
        s = manager.get_stats()
        stat_reviewed.value = str(s["reviewed"])
        stat_approved.value = str(s["approved"])
        stat_highlighted.value = str(s["highlighted"])
        stat_rejected.value = str(s["rejected"])
        stat_avg_time.value = f'{s["avg_time"]:.1f}s'
        page.update()

    def update_progress() -> None:
        total = len(manager.current_batch)
        idx = manager.current_index + 1 if total else 0
        txt_progress.value = f"{idx}/{total}"
        progress_bar.value = (idx / total) if total else 0
        txt_batch.value = f"Batch: {manager.batch_id}"
        btn_prev.disabled = idx <= 1
        btn_next.disabled = idx >= total
        page.update()

    def update_details(photo: Dict[str, Any]) -> None:
        txt_cliente.value = f"Cliente: {str(photo.get('cliente') or '-').strip()}"
        txt_vendedor.value = f"Vendedor: {str(photo.get('vendedor') or '-').strip()}"
        txt_tipo_pdv.value = f"Tipo PDV: {str(photo.get('tipo') or '-').strip()}"
        fecha = str(photo.get("fecha") or "").strip()
        hora = str(photo.get("hora") or "").strip()
        txt_fecha.value = f"Fecha/Hora: {fecha} {hora}".strip()
        txt_img_status.value = f"Imagen: {last_img_reason['value']}"
        page.update()

    def show_current_photo() -> None:
        photo = manager.get_current_photo()
        if not photo:
            set_buttons_enabled(False)
            return

        img_control.src = PLACEHOLDER_IMG
        last_img_reason["value"] = "-"
        update_details(photo)

        link = str(photo.get("url_foto") or "").strip()
        if link:
            data, reason = fetch_image_bytes(link)
            last_img_reason["value"] = reason
            if data:
                try:
                    img_control.src = _bytes_to_base64_src(data)
                except Exception:
                    img_control.src = PLACEHOLDER_IMG
        else:
            last_img_reason["value"] = "SIN_LINK"

        update_progress()
        set_buttons_enabled(True)
        update_details(photo)

    # =========================================================================
    # LOAD BATCH
    # =========================================================================

    def load_batch() -> None:
        set_empty(False)
        set_loading(True)
        set_buttons_enabled(False)
        txt_comment.value = ""
        page.update()

        def _load():
            ok = manager.load_new_batch()

            def on_loaded():
                set_loading(False)
                if not ok:
                    set_empty(True)
                    set_buttons_enabled(False)
                    return
                show_current_photo()
                update_stats()
                _show_snackbar(page, "Pendientes cargadas", bgcolor="green")

            # Ejecutar callback en el hilo principal de Flet
            page.run_thread(on_loaded)

        page.run_thread(_load)

    # =========================================================================
    # NAVIGATION / EVALUATE / KEYBOARD
    # =========================================================================

    def prev_photo() -> None:
        if manager.go_prev():
            show_current_photo()

    def next_photo() -> None:
        if manager.go_next():
            show_current_photo()
            return
        load_batch()

    def evaluate(decision: str) -> None:
        if state["is_evaluating"]:
            return

        state["is_evaluating"] = True
        set_buttons_enabled(False)
        page.update()

        extra = (txt_comment.value or "").strip()

        def _save():
            success, code = manager.evaluate_current(decision, extra)

            def on_saved():
                state["is_evaluating"] = False
                if not success:
                    if code == "LOCKED":
                        _show_snackbar(page, "Ya fue evaluada por otra persona", bgcolor="orange")
                        next_photo()
                        return
                    _show_snackbar(page, "Error guardando en Sheets", bgcolor="red")
                    set_buttons_enabled(True)
                    return

                if decision == STATUS_APPROVED:
                    _show_snackbar(page, "Foto aprobada", bgcolor="green")
                elif decision == STATUS_HIGHLIGHTED:
                    _show_snackbar(page, "Foto destacada", bgcolor="orange")
                elif decision == STATUS_REJECTED:
                    _show_snackbar(page, "Foto rechazada", bgcolor="red")

                txt_comment.value = ""
                update_stats()
                next_photo()

            page.run_thread(on_saved)

        page.run_thread(_save)

    def on_keyboard(e: ft.KeyboardEvent) -> None:
        if state["is_typing"]:
            return
        if state["is_loading"] or state["is_empty"] or state["is_evaluating"]:
            return
        if e.key == "ArrowLeft":
            prev_photo()
        elif e.key == "ArrowRight":
            next_photo()
        elif e.key.upper() == "A":
            evaluate(STATUS_APPROVED)
        elif e.key.upper() == "D":
            evaluate(STATUS_HIGHLIGHTED)
        elif e.key.upper() == "R":
            evaluate(STATUS_REJECTED)

    # =========================================================================
    # EVENT BINDINGS
    # =========================================================================
    btn_prev.on_click = lambda e: prev_photo()
    btn_next.on_click = lambda e: next_photo()
    btn_approve.on_click = lambda e: evaluate(STATUS_APPROVED)
    btn_highlight.on_click = lambda e: evaluate(STATUS_HIGHLIGHTED)
    btn_reject.on_click = lambda e: evaluate(STATUS_REJECTED)

    def on_open_link(e) -> None:
        photo = manager.get_current_photo()
        if not photo:
            return
        link = str(photo.get("url_foto") or "").strip()
        if link:
            safe_open_url(link)

    btn_open_link.on_click = on_open_link
    btn_reload.on_click = lambda e: load_batch()
    page.on_keyboard_event = on_keyboard

    load_batch()


if __name__ == "__main__":
    ft.app(target=main)
