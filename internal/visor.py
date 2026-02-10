# -*- coding: utf-8 -*-
"""
file: visor.py
Visor de Fotos - Panel de Supervisión para evaluar exhibiciones.
Soporta exhibiciones multi-foto (ráfaga) con galería interactiva.
"""

from __future__ import annotations

import base64
import re
import socket
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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
GROUPING_WINDOW_SECONDS = 90  # Ventana de agrupación para fotos de misma exhibición

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
COLOR_BLUE = "#60a5fa"
THUMB_ACTIVE_BORDER = "#22d3ee"
THUMB_SEEN_BG = "#1a3a2a"
THUMB_UNSEEN_BG = "#374151"

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
# AGRUPACIÓN DE EXHIBICIONES MULTI-FOTO
# =============================================================================

def _parse_timestamp(fecha: str, hora: str) -> Optional[datetime]:
    """Parsea fecha y hora de Sheets a datetime."""
    f = (fecha or "").strip()
    h = (hora or "").strip()
    if not f:
        return None
    try:
        if h:
            return datetime.strptime(f"{f} {h}", "%d/%m/%Y %H:%M")
        return datetime.strptime(f, "%d/%m/%Y")
    except Exception:
        return None


def _same_exhibition(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Determina si dos filas pertenecen a la misma exhibición."""
    if str(a.get("vendedor") or "").strip() != str(b.get("vendedor") or "").strip():
        return False
    if str(a.get("cliente") or "").strip() != str(b.get("cliente") or "").strip():
        return False
    if str(a.get("tipo") or "").strip() != str(b.get("tipo") or "").strip():
        return False

    ts_a = _parse_timestamp(a.get("fecha", ""), a.get("hora", ""))
    ts_b = _parse_timestamp(b.get("fecha", ""), b.get("hora", ""))

    if ts_a and ts_b:
        diff = abs((ts_a - ts_b).total_seconds())
        if diff > GROUPING_WINDOW_SECONDS:
            return False

    return True


def group_into_exhibitions(photos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Agrupa filas individuales en exhibiciones.
    Cada exhibición contiene 1+ fotos del mismo vendedor/cliente/tipo dentro
    de la ventana temporal.

    Returns:
        Lista de exhibiciones, cada una con:
        - id: str identificador único
        - vendedor, cliente, tipo, fecha, hora: datos de la primera foto
        - fotos: lista de dicts con {row_num, uuid, url_foto, fecha, hora}
        - total_fotos: int
    """
    if not photos:
        return []

    exhibitions: List[Dict[str, Any]] = []
    processed: Set[int] = set()

    for i, photo in enumerate(photos):
        if i in processed:
            continue

        # Crear nueva exhibición con esta foto como base
        exh: Dict[str, Any] = {
            "vendedor": str(photo.get("vendedor") or "-").strip(),
            "cliente": str(photo.get("cliente") or "-").strip(),
            "tipo": str(photo.get("tipo") or "-").strip(),
            "fecha": str(photo.get("fecha") or "").strip(),
            "hora": str(photo.get("hora") or "").strip(),
            "fotos": [{
                "row_num": photo.get("row_num"),
                "uuid": photo.get("uuid"),
                "url_foto": photo.get("url_foto", ""),
                "fecha": photo.get("fecha", ""),
                "hora": photo.get("hora", ""),
                "msg_id_telegram": photo.get("msg_id_telegram"),
            }],
        }
        processed.add(i)

        # Buscar fotos relacionadas
        for j, other in enumerate(photos):
            if j in processed:
                continue
            if _same_exhibition(photo, other):
                exh["fotos"].append({
                    "row_num": other.get("row_num"),
                    "uuid": other.get("uuid"),
                    "url_foto": other.get("url_foto", ""),
                    "fecha": other.get("fecha", ""),
                    "hora": other.get("hora", ""),
                    "msg_id_telegram": other.get("msg_id_telegram"),
                })
                processed.add(j)

        # Ordenar fotos por hora
        exh["fotos"].sort(key=lambda f: f.get("hora") or "")
        exh["total_fotos"] = len(exh["fotos"])

        # Generar ID
        first_uuid = exh["fotos"][0].get("uuid") or str(i)
        exh["id"] = f"{exh['vendedor']}_{exh['cliente']}_{first_uuid[:8]}"

        exhibitions.append(exh)

    return exhibitions


# =============================================================================
# BATCH MANAGER (con soporte multi-foto)
# =============================================================================

class BatchManager:
    def __init__(self):
        if SheetsManager is None:
            raise RuntimeError(f"No se pudo importar SheetsManager: {_IMPORT_ERROR}")
        self.sheets = SheetsManager()
        self.machine_id = get_machine_id()

        self.exhibitions: List[Dict[str, Any]] = []
        self.current_exh_index = 0
        self.current_photo_index = 0  # Índice de foto dentro de la exhibición
        self.batch_id = "-"

        # Tracking de fotos vistas por exhibición
        self.photos_seen: Dict[str, Set[int]] = {}  # exh_id -> set de índices vistos

        self._lock = threading.Lock()

        self.reviewed = 0
        self.approved = 0
        self.highlighted = 0
        self.rejected = 0
        self.total_seconds_spent = 0.0
        self.last_photo_loaded_at: Optional[float] = None

        # Cache de imágenes descargadas (para thumbnails y galería)
        self._image_cache: Dict[str, Optional[str]] = {}  # url -> base64_src or None

    def load_new_batch(self) -> bool:
        with self._lock:
            try:
                pendientes = self.sheets.get_pending_evaluations()
            except Exception:
                pendientes = []

            if not pendientes:
                self.exhibitions = []
                self.current_exh_index = 0
                self.current_photo_index = 0
                self.batch_id = "-"
                self.photos_seen = {}
                return False

            # Agrupar en exhibiciones
            all_exhibitions = group_into_exhibitions(pendientes)

            # Tomar un batch limitado
            self.exhibitions = all_exhibitions[:BATCH_SIZE]
            self.current_exh_index = 0
            self.current_photo_index = 0
            self.batch_id = f"batch_{int(time.time())}"
            self.last_photo_loaded_at = time.time()

            # Inicializar tracking de fotos vistas
            self.photos_seen = {}
            for exh in self.exhibitions:
                self.photos_seen[exh["id"]] = set()

            return True

    def get_current_exhibition(self) -> Optional[Dict[str, Any]]:
        if 0 <= self.current_exh_index < len(self.exhibitions):
            return self.exhibitions[self.current_exh_index]
        return None

    def get_current_photo(self) -> Optional[Dict[str, Any]]:
        exh = self.get_current_exhibition()
        if not exh:
            return None
        fotos = exh.get("fotos", [])
        if 0 <= self.current_photo_index < len(fotos):
            return fotos[self.current_photo_index]
        return None

    def mark_photo_seen(self) -> None:
        """Marca la foto actual como vista."""
        exh = self.get_current_exhibition()
        if not exh:
            return
        exh_id = exh["id"]
        if exh_id not in self.photos_seen:
            self.photos_seen[exh_id] = set()
        self.photos_seen[exh_id].add(self.current_photo_index)

    def all_photos_seen(self) -> bool:
        """Retorna True si se vieron todas las fotos de la exhibición actual."""
        exh = self.get_current_exhibition()
        if not exh:
            return False
        total = exh.get("total_fotos", 1)
        if total <= 1:
            return True  # Exhibiciones de 1 foto siempre están "vistas"
        exh_id = exh["id"]
        seen = self.photos_seen.get(exh_id, set())
        return len(seen) >= total

    def get_seen_count(self) -> Tuple[int, int]:
        """Retorna (vistas, total) para la exhibición actual."""
        exh = self.get_current_exhibition()
        if not exh:
            return 0, 0
        total = exh.get("total_fotos", 1)
        exh_id = exh["id"]
        seen = len(self.photos_seen.get(exh_id, set()))
        return seen, total

    def go_prev_exhibition(self) -> bool:
        with self._lock:
            if self.current_exh_index > 0:
                self.current_exh_index -= 1
                self.current_photo_index = 0
                self.last_photo_loaded_at = time.time()
                return True
            return False

    def go_next_exhibition(self) -> bool:
        with self._lock:
            if self.current_exh_index < len(self.exhibitions) - 1:
                self.current_exh_index += 1
                self.current_photo_index = 0
                self.last_photo_loaded_at = time.time()
                return True
            return False

    def go_to_photo(self, index: int) -> bool:
        """Navega a una foto específica dentro de la exhibición actual."""
        exh = self.get_current_exhibition()
        if not exh:
            return False
        total = exh.get("total_fotos", 1)
        if 0 <= index < total:
            self.current_photo_index = index
            return True
        return False

    def go_prev_photo(self) -> bool:
        exh = self.get_current_exhibition()
        if not exh:
            return False
        if self.current_photo_index > 0:
            self.current_photo_index -= 1
            return True
        return False

    def go_next_photo(self) -> bool:
        exh = self.get_current_exhibition()
        if not exh:
            return False
        total = exh.get("total_fotos", 1)
        if self.current_photo_index < total - 1:
            self.current_photo_index += 1
            return True
        return False

    def _mark_time_spent(self) -> None:
        now = time.time()
        if self.last_photo_loaded_at is None:
            self.last_photo_loaded_at = now
            return
        self.total_seconds_spent += max(0.0, now - self.last_photo_loaded_at)
        self.last_photo_loaded_at = now

    def evaluate_current_exhibition(self, decision: str, comment_extra: str) -> Tuple[bool, str]:
        """Evalúa TODAS las fotos de la exhibición actual con el mismo estado."""
        with self._lock:
            exh = self.get_current_exhibition()
            if not exh:
                return False, "ERROR"

            fotos = exh.get("fotos", [])
            if not fotos:
                return False, "ERROR"

            self._mark_time_spent()

            extra = (comment_extra or "").strip()
            base = f"Evaluado por {self.machine_id}"
            comments = f"{base} | Nota: {extra}" if extra else base

            all_ok = True
            last_code = "OK"

            for foto in fotos:
                row_num = int(foto.get("row_num") or 0)
                if row_num <= 1:
                    continue

                try:
                    res = self.sheets.update_evaluation_status(
                        row_num=row_num,
                        new_status=decision,
                        comments=comments,
                    )
                except Exception:
                    res = "ERROR"

                if res != "OK":
                    all_ok = False
                    last_code = res

            if not all_ok and last_code == "LOCKED":
                return False, "LOCKED"
            if not all_ok:
                return False, last_code

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

    def get_cached_image(self, url: str) -> Optional[str]:
        """Obtiene imagen cacheada o None si no está en cache."""
        return self._image_cache.get(url)

    def cache_image(self, url: str, base64_src: Optional[str]) -> None:
        """Guarda imagen en cache."""
        self._image_cache[url] = base64_src


# =============================================================================
# SNACKBAR HELPER
# =============================================================================

def _show_snackbar(page: ft.Page, message: str, bgcolor: str = "green") -> None:
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
    txt_total_fotos = ft.Text("", size=12, color=COLOR_MUTED)
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
                        ft.Container(width=8),
                        txt_total_fotos,
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
    # IMAGE VIEWER (foto principal)
    # =========================================================================
    img_control = ft.Image(src=PLACEHOLDER_IMG, fit=ft.BoxFit.CONTAIN, expand=True, border_radius=10)

    btn_prev_exh = ft.IconButton(icon=ft.Icons.ARROW_BACK, icon_size=36, tooltip="Exhibicion anterior", disabled=True)
    btn_next_exh = ft.IconButton(icon=ft.Icons.ARROW_FORWARD, icon_size=36, tooltip="Siguiente exhibicion", disabled=True)

    img_container = ft.Container(
        content=ft.Row(
            [btn_prev_exh, ft.Container(content=img_control, expand=True), btn_next_exh],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        ),
        bgcolor=BG_IMG,
        padding=10,
        border_radius=12,
        expand=True,
    )

    # =========================================================================
    # GALERÍA DE MINIATURAS (debajo de la imagen principal)
    # =========================================================================
    txt_foto_counter = ft.Text("", size=13, weight="bold", color=COLOR_CYAN)
    txt_fotos_vistas_status = ft.Text("", size=12, color=COLOR_MUTED)
    gallery_row = ft.Row(spacing=8, scroll=ft.ScrollMode.AUTO)

    btn_prev_foto = ft.IconButton(
        icon=ft.Icons.CHEVRON_LEFT, icon_size=24,
        tooltip="Foto anterior", disabled=True,
    )
    btn_next_foto = ft.IconButton(
        icon=ft.Icons.CHEVRON_RIGHT, icon_size=24,
        tooltip="Siguiente foto", disabled=True,
    )

    gallery_nav = ft.Row(
        [
            btn_prev_foto,
            ft.Container(content=gallery_row, expand=True),
            btn_next_foto,
        ],
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    gallery_panel = ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [txt_foto_counter, ft.Container(expand=True), txt_fotos_vistas_status],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                gallery_nav,
            ],
            spacing=6,
        ),
        bgcolor=BG_PANEL,
        padding=10,
        border_radius=10,
        visible=False,  # Solo visible cuando hay múltiples fotos
    )

    # =========================================================================
    # RIGHT PANEL: DETAILS
    # =========================================================================
    txt_cliente = ft.Text("Cliente: -", size=14)
    txt_vendedor = ft.Text("Vendedor: -", size=14)
    txt_tipo_pdv = ft.Text("Tipo PDV: -", size=14, color=COLOR_CYAN)
    txt_fecha = ft.Text("Fecha/Hora: -", size=12, color=COLOR_MUTED)
    txt_img_status = ft.Text("Imagen: -", size=12, color=COLOR_MUTED)
    txt_fotos_count = ft.Text("", size=13, weight="bold", color=COLOR_BLUE)
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
                txt_fotos_count,
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
                ft.Row([ft.Text("<- ->", size=12, weight="bold"), ft.Text("Navegar exhibiciones", size=11, color=COLOR_MUTED)]),
                ft.Row([ft.Text("1-5", size=12, weight="bold"), ft.Text("Ir a foto N", size=11, color=COLOR_MUTED)]),
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
    # BOTTOM BAR: EVALUATION BUTTONS + COMMENT + LOCK WARNING
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

    txt_lock_warning = ft.Container(
        content=ft.Row(
            [
                ft.Icon(ft.Icons.LOCK, size=16, color=COLOR_AMBER),
                ft.Text("Ve todas las fotos antes de evaluar", size=12, color=COLOR_AMBER, weight="bold"),
            ],
            alignment=ft.MainAxisAlignment.CENTER,
        ),
        bgcolor="#3d2800",
        padding=ft.padding.symmetric(vertical=6, horizontal=12),
        border_radius=8,
        visible=False,
    )

    bottom_bar = ft.Container(
        content=ft.Column(
            [
                txt_lock_warning,
                ft.Row(
                    [
                        ft.Row([btn_approve, ft.Container(width=12), btn_highlight, ft.Container(width=12), btn_reject], expand=True),
                        ft.Container(width=14),
                        ft.Container(content=txt_comment, width=320),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
            ],
            spacing=6,
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
                ft.Text("No hay mas exhibiciones pendientes en este momento", size=15, color=COLOR_MUTED),
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
        [
            ft.Column([img_container, gallery_panel], expand=True, spacing=8),
            ft.Container(width=14),
            right_panel,
        ],
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

    def set_eval_buttons_enabled(enabled: bool) -> None:
        btn_approve.disabled = not enabled
        btn_highlight.disabled = not enabled
        btn_reject.disabled = not enabled

    def update_eval_buttons() -> None:
        """Actualiza el estado de los botones según si se vieron todas las fotos."""
        exh = manager.get_current_exhibition()
        if not exh:
            set_eval_buttons_enabled(False)
            txt_lock_warning.visible = False
            page.update()
            return

        can_eval = manager.all_photos_seen()
        set_eval_buttons_enabled(can_eval)
        txt_lock_warning.visible = not can_eval
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
        total_exh = len(manager.exhibitions)
        idx = manager.current_exh_index + 1 if total_exh else 0
        txt_progress.value = f"Exhibicion {idx}/{total_exh}"
        progress_bar.value = (idx / total_exh) if total_exh else 0
        txt_batch.value = f"Batch: {manager.batch_id}"

        # Contar fotos totales
        total_fotos = sum(e.get("total_fotos", 1) for e in manager.exhibitions)
        txt_total_fotos.value = f"({total_fotos} fotos totales)"

        btn_prev_exh.disabled = idx <= 1
        btn_next_exh.disabled = idx >= total_exh
        page.update()

    def update_details() -> None:
        exh = manager.get_current_exhibition()
        if not exh:
            return

        txt_cliente.value = f"Cliente: {exh.get('cliente', '-')}"
        txt_vendedor.value = f"Vendedor: {exh.get('vendedor', '-')}"
        txt_tipo_pdv.value = f"Tipo PDV: {exh.get('tipo', '-')}"
        fecha = exh.get("fecha", "")
        hora = exh.get("hora", "")
        txt_fecha.value = f"Fecha/Hora: {fecha} {hora}".strip()
        txt_img_status.value = f"Imagen: {last_img_reason['value']}"

        total = exh.get("total_fotos", 1)
        if total > 1:
            txt_fotos_count.value = f"{total} fotos en esta exhibicion"
            txt_fotos_count.visible = True
        else:
            txt_fotos_count.value = ""
            txt_fotos_count.visible = False

        page.update()

    def _build_thumbnail(index: int, foto: Dict[str, Any], is_active: bool, is_seen: bool) -> ft.Container:
        """Construye un widget de miniatura para la galería."""
        if is_active:
            border = ft.border.all(3, THUMB_ACTIVE_BORDER)
            bg = BG_IMG
        elif is_seen:
            border = ft.border.all(2, COLOR_GREEN)
            bg = THUMB_SEEN_BG
        else:
            border = ft.border.all(1, "white20")
            bg = THUMB_UNSEEN_BG

        status_icon = ""
        if is_seen:
            status_icon = "✓"

        label = ft.Text(f"Foto {index + 1} {status_icon}", size=10, text_align=ft.TextAlign.CENTER)

        def on_thumb_click(e, idx=index):
            navigate_to_photo(idx)

        return ft.Container(
            content=ft.Column(
                [
                    ft.Container(
                        content=ft.Icon(
                            ft.Icons.IMAGE,
                            size=28,
                            color=COLOR_GREEN if is_seen else (COLOR_CYAN if is_active else COLOR_MUTED),
                        ),
                        width=60,
                        height=50,
                        alignment=ft.Alignment.CENTER,
                        bgcolor=bg,
                        border_radius=6,
                        border=border,
                    ),
                    label,
                ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=2,
            ),
            on_click=on_thumb_click,
            ink=True,
            padding=4,
            border_radius=8,
        )

    def update_gallery() -> None:
        """Actualiza la galería de miniaturas."""
        exh = manager.get_current_exhibition()
        if not exh:
            gallery_panel.visible = False
            page.update()
            return

        total = exh.get("total_fotos", 1)
        if total <= 1:
            gallery_panel.visible = False
            page.update()
            return

        gallery_panel.visible = True

        # Actualizar contador
        current = manager.current_photo_index + 1
        txt_foto_counter.value = f"Foto {current} de {total}"

        # Actualizar estado de vistas
        seen, total_f = manager.get_seen_count()
        if seen >= total_f:
            txt_fotos_vistas_status.value = f"Vistas: {seen}/{total_f} ✓"
            txt_fotos_vistas_status.color = COLOR_GREEN
        else:
            txt_fotos_vistas_status.value = f"Vistas: {seen}/{total_f}"
            txt_fotos_vistas_status.color = COLOR_AMBER

        # Navegación de fotos
        btn_prev_foto.disabled = manager.current_photo_index <= 0
        btn_next_foto.disabled = manager.current_photo_index >= total - 1

        # Reconstruir miniaturas
        exh_id = exh["id"]
        seen_set = manager.photos_seen.get(exh_id, set())
        gallery_row.controls.clear()

        for i, foto in enumerate(exh.get("fotos", [])):
            is_active = (i == manager.current_photo_index)
            is_seen = (i in seen_set)
            thumb = _build_thumbnail(i, foto, is_active, is_seen)
            gallery_row.controls.append(thumb)

        page.update()

    def show_current_photo() -> None:
        """Muestra la foto actual de la exhibición actual."""
        photo = manager.get_current_photo()
        if not photo:
            set_eval_buttons_enabled(False)
            return

        img_control.src = PLACEHOLDER_IMG
        last_img_reason["value"] = "-"
        update_details()

        link = str(photo.get("url_foto") or "").strip()
        if link:
            # Intentar cache primero
            cached = manager.get_cached_image(link)
            if cached is not None:
                img_control.src = cached
                last_img_reason["value"] = "OK (cache)"
            else:
                data, reason = fetch_image_bytes(link)
                last_img_reason["value"] = reason
                if data:
                    try:
                        src = _bytes_to_base64_src(data)
                        img_control.src = src
                        manager.cache_image(link, src)
                    except Exception:
                        img_control.src = PLACEHOLDER_IMG
                        manager.cache_image(link, None)
                else:
                    manager.cache_image(link, None)
        else:
            last_img_reason["value"] = "SIN_LINK"

        # Marcar como vista
        manager.mark_photo_seen()

        update_progress()
        update_gallery()
        update_eval_buttons()
        update_details()

    def navigate_to_photo(index: int) -> None:
        """Navega a una foto específica dentro de la exhibición."""
        if manager.go_to_photo(index):
            show_current_photo()

    # =========================================================================
    # LOAD BATCH
    # =========================================================================

    def load_batch() -> None:
        set_empty(False)
        set_loading(True)
        set_eval_buttons_enabled(False)
        txt_comment.value = ""
        page.update()

        def _load():
            ok = manager.load_new_batch()

            def on_loaded():
                set_loading(False)
                if not ok:
                    set_empty(True)
                    set_eval_buttons_enabled(False)
                    return
                show_current_photo()
                update_stats()
                n_exh = len(manager.exhibitions)
                total_fotos = sum(e.get("total_fotos", 1) for e in manager.exhibitions)
                _show_snackbar(page, f"{n_exh} exhibiciones cargadas ({total_fotos} fotos)", bgcolor="green")

            page.run_thread(on_loaded)

        page.run_thread(_load)

    # =========================================================================
    # NAVIGATION / EVALUATE / KEYBOARD
    # =========================================================================

    def prev_exhibition() -> None:
        if manager.go_prev_exhibition():
            show_current_photo()

    def next_exhibition() -> None:
        if manager.go_next_exhibition():
            show_current_photo()
            return
        load_batch()

    def prev_photo() -> None:
        if manager.go_prev_photo():
            show_current_photo()

    def next_photo() -> None:
        if manager.go_next_photo():
            show_current_photo()

    def evaluate(decision: str) -> None:
        if state["is_evaluating"]:
            return

        # Verificar que se vieron todas las fotos
        if not manager.all_photos_seen():
            _show_snackbar(page, "Debes ver todas las fotos antes de evaluar", bgcolor="orange")
            return

        state["is_evaluating"] = True
        set_eval_buttons_enabled(False)
        page.update()

        extra = (txt_comment.value or "").strip()

        def _save():
            exh = manager.get_current_exhibition()
            n_fotos = exh.get("total_fotos", 1) if exh else 1

            success, code = manager.evaluate_current_exhibition(decision, extra)

            def on_saved():
                state["is_evaluating"] = False
                if not success:
                    if code == "LOCKED":
                        _show_snackbar(page, "Ya fue evaluada por otra persona", bgcolor="orange")
                        next_exhibition()
                        return
                    _show_snackbar(page, "Error guardando en Sheets", bgcolor="red")
                    update_eval_buttons()
                    return

                fotos_text = f" ({n_fotos} fotos)" if n_fotos > 1 else ""
                if decision == STATUS_APPROVED:
                    _show_snackbar(page, f"Exhibicion aprobada{fotos_text}", bgcolor="green")
                elif decision == STATUS_HIGHLIGHTED:
                    _show_snackbar(page, f"Exhibicion destacada{fotos_text}", bgcolor="orange")
                elif decision == STATUS_REJECTED:
                    _show_snackbar(page, f"Exhibicion rechazada{fotos_text}", bgcolor="red")

                txt_comment.value = ""
                update_stats()
                next_exhibition()

            page.run_thread(on_saved)

        page.run_thread(_save)

    def on_keyboard(e: ft.KeyboardEvent) -> None:
        if state["is_typing"]:
            return
        if state["is_loading"] or state["is_empty"] or state["is_evaluating"]:
            return

        key = e.key

        if key == "ArrowLeft":
            prev_exhibition()
        elif key == "ArrowRight":
            next_exhibition()
        elif key.upper() == "A":
            evaluate(STATUS_APPROVED)
        elif key.upper() == "D":
            evaluate(STATUS_HIGHLIGHTED)
        elif key.upper() == "R":
            evaluate(STATUS_REJECTED)
        elif key in ("1", "2", "3", "4", "5"):
            # Navegar a foto N dentro de la exhibición
            idx = int(key) - 1
            navigate_to_photo(idx)

    # =========================================================================
    # EVENT BINDINGS
    # =========================================================================
    btn_prev_exh.on_click = lambda e: prev_exhibition()
    btn_next_exh.on_click = lambda e: next_exhibition()
    btn_prev_foto.on_click = lambda e: prev_photo()
    btn_next_foto.on_click = lambda e: next_photo()
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
