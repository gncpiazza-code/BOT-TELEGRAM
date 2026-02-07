# file: src/sheets_manager.py
import base64
import io
import logging
import os
import ssl
import sys
import uuid
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


# ✅ LOGGING MEJORADO
try:
    from logger_config import get_logger, log_exception
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger("SheetsManager")


def _get_ar_tz():
    if ZoneInfo is None:
        return timezone(timedelta(hours=-3))
    try:
        return ZoneInfo("America/Argentina/Buenos_Aires")
    except Exception:
        return timezone(timedelta(hours=-3))


AR_TZ = _get_ar_tz()


def _setup_import_paths() -> None:
    here = Path(__file__).resolve().parent
    root = here.parent

    candidates = [
        root,
        root / "src",
        root / "CONFIG_GLOBAL",
    ]

    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        candidates.extend([exe_dir, exe_dir / "CONFIG_GLOBAL"])

    for p in candidates:
        p_str = str(p)
        if p_str not in sys.path:
            sys.path.insert(0, p_str)


def _ensure_tls_ca_bundle() -> Tuple[Optional[str], List[Tuple[str, str]]]:
    vars_to_check = ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE")
    bad: List[Tuple[str, str]] = []

    for k in vars_to_check:
        v = (os.environ.get(k) or "").strip()
        if v and not os.path.exists(v):
            bad.append((k, v))

    if not bad:
        return None, []

    ca_path: Optional[str] = None

    try:
        import certifi  # type: ignore

        c = certifi.where()
        if c and os.path.exists(c):
            ca_path = c
    except Exception:
        ca_path = None

    if not ca_path:
        try:
            default_cafile = ssl.get_default_verify_paths().cafile
            if default_cafile and os.path.exists(default_cafile):
                ca_path = default_cafile
        except Exception:
            ca_path = None

    if not ca_path:
        return None, bad

    os.environ["SSL_CERT_FILE"] = ca_path
    os.environ["REQUESTS_CA_BUNDLE"] = ca_path
    os.environ["CURL_CA_BUNDLE"] = ca_path
    return ca_path, bad


_setup_import_paths()

try:
    from config_manager import ConfigManager
except Exception as e:
    raise ImportError(
        f"No se pudo importar ConfigManager. Revisá la carpeta CONFIG_GLOBAL. Detalle: {e}"
    )


@dataclass
class UploadInfo:
    drive_link: str
    file_id: str


class SheetsManager:
    """
    Google Sheets + Drive.
    """

    def __init__(self):
        logger.info("="*60)
        logger.info("📊 Inicializando SheetsManager...")
        logger.info("="*60)
        self.cfg = ConfigManager()

        self.gc = None
        self.drive_service = None
        self.spreadsheet = None

        self.sheet_map: Dict[str, str] = {}
        self._ws_cache: Dict[str, Any] = {}
        self._drive_folder_cache: Dict[Tuple[str, str], str] = {}

        self.last_error: str = ""

        self._connect()
        self._check_structure_safe()

    def is_connected(self) -> bool:
        return self.spreadsheet is not None

    def _collect_token_candidates(self) -> List[str]:
        candidates: List[str] = []

        def add_base(b: Path) -> None:
            b = b.resolve()
            candidates.append(str(b / "CONFIG_GLOBAL" / "token.json"))
            candidates.append(str(b / "token.json"))
            cur = b
            for _ in range(6):
                cur = cur.parent
                candidates.append(str(cur / "CONFIG_GLOBAL" / "token.json"))
                candidates.append(str(cur / "token.json"))

        env_path = (os.environ.get("GOOGLE_TOKEN_PATH") or "").strip()
        if env_path:
            candidates.append(env_path)

        try:
            base_dir = Path(__file__).resolve().parent.parent
            add_base(base_dir)
        except Exception:
            pass

        try:
            add_base(Path(os.getcwd()))
        except Exception:
            pass

        try:
            if sys.argv and sys.argv[0]:
                add_base(Path(sys.argv[0]).resolve().parent)
        except Exception:
            pass

        if getattr(sys, "frozen", False):
            try:
                add_base(Path(sys.executable).resolve().parent)
            except Exception:
                pass

        seen = set()
        unique: List[str] = []
        for p in candidates:
            if p and p not in seen:
                seen.add(p)
                unique.append(p)
        return unique

    def _resolve_token_path(self) -> Optional[str]:
        possible = self._collect_token_candidates()
        token_path = next((p for p in possible if os.path.exists(p)), None)
        if token_path:
            return token_path
        self.last_error = f"No se encontró token.json. Busqué en: {possible}"
        logger.error(f"❌ CRÍTICO: {self.last_error}")
        return None

    def _connect(self) -> None:
        logger.info("🔌 Conectando a Google Sheets...")
        google_conf = self.cfg.get_google_cloud_config()
        sheet_id = str(google_conf.get("sheet_id_maestro") or "").strip()

        if not sheet_id:
            self.last_error = "Falta google_cloud.sheet_id_maestro en config.json"
            logger.error(f"❌ {self.last_error}")
            return

        token_path = self._resolve_token_path()
        if not token_path:
            return

        ca_path, bad_env = _ensure_tls_ca_bundle()
        if bad_env and ca_path:
            logger.warning(
                f"🔒 TLS fix aplicado. Variables rotas: {bad_env}. Usando CA bundle: {ca_path}"
            )
        elif bad_env and not ca_path:
            self.last_error = (
                f"TLS CA bundle roto por variables de entorno: {bad_env}. "
                f"No pude resolver un CA bundle válido."
            )
            logger.error(f"❌ {self.last_error}")
            return

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        try:
            creds = Credentials.from_authorized_user_file(token_path, scopes)
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())

            self.gc = gspread.authorize(creds)
            self.drive_service = build("drive", "v3", credentials=creds)
            self.spreadsheet = self.gc.open_by_key(sheet_id)
            logger.info(f"✅ Conectado exitosamente a spreadsheet: {sheet_id[:20]}...")
            self.last_error = ""

        except Exception as e:
            self.last_error = f"Error de conexión Google: {e}"
            logger.error(f"❌ {self.last_error}")

    def _check_structure_safe(self) -> None:
        if not self.spreadsheet:
            return

        structures = {
            "CONFIG": ["KEY", "VALUE", "DESCRIPCION", "LAST_UPDATE"],
            "USERS": ["ID_TELEGRAM", "NOMBRE_VENDEDOR", "GRUPO/ZONA", "ROL", "ESTADO"],
            "RAW_LOGS": [
                "UUID", "TIMESTAMP", "ID_USER", "USER_NAME", "TYPE", "FILE_ID",
                "URL_DRIVE", "RAW_JSON", "CLIENT_INPUT", "STATUS", "HASH", "IS_FRAUD",
            ],
            "STATS": [
                "FECHA", "HORA", "VENDEDOR", "GRUPO", "CLIENTE", "TIPO_PDV",
                "LINK_FOTO", "ESTADO_AUDITORIA", "COMENTARIOS", "UUID_REF",
                "MSG_ID_SUPERVISOR", "CONTEO_GRUPO", "CHAT_ID_REF", "SYNC_TELEGRAM",
            ],
            "GROUPS": ["CHAT_ID", "TITULO", "FIRST_SEEN", "LAST_SEEN"],
            "DASHBOARD": [],
        }

        existing_ws = {ws.title: ws for ws in self.spreadsheet.worksheets()}

        for logical_name, headers in structures.items():
            target_ws = None
            for title, ws in existing_ws.items():
                if title.strip().upper() == logical_name.strip().upper():
                    target_ws = ws
                    self.sheet_map[logical_name] = title
                    break

            if not target_ws:
                try:
                    target_ws = self.spreadsheet.add_worksheet(logical_name, rows=400, cols=40)
                    if headers:
                        target_ws.append_row(headers)
                    logger.info(f"🛠 Creada pestaña: {logical_name}")
                    self.sheet_map[logical_name] = logical_name
                except Exception:
                    continue
            else:
                self.sheet_map[logical_name] = target_ws.title
                if headers:
                    try:
                        current_headers = target_ws.row_values(1)
                        if len(current_headers) < len(headers):
                            for i in range(len(current_headers), len(headers)):
                                target_ws.update_cell(1, i + 1, headers[i])
                            logger.info(f"🔧 Headers actualizados en {logical_name}")
                    except Exception:
                        continue

    def _get_ws(self, name: str):
        if not self.spreadsheet:
            return None
        mapped = self.sheet_map.get(name, name)
        if mapped in self._ws_cache:
            return self._ws_cache[mapped]
        try:
            ws = self.spreadsheet.worksheet(mapped)
            self._ws_cache[mapped] = ws
            return ws
        except Exception:
            return None

    def _escape_drive_query_value(self, s: str) -> str:
        s = s.replace("\\", "\\\\").replace("'", "\\'")
        return s

    def _ensure_drive_folder(self, parent_id: str, folder_name: str) -> str:
        if not self.drive_service or not parent_id or not folder_name:
            return ""

        cache_key = (parent_id, folder_name)
        if cache_key in self._drive_folder_cache:
            return self._drive_folder_cache[cache_key]

        safe_name = self._escape_drive_query_value(folder_name)
        q = (
            "mimeType='application/vnd.google-apps.folder' and trashed=false "
            f"and name='{safe_name}' and '{parent_id}' in parents"
        )

        try:
            res = self.drive_service.files().list(q=q, fields="files(id,name)", pageSize=10).execute()
            files = res.get("files", [])
            if files:
                fid = files[0]["id"]
                self._drive_folder_cache[cache_key] = fid
                return fid
        except Exception:
            pass

        try:
            meta = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            }
            created = self.drive_service.files().create(body=meta, fields="id").execute()
            fid = created.get("id", "")
            if fid:
                self._drive_folder_cache[cache_key] = fid
            return fid
        except Exception:
            return ""

    def _to_ar(self, dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        try:
            return dt.astimezone(AR_TZ)
        except Exception:
            return dt

    def _parse_sent_datetime(self, sent_at: Any) -> datetime:
        if isinstance(sent_at, datetime):
            return self._to_ar(sent_at)
        if isinstance(sent_at, (int, float)):
            dt = datetime.fromtimestamp(sent_at, tz=timezone.utc)
            return self._to_ar(dt)
        if isinstance(sent_at, str):
            raw = sent_at.strip()
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return self._to_ar(dt)
            except Exception:
                pass
            for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
                try:
                    dt = datetime.strptime(raw, fmt)
                    dt = dt.replace(tzinfo=AR_TZ)
                    return dt
                except Exception:
                    pass
        return datetime.now(AR_TZ)

    def upload_image_to_drive(self, file_bytes: bytes, filename: str, user_id: int, username: str, group_title: str = "BOT_UPLOAD") -> Any:
        """
        Wrapper de compatibilidad para host_bot.py.
        """
        link = self.upload_to_drive(
            file_bytes=file_bytes,
            filename=filename,
            group_title=group_title, 
            sent_at=datetime.now()
        )
        
        class UploadResult:
            def __init__(self, lnk):
                self.drive_link = lnk
        
        return UploadResult(link)

    def upload_to_drive(self, file_bytes: bytes, filename: str, group_title: str, sent_at: Any) -> str:
        base_folder_id = self.cfg.get_google_cloud_config().get("drive_folder_id")
        if not base_folder_id or not self.drive_service:
            return ""

        group_title = (group_title or "SIN_GRUPO").strip()
        sent_dt = self._parse_sent_datetime(sent_at)
        date_folder = sent_dt.strftime("%d-%m-%Y")

        try:
            group_folder_id = self._ensure_drive_folder(base_folder_id, group_title)
            if not group_folder_id:
                return ""
            date_folder_id = self._ensure_drive_folder(group_folder_id, date_folder)
            if not date_folder_id:
                return ""
        except Exception as e:
            logger.error(f"❌ Error crítico carpetas Drive: {e}")
            return ""

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                meta = {"name": filename, "parents": [date_folder_id]}
                media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype="image/jpeg", resumable=True)
                
                f = self.drive_service.files().create(body=meta, media_body=media, fields="webViewLink").execute()
                return f.get("webViewLink", "")
                
            except Exception as e:
                wait_s = attempt * 2
                logger.warning(f"⚠️ Falló subida Drive (Intento {attempt}/{max_retries}): {e}. Reintentando en {wait_s}s...")
                if attempt < max_retries:
                    time.sleep(wait_s)
                else:
                    logger.error("❌ Falló definitivamente la subida a Drive.")
        
        return ""

    def get_pos_types(self) -> List[str]:
        try:
            ws = self._get_ws("CONFIG")
            if not ws:
                return ["Kiosco", "Almacén", "Supermercado"]
            records = ws.get_all_records()
            for r in records:
                if str(r.get("KEY", "")).strip() == "tipos_pdv":
                    return [x.strip() for x in str(r.get("VALUE", "")).split(",") if x.strip()]
        except Exception:
            pass
        return ["Kiosco", "Almacén", "Supermercado"]

    def get_next_id(self) -> str:
        return str(uuid.uuid4())
        
    def log_raw(self, user_id: int, username: str, nro_cliente: str, tipo_pdv: str, drive_link: str, group_title: str = "BOT_UPLOAD", chat_id: int = 0) -> str:
        """
        Registra la subida en RAW_LOGS y devuelve el UUID generado.
        """
        if not drive_link:
            return ""
            
        new_uuid = self.get_next_id()
        file_id_fake = f"DRIVE_{int(time.time())}"
        
        data = {
            "id": new_uuid,
            "sent_at": datetime.now(),
            "uploader_id": user_id,
            "uploader_name": username,
            "file_id": file_id_fake,
            "drive_link": drive_link,
            "client_id": nro_cliente,
            "hash_md5": "MANUAL_UPLOAD",
            "is_fraud": False,
            "group_title": group_title,
            "type": tipo_pdv,
            "chat_id": chat_id
        }
        
        self.register_image(data)
        return new_uuid

    def registrar_aprobacion_directa(self, user_id: int, username: str, nro_cliente: str, tipo_pdv: str, drive_link: str, estado: str, supervisor_name: str) -> str:
        """
        Registra una imagen directamente en RAW_LOGS y STATS con un estado final.
        """
        if not drive_link:
            return ""
            
        new_uuid = self.get_next_id()
        file_id_fake = f"DRIVE_{int(time.time())}"
        ts_now = datetime.now(AR_TZ)
        fecha = ts_now.strftime("%d/%m/%Y")
        hora = ts_now.strftime("%H:%M")
        
        ws_raw = self._get_ws("RAW_LOGS")
        if ws_raw:
            try:
                ws_raw.append_row([
                    new_uuid,
                    ts_now.strftime("%d/%m/%Y %H:%M:%S"),
                    user_id,
                    username,
                    "Foto",
                    file_id_fake,
                    drive_link,
                    "",
                    nro_cliente,
                    estado.upper(),
                    "DIRECT_UPLOAD",
                    "NO",
                ], value_input_option="USER_ENTERED")
            except Exception as e:
                logger.error(f"❌ Error registrando aprobación directa en RAW_LOGS: {e}")

        ws_stats = self._get_ws("STATS")
        if ws_stats:
            nota = f"Evaluado por {supervisor_name} (Subida directa)"
            row_stats = [
                fecha,
                hora,
                username,
                "BOT_UPLOAD",
                nro_cliente,
                tipo_pdv,
                drive_link,
                estado,
                nota,
                new_uuid,
                "",
                '=COUNTIF(D:D, "BOT_UPLOAD")',
                0,
                "OK",
            ]
            try:
                ws_stats.append_row(row_stats, value_input_option="USER_ENTERED")
                logger.info(f"✅ Aprobación directa registrada: UUID={new_uuid[:8]} Estado={estado}")
                return new_uuid
            except Exception as e:
                logger.error(f"❌ Error registrando aprobación directa en STATS: {e}")
                return ""
        return ""

    def upsert_user(self, user_id: int, username: str, full_name: str, role: str = "vendedor") -> None:
        ws = self._get_ws("USERS")
        if not ws:
            return
        uid_str = str(user_id)
        username = (username or "").strip().lstrip("@")
        full_name = (full_name or "").strip()
        role = (role or "vendedor").strip()

        try:
            cell = ws.find(uid_str, in_column=1)
        except Exception:
            cell = None

        if not cell:
            try:
                ws.append_row([uid_str, full_name, username, role, "activo"], value_input_option="USER_ENTERED")
            except Exception:
                pass
            return

        row = cell.row
        try:
            existing = ws.row_values(row)
        except Exception:
            existing = []

        def safe_get(idx: int) -> str:
            return existing[idx] if idx < len(existing) else ""

        cur_name = safe_get(1)
        cur_user = safe_get(2)
        cur_role = safe_get(3)
        cur_state = safe_get(4) or "activo"

        try:
            if full_name and full_name != cur_name:
                ws.update_cell(row, 2, full_name)
            if username and username != cur_user:
                ws.update_cell(row, 3, username)
            if role and cur_role == "":
                ws.update_cell(row, 4, role)
            if cur_state == "":
                ws.update_cell(row, 5, "activo")
        except Exception:
            pass

    def get_user_id_by_username(self, username: str) -> Optional[int]:
        username = (username or "").strip().lstrip("@").lower()
        if not username:
            return None
        ws = self._get_ws("USERS")
        if not ws:
            return None
        try:
            records = ws.get_all_records()
            for r in records:
                u = str(r.get("GRUPO/ZONA", "")).strip().lstrip("@").lower()
                if u == username:
                    try:
                        return int(str(r.get("ID_TELEGRAM", "")).strip())
                    except Exception:
                        return None
        except Exception:
            return None
        return None

    def get_user_role(self, chat_id: int, user_id: int) -> str:
        admin_id = str(self.cfg.get_telegram_config().get("admin_id") or "").strip()
        if admin_id and str(user_id) == admin_id:
            return "admin"

        ws = self._get_ws("USERS")
        if not ws:
            return "desconocido"

        try:
            cell = ws.find(str(user_id), in_column=1)
            if not cell:
                return "desconocido"
            row = ws.row_values(cell.row)
            if len(row) < 5:
                return "desconocido"
            estado = str(row[4]).strip().lower()
            if estado and estado not in ("activo", "active", "ok"):
                return "desconocido"
            rol = str(row[3]).strip()
            return rol if rol else "vendedor"
        except Exception:
            return "desconocido"

    def set_user_role(self, chat_id: int, user_id: int, username: str, role: str, full_name: str) -> None:
        self.upsert_user(user_id=user_id, username=username, full_name=full_name, role=role)

    def upsert_group(self, chat_id: int, title: str) -> None:
        ws = self._get_ws("GROUPS")
        if not ws:
            return
        cid = str(chat_id)
        title = (title or "").strip()
        now = datetime.now(AR_TZ).strftime("%d/%m/%Y %H:%M:%S")

        try:
            cell = ws.find(cid, in_column=1)
        except Exception:
            cell = None

        if not cell:
            try:
                ws.append_row([cid, title, now, now], value_input_option="USER_ENTERED")
            except Exception:
                pass
            return

        try:
            row = cell.row
            if title:
                ws.update_cell(row, 2, title)
            ws.update_cell(row, 4, now)
        except Exception:
            pass

    def get_groups(self) -> List[Dict[str, str]]:
        ws = self._get_ws("GROUPS")
        if not ws:
            return []
        try:
            rows = ws.get_all_records()
            out = []
            for r in rows:
                cid = str(r.get("CHAT_ID", "")).strip()
                titulo = str(r.get("TITULO", "")).strip()
                if cid:
                    out.append({
                        "chat_id": cid,
                        "title": titulo,
                        "first_seen": str(r.get("FIRST_SEEN", "")).strip(),
                        "last_seen": str(r.get("LAST_SEEN", "")).strip(),
                    })
            out.sort(key=lambda x: (x["title"] or "", x["chat_id"]))
            return out
        except Exception:
            return []

    def register_image(self, img_data: Dict[str, Any]) -> None:
        """
        Registra la imagen en RAW_LOGS y en STATS.
        """
        uuid_val = img_data.get("id", "")
        logger.info(f"📝 Registrando imagen: UUID={uuid_val[:8]}...")
        sent_dt = self._parse_sent_datetime(img_data.get("sent_at"))
        fecha = sent_dt.strftime("%d/%m/%Y")
        hora = sent_dt.strftime("%H:%M")

        ws_raw = self._get_ws("RAW_LOGS")
        ws_stats = self._get_ws("STATS")
        if not ws_raw or not ws_stats:
            return

        raw_link = img_data.get("drive_link", "")
        
        try:
            ws_raw.append_row([
                img_data["id"],
                datetime.now(AR_TZ).strftime("%d/%m/%Y %H:%M:%S"),
                img_data["uploader_id"],
                img_data["uploader_name"],
                "Foto",
                img_data["file_id"],
                raw_link,
                "",
                img_data["client_id"],
                "OK",
                img_data["hash_md5"],
                "SI" if img_data["is_fraud"] else "NO",
            ], value_input_option="USER_ENTERED")
        except Exception as e:
            logger.error(f"❌ Error registrando en RAW_LOGS: {e}")
            
        row_stats = [
            fecha,
            hora,
            img_data["uploader_name"],
            img_data.get("group_title", ""),
            img_data["client_id"],
            img_data["type"],
            raw_link,    
            "Pendiente",
            "",
            img_data["id"],
            "",
            f'=COUNTIF(D:D, "{img_data.get("group_title","")}")',
            img_data["chat_id"],
            "",
        ]

        try:
            ws_stats.append_row(row_stats, value_input_option="USER_ENTERED")
            logger.info(f"✅ Imagen registrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error registrando en STATS: {e}")
    def update_telegram_refs(self, uuid_ref: str, chat_id: int, msg_id: int) -> None:
        """Guarda referencias de Telegram en STATS (MSG_ID_SUPERVISOR y CHAT_ID_REF)."""
        ws = self._get_ws("STATS")
        if not ws:
            return
        try:
            cell = ws.find(str(uuid_ref))
            if not cell:
                return
            ws.update_cell(cell.row, 11, str(msg_id))
            ws.update_cell(cell.row, 13, str(chat_id))
        except Exception:
            pass



    def update_supervisor_msg_id(self, uuid_ref: str, msg_id: int) -> None:
        ws = self._get_ws("STATS")
        if not ws:
            return
        try:
            cell = ws.find(str(uuid_ref))
            if cell:
                ws.update_cell(cell.row, 11, str(msg_id))
        except Exception:
            pass

    def update_status_by_uuid(self, uuid_ref: str, new_status: str, supervisor_name: str = "Bot", comments: str = "") -> str:
        """
        Retorna: "OK", "LOCKED", "ERROR"
        """
        logger.info(f"🔄 Actualizando estado: UUID={uuid_ref[:8]}... → {new_status}")
        ws = self._get_ws("STATS")
        if not ws:
            return "ERROR"
        try:
            cell = ws.find(str(uuid_ref))
            if not cell:
                return "ERROR"
            
            # --- OPTIMISTIC LOCKING ---
            try:
                current_status = ws.cell(cell.row, 8).value
            except Exception:
                current_status = ""

            if current_status and current_status not in ("Pendiente", ""):
                logger.warning(f"🔒 UUID ya evaluado (estado actual: {current_status})")
                return "LOCKED"
            # --------------------------

            ws.update_cell(cell.row, 8, new_status)
            
            note = f"Evaluado por {supervisor_name}"
            if comments:
                note += f" | Nota: {comments}"
            
            ws.update_cell(cell.row, 9, note)
            ws.update_cell(cell.row, 14, "OK")
            logger.info(f"✅ Estado actualizado correctamente")
            return "OK"
        except Exception as e:
            logger.error(f"❌ ERROR actualizando estado: {e}")
            return "ERROR"

    def get_unsynced_actions(self) -> List[Dict[str, Any]]:
        ws = self._get_ws("STATS")
        if not ws:
            return []
        try:
            rows = ws.get_all_records()
            grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for i, row in enumerate(rows):
                estado = str(row.get("ESTADO_AUDITORIA", "")).strip()
                # Si el estado es "Pendiente", ignoramos
                if estado in ("Pendiente", ""):
                    continue

                sync = str(row.get("SYNC_TELEGRAM", "")).strip().upper()
                # Si ya está marcado como OK, ignoramos
                if sync == "OK":
                    continue

                msg_id = str(row.get("MSG_ID_SUPERVISOR", "")).strip()
                chat_id = str(row.get("CHAT_ID_REF", "")).strip()

                # Si no tenemos referencias de Telegram, no podemos editar nada
                if not msg_id or not chat_id:
                    continue

                key = (chat_id, msg_id)
                if key not in grouped:
                    grouped[key] = {
                        "row_nums": [i + 2],
                        "uuid": row.get("UUID_REF"),
                        "estado": estado,
                        "chat_id": int(float(chat_id)),  # Usar float por si Google Sheets pone .0
                        "msg_id": int(float(msg_id)),
                        "cliente": row.get("CLIENTE"),
                        "tipo": row.get("TIPO_PDV"),
                        "vendedor": row.get("VENDEDOR"),
                        "comentarios": str(row.get("COMENTARIOS", "")).strip()
                    }
                else:
                    grouped[key]["row_nums"].append(i + 2)
            return list(grouped.values())
        except Exception as e:
            logger.error(f"Error en get_unsynced_actions: {e}")
            return []

    def mark_as_synced_rows(self, row_nums: List[int]) -> None:
        if not row_nums:
            return
        ws = self._get_ws("STATS")
        if not ws:
            return
        for rn in row_nums:
            try:
                ws.update_cell(rn, 14, "OK")
            except Exception:
                continue

    def get_pending_evaluations(self) -> List[Dict[str, Any]]:
        ws = self._get_ws("STATS")
        if not ws:
            return []
        try:
            rows = ws.get_all_records()
            pendientes = []
            for i, row in enumerate(rows):
                if str(row.get("ESTADO_AUDITORIA", "")).strip() == "Pendiente":
                    raw = str(row.get("LINK_FOTO", "")).strip()
                    url = raw if raw.startswith("http") else ""
                    if '"' in raw:
                        parts = raw.split('"')
                        if len(parts) > 1 and "http" in parts[1]:
                            url = parts[1]
                    pendientes.append({
                        "row_num": i + 2,
                        "uuid": row.get("UUID_REF"),
                        "cliente": row.get("CLIENTE"),
                        "vendedor": row.get("VENDEDOR"),
                        "url_foto": url,
                        "msg_id_telegram": row.get("MSG_ID_SUPERVISOR"),
                        "fecha": row.get("FECHA", ""),
                        "hora": row.get("HORA", ""),
                        "tipo": row.get("TIPO_PDV", ""),
                    })
            return pendientes
        except Exception as e:
            logger.error(f"❌ Error obteniendo pendientes: {e}")
            return []

    def update_evaluation_status(self, row_num: int, new_status: str, comments: str = "") -> str:
        ws = self._get_ws("STATS")
        if not ws:
            return "ERROR"
        try:
            try:
                current_status = ws.cell(row_num, 8).value
            except Exception:
                current_status = ""
            
            if current_status and current_status not in ("Pendiente", ""):
                return "LOCKED"

            ws.update_cell(row_num, 8, new_status)
            ws.update_cell(row_num, 9, comments)
            ws.update_cell(row_num, 14, "") 
            return "OK"
        except Exception:
            return "ERROR"

    def get_image_data_base64(self, drive_link: str) -> Optional[str]:
        if not drive_link or "drive.google.com" not in drive_link or not self.drive_service:
            return None
        try:
            if "id=" in drive_link:
                fid = drive_link.split("id=")[1].split("&")[0]
            elif "/d/" in drive_link:
                fid = drive_link.split("/d/")[1].split("/")[0]
            else:
                return None
            request = self.drive_service.files().get_media(fileId=fid)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return base64.b64encode(fh.getvalue()).decode("utf-8")
        except Exception:
            return None

    def get_all_hashes(self) -> List[str]:
        ws = self._get_ws("RAW_LOGS")
        if not ws:
            return []
        try:
            vals = ws.col_values(11)
            return vals[1:] if len(vals) > 1 else []
        except Exception:
            return []

    def _parse_ddmmyyyy(self, s: str) -> Optional[date]:
        s = (s or "").strip()
        if not s:
            return None
        try:
            return datetime.strptime(s, "%d/%m/%Y").date()
        except Exception:
            return None

    def _stats_from_rows(
        self,
        stats_rows: List[Dict[str, Any]],
        uuid_to_user: Dict[str, str],
        user_id: Optional[int],
        start_d: Optional[date],
        end_d: Optional[date],
    ) -> Dict[str, Any]:
        counts = {"aprobadas": 0, "destacadas": 0, "rechazadas": 0, "pendientes": 0, "total": 0, "puntos": 0}
        min_d: Optional[date] = None
        max_d: Optional[date] = None

        for row in stats_rows:
            uuid_ref = str(row.get("UUID_REF", "")).strip()
            if user_id is not None and uuid_to_user.get(uuid_ref) != str(user_id):
                continue

            d = self._parse_ddmmyyyy(str(row.get("FECHA", "")).strip())
            if d is None:
                continue

            if start_d and d < start_d:
                continue
            if end_d and d > end_d:
                continue

            min_d = d if min_d is None else min(min_d, d)
            max_d = d if max_d is None else max(max_d, d)

            estado = str(row.get("ESTADO_AUDITORIA", "")).strip()
            if estado == "Aprobado":
                counts["aprobadas"] += 1
                counts["puntos"] += 1
            elif estado == "Destacado":
                counts["destacadas"] += 1
                counts["aprobadas"] += 1
                counts["puntos"] += 2
            elif estado == "Rechazado":
                counts["rechazadas"] += 1
            else:
                counts["pendientes"] += 1
            counts["total"] += 1

        return {"counts": counts, "min_date": min_d, "max_date": max_d}

    def get_stats_report(self, user_id: Optional[int] = None) -> Dict[str, Any]:
        ws_stats = self._get_ws("STATS")
        ws_raw = self._get_ws("RAW_LOGS")
        if not ws_stats or not ws_raw:
            return {
                "historico": {
                    "counts": {"aprobadas": 0, "rechazadas": 0, "pendientes": 0, "total": 0},
                    "min_date": None,
                    "max_date": None,
                },
                "ultimo_mes": {
                    "counts": {"aprobadas": 0, "rechazadas": 0, "pendientes": 0, "total": 0},
                    "start_date": None,
                    "end_date": None,
                },
            }

        try:
            stats_rows = ws_stats.get_all_records()
        except Exception:
            stats_rows = []

        uuid_to_user: Dict[str, str] = {}
        try:
            raw_rows = ws_raw.get_all_records()
            for r in raw_rows:
                uid = str(r.get("ID_USER", "")).strip()
                uu = str(r.get("UUID", "")).strip()
                if uid and uu:
                    uuid_to_user[uu] = uid
        except Exception:
            pass

        today = datetime.now(AR_TZ).date()
        last30_start = today - timedelta(days=30)

        historico = self._stats_from_rows(
            stats_rows=stats_rows,
            uuid_to_user=uuid_to_user,
            user_id=user_id,
            start_d=None,
            end_d=None,
        )

        ultimo_mes = self._stats_from_rows(
            stats_rows=stats_rows,
            uuid_to_user=uuid_to_user,
            user_id=user_id,
            start_d=last30_start,
            end_d=today,
        )

        return {
            "historico": historico,
            "ultimo_mes": {
                "counts": ultimo_mes["counts"],
                "start_date": last30_start,
                "end_date": today,
            },
        }

    def get_ranking_report(self) -> List[Dict[str, Any]]:
        ws_stats = self._get_ws("STATS")
        ws_raw = self._get_ws("RAW_LOGS")
        if not ws_stats or not ws_raw:
            return []

        try:
            stats_rows = ws_stats.get_all_records()
        except Exception:
            stats_rows = []

        uuid_to_user: Dict[str, str] = {}
        user_to_name: Dict[str, str] = {}
        try:
            raw_rows = ws_raw.get_all_records()
            for r in raw_rows:
                uid = str(r.get("ID_USER", "")).strip()
                uu = str(r.get("UUID", "")).strip()
                uname = str(r.get("USER_NAME", "")).strip()
                if uid and uu:
                    uuid_to_user[uu] = uid
                if uid and uname:
                    user_to_name[uid] = uname
        except Exception:
            pass

        today = datetime.now(AR_TZ).date()
        month_start = today.replace(day=1)
        
        vendedor_stats: Dict[str, Dict[str, int]] = {}
        
        for row in stats_rows:
            uuid_ref = str(row.get("UUID_REF", "")).strip()
            user_id = uuid_to_user.get(uuid_ref, "")
            
            vendedor = user_to_name.get(user_id, str(row.get("VENDEDOR", "")).strip() or "Sin nombre")
            
            d = self._parse_ddmmyyyy(str(row.get("FECHA", "")).strip())
            if d is None or d < month_start or d > today:
                continue
            
            estado = str(row.get("ESTADO_AUDITORIA", "")).strip()
            
            if vendedor not in vendedor_stats:
                vendedor_stats[vendedor] = {
                    "puntos": 0, "aprobadas": 0, "destacadas": 0, "rechazadas": 0, "total": 0
                }
            
            if estado == "Aprobado":
                vendedor_stats[vendedor]["aprobadas"] += 1
                vendedor_stats[vendedor]["puntos"] += 1
                vendedor_stats[vendedor]["total"] += 1
            elif estado == "Destacado":
                vendedor_stats[vendedor]["destacadas"] += 1
                vendedor_stats[vendedor]["aprobadas"] += 1
                vendedor_stats[vendedor]["puntos"] += 2
                vendedor_stats[vendedor]["total"] += 1
            elif estado == "Rechazado":
                vendedor_stats[vendedor]["rechazadas"] += 1
                vendedor_stats[vendedor]["total"] += 1
        
        ranking = []
        for vendedor, stats in vendedor_stats.items():
            ranking.append({
                "vendedor": vendedor,
                "puntos": stats["puntos"],
                "aprobadas": stats["aprobadas"],
                "destacadas": stats["destacadas"],
                "rechazadas": stats["rechazadas"],
                "total": stats["total"]
            })
        
        ranking.sort(key=lambda x: x["puntos"], reverse=True)
        return ranking

    def get_semaforo_estado(self) -> Dict[str, Any]:
        ws = self._get_ws("BOT_CONTROL")
        if not ws:
            return {"estado": "LIBRE", "inicio": None, "archivos_total": 0, "progreso": "", "timestamp_lectura": datetime.now(AR_TZ)}
        try:
            valores = ws.get("A2:D2", default_blank="")[0] if ws.row_count >= 2 else []
            if len(valores) < 1:
                return {"estado": "LIBRE", "inicio": None, "archivos_total": 0, "progreso": "", "timestamp_lectura": datetime.now(AR_TZ)}
            
            estado = str(valores[0]).strip().upper() if len(valores) > 0 else "LIBRE"
            inicio = str(valores[1]).strip() if len(valores) > 1 else ""
            archivos = valores[2] if len(valores) > 2 else 0
            progreso = str(valores[3]).strip() if len(valores) > 3 else ""
            
            if estado not in ("LIBRE", "DISTRIBUYENDO"):
                estado = "LIBRE"
            
            return {
                "estado": estado,
                "inicio": inicio if inicio else None,
                "archivos_total": int(archivos) if archivos else 0,
                "progreso": progreso,
                "timestamp_lectura": datetime.now(AR_TZ)
            }
        except Exception as e:
            logger.error(f"Error semáforo: {e}")
            return {"estado": "LIBRE", "inicio": None, "archivos_total": 0, "progreso": "", "timestamp_lectura": datetime.now(AR_TZ)}

    def encolar_imagen_pendiente(self, chat_id: int, message_id: int, user_id: int, file_id: str, username: str = "", timestamp: Optional[datetime] = None) -> bool:
        ws = self._get_ws("COLA_IMAGENES")
        if not ws:
            ws = self._create_cola_imagenes_sheet()
            if not ws: return False
        
        try:
            ts = timestamp or datetime.now(AR_TZ)
            ws.append_row([str(uuid.uuid4()), str(chat_id), str(user_id), username, file_id, ts.strftime("%d/%m/%Y %H:%M:%S"), str(message_id), "NO"])
            return True
        except Exception:
            return False

    def get_imagenes_pendientes(self) -> List[Dict[str, Any]]:
        ws = self._get_ws("COLA_IMAGENES")
        if not ws: return []
        try:
            rows = ws.get_all_records()
            pendientes = []
            for i, row in enumerate(rows):
                if str(row.get("PROCESADO", "")).strip().upper() != "SI":
                    pendientes.append({
                        "row_num": i + 2,
                        "uuid": row.get("UUID_MSG"),
                        "chat_id": int(row.get("CHAT_ID", 0)),
                        "user_id": int(row.get("USER_ID", 0)),
                        "username": row.get("USERNAME", ""),
                        "file_id": row.get("FILE_ID", ""),
                        "timestamp": row.get("TIMESTAMP", ""),
                        "message_id": int(row.get("MSG_ID", 0))
                    })
            return pendientes
        except Exception:
            return []

    def marcar_imagen_procesada(self, row_num: int) -> bool:
        ws = self._get_ws("COLA_IMAGENES")
        if not ws: return False
        try:
            ws.update_cell(row_num, 8, "SI")
            return True
        except Exception:
            return False

    def limpiar_cola_imagenes(self) -> int:
        ws = self._get_ws("COLA_IMAGENES")
        if not ws: return 0
        try:
            rows = ws.get_all_records()
            filas = [i + 2 for i, r in enumerate(rows) if str(r.get("PROCESADO", "")).strip().upper() == "SI"]
            for row_num in reversed(filas):
                ws.delete_rows(row_num)
            return len(filas)
        except Exception:
            return 0

    def _create_cola_imagenes_sheet(self):
        try:
            ws = self.spreadsheet.add_worksheet(title="COLA_IMAGENES", rows=100, cols=8)
            ws.update("A1:H1", [["UUID_MSG", "CHAT_ID", "USER_ID", "USERNAME", "FILE_ID", "TIMESTAMP", "MSG_ID", "PROCESADO"]])
            return ws
        except Exception:
            return None