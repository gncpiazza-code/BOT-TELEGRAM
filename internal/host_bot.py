# -*- coding: utf-8 -*-
# file: host_bot.py
import os
import sys
import time
import logging
import asyncio
import signal
import socket
import getpass
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, BotCommand
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

# --- PARCHE CRÍTICO PARA WINDOWS ---
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        pass
# -----------------------------------

# ✅ DETECCIÓN ROBUSTA DE RUTAS
def _setup_paths():
    """Configura sys.path para encontrar módulos en cualquier escenario."""
    script_dir = Path(__file__).resolve().parent
    
    search_paths = [
        script_dir,
        script_dir / "src",
        script_dir / "CONFIG_GLOBAL",
        script_dir.parent,
        script_dir.parent / "src",
        script_dir.parent / "CONFIG_GLOBAL",
    ]
    
    for path in search_paths:
        if path.exists() and str(path) not in sys.path:
            sys.path.insert(0, str(path))
    
    print(f"[DEBUG] Script ejecutandose desde: {script_dir}")

_setup_paths()

# Zona horaria Argentina (para jobs y snapshots)
AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

if "REQUESTS_CA_BUNDLE" in os.environ:
    del os.environ['REQUESTS_CA_BUNDLE']
if "CURL_CA_BUNDLE" in os.environ:
    del os.environ['CURL_CA_BUNDLE']


try:
    from host_lock import HostLock
except ImportError:
    print("⚠️ No se encontró host_lock.py - Sistema de host único deshabilitado")
    HostLock = None

# ✅ Imports robustos
try:
    from src.sheets_manager import SheetsManager
except ImportError:
    from sheets_manager import SheetsManager

try:
    from src.anti_fraud import AntiFraudSystem
except ImportError:
    from anti_fraud import AntiFraudSystem

try:
    from semaforo_monitor import SemaforoMonitor
except ImportError:
    try:
        from src.semaforo_monitor import SemaforoMonitor
    except ImportError:
        print("⚠️ No se encontró semaforo_monitor.py.")
        raise

try:
    from config_manager import ConfigManager
except ImportError:
    from CONFIG_GLOBAL.config_manager import ConfigManager

try:
    from logger_config import setup_logging, get_logger, log_exception  # type: ignore
    setup_logging(log_level=logging.INFO, detailed=True)
    logger = get_logger(__name__)
except Exception:  # logger_config ausente o falló
    logging.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s | %(levelname)-8s | %(name)-15s | %(funcName)-20s | L%(lineno)-4d | %(message)s",
        level=logging.INFO,
    )
    logger = logging.getLogger("HostBot")

    def log_exception(*args: Any, **kwargs: Any) -> None:  # fallback
        logger.exception("Unhandled exception", exc_info=True)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    print("⚠️ logger_config.py no encontrado (o falló), usando logging básico")

print("=" * 80)
print("🤖 BOT HOST INICIANDO...")
print("=" * 80)
sys.stdout.flush()
logger.info("=" * 80)
logger.info("🤖 BOT HOST INICIANDO...")
logger.info("=" * 80)


cfg = ConfigManager()
sheets = SheetsManager()
semaforo = SemaforoMonitor(sheets, intervalo_segundos=15)

def log_and_print(message: str, level: str = "info"):
    print(message)
    sys.stdout.flush()
    if level == "info":
        logger.info(message)
    elif level == "warning":
        logger.warning(message)
    elif level == "error":
        logger.error(message)
    elif level == "debug":
        logger.debug(message)

antifraud = AntiFraudSystem()
BOT_OWNER_ID = str(cfg.get_telegram_config().get("admin_id") or "").strip()


# ============================================================================
# SISTEMA DE CACHE DE ROLES (24 HORAS)
# ============================================================================

role_cache: Dict[Tuple[int, int], str] = {}  # (chat_id, user_id) -> rol
role_cache_loaded_at: Optional[float] = None
ROLE_CACHE_TTL = 86400  # 24 horas en segundos


def load_roles_cache() -> None:
    """Carga todos los roles desde Sheets al cache en memoria."""
    global role_cache, role_cache_loaded_at
    
    logger.info("🔄 Cargando cache de roles desde GROUP_ROLES...")
    try:
        all_roles = sheets.get_all_group_roles()
        
        # Convertir lista a dict para búsqueda rápida
        role_cache = {}
        for role_info in all_roles:
            chat_id = role_info["chat_id"]
            user_id = role_info["user_id"]
            rol = role_info["rol"]
            role_cache[(chat_id, user_id)] = rol
        
        role_cache_loaded_at = time.time()
        logger.info(f"✅ Cache cargado: {len(role_cache)} asignaciones de roles")
    except Exception as e:
        logger.error(f"❌ Error cargando cache de roles: {e}")
        role_cache = {}
        role_cache_loaded_at = time.time()


def should_reload_role_cache() -> bool:
    """Verifica si el cache debe recargarse (después de 24hs)."""
    if role_cache_loaded_at is None:
        return True
    elapsed = time.time() - role_cache_loaded_at
    return elapsed >= ROLE_CACHE_TTL


def get_cached_role(chat_id: int, user_id: int) -> str:
    """
    Obtiene el rol de un usuario desde el cache en memoria.
    Si el cache expiró, lo recarga automáticamente.
    
    Returns:
        Rol del usuario: "vendedor", "supervisor", "observador"
    """
    # Superusuario siempre tiene permisos globales
    if str(user_id) == BOT_OWNER_ID:
        return "supervisor"  # Superusuario actúa como supervisor global
    
    if should_reload_role_cache():
        load_roles_cache()
    
    return role_cache.get((chat_id, user_id), "observador")


def invalidate_role_cache() -> None:
    """Invalida el cache forzando recarga en próxima consulta."""
    global role_cache_loaded_at
    logger.info("🔄 Cache de roles invalidado - se recargará en próxima consulta")
    role_cache_loaded_at = None


# ============================================================================
# SISTEMA DE HIBERNACIÓN CON SNAPSHOT (22:00-06:00 ARGENTINA)
# ============================================================================

hibernation_snapshot: Dict[str, Any] = {}


def is_hibernation_time() -> bool:
    """Verifica si estamos en horario de hibernación (22:00-06:00 Argentina)."""
    now_ar = datetime.now(AR_TZ)
    hour = now_ar.hour
    return hour >= 22 or hour < 6


async def take_hibernation_snapshot(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Toma snapshot de datos antes de hibernar (para /stats y /ranking)."""
    global hibernation_snapshot
    
    logger.info("📸 Tomando snapshot para hibernación...")
    
    try:
        # Cargar ranking y dejarlo en memoria
        ranking = sheets.get_ranking_report()
        
        hibernation_snapshot = {
            "timestamp": datetime.now(AR_TZ).strftime("%d/%m/%Y %H:%M:%S"),
            "ranking": ranking,
            "stats_cache": {}  # Se llenará bajo demanda durante hibernación
        }
        
        logger.info(f"✅ Snapshot tomado: {len(ranking)} vendedores en ranking")
    except Exception as e:
        logger.error(f"❌ Error tomando snapshot: {e}")
        hibernation_snapshot = {
            "timestamp": "ERROR", 
            "ranking": [], 
            "stats_cache": {}
        }


async def handle_hibernation_start(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Inicia hibernación a las 22:00.
    Job programado que se ejecuta automáticamente.
    """
    global bot_hibernating
    if host_lock and not host_lock.is_host:
        return
    
    if bot_hibernating:
        return
    
    bot_hibernating = True
    logger.info("🌙 ======== HIBERNACIÓN INICIADA (22:00-06:00) ========")
    
    # Tomar snapshot
    await take_hibernation_snapshot(context)
    
    # Notificar superusuario (si existe notify_superuser)
    try:
        await notify_superuser(
            context,
            "🌙 <b>Bot en Hibernación</b>\n\n"
            f"Horario: 22:00-06:00 (hora Argentina)\n"
            f"📸 Snapshot tomado a las {hibernation_snapshot.get('timestamp', '-')}\n\n"
            "Durante este tiempo:\n"
            "• ❌ No se procesan exhibiciones\n"
            "• ✅ Responden /stats y /ranking (con datos del snapshot)\n"
            "• ❌ Otros comandos deshabilitados",
            parse_mode=ParseMode.HTML
        )
    except:
        pass


async def handle_hibernation_end(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Termina hibernación a las 06:00.
    Job programado que se ejecuta automáticamente.
    """
    global bot_hibernating, hibernation_snapshot
    if host_lock and not host_lock.is_host:
        return
    
    if not bot_hibernating:
        return
    
    bot_hibernating = False
    hibernation_snapshot = {}
    logger.info("☀️ ======== HIBERNACIÓN FINALIZADA (06:00) ========")
    
    # Forzar recarga de cache de roles
    invalidate_role_cache()
    
    # Notificar superusuario
    try:
        await notify_superuser(
            context,
            "☀️ <b>Bot Operativo</b>\n\n"
            "Hibernación terminada\n"
            "✅ Todos los sistemas activos",
            parse_mode=ParseMode.HTML
        )
    except:
        pass


# ============================================================================
# REGISTRO AUTOMÁTICO DE KNOWN USERS
# ============================================================================

def register_user_interaction(chat_id: int, user_id: int, username: str, full_name: str) -> None:
    """
    Registra que un usuario interactuó en un grupo (auto-registro en KNOWN_USERS).
    Llamar en TODOS los handlers de mensajes, fotos, y comandos.
    
    Args:
        chat_id: ID del grupo (negativo)
        user_id: ID del usuario
        username: Username del usuario (sin @)
        full_name: Nombre completo del usuario
    """
    # Solo en grupos (chat_id negativo)
    if chat_id >= 0:
        return
    
    try:
        # Registrar en KNOWN_USERS (actualiza LAST_SEEN si ya existe)
        sheets.register_known_user(
            chat_id=chat_id,
            user_id=user_id,
            username=username or "",
            full_name=full_name or "Usuario"
        )
    except Exception as e:
        # Error silencioso, no bloquear el flujo
        logger.debug(f"Error registrando known user: {e}")


# ============================================================================
# CONFIGURACIÓN INICIAL DEL BOT
# ============================================================================

async def post_init_extensions(application: Application) -> None:
    """
    Extensión de post_init para cargar cache de roles y configurar hibernación.
    Llamar AL FINAL de la función post_init() existente.
    """
    logger.info("🔧 Inicializando extensiones del bot...")
    
    # 1. Cargar cache de roles
    load_roles_cache()
    
    # 2. Verificar si estamos en horario de hibernación al iniciar
    if is_hibernation_time():
        global bot_hibernating
        bot_hibernating = True
        logger.warning("🌙 Bot iniciado durante horario de hibernación (22:00-06:00)")
        await take_hibernation_snapshot(application)
    
    # 3. Programar jobs de hibernación
    try:
        # ========================================
        # 🧪 MODO TEST: Descomenta estas líneas para testear con horarios automáticos
        # ========================================
        # DESCOMENTAR PARA TEST (hibernación en 2 minutos):
        # hora_test_inicio = (datetime.now(AR_TZ) + timedelta(minutes=2)).time()
        # hora_test_fin = (datetime.now(AR_TZ) + timedelta(minutes=4)).time()
        # logger.warning(f"🧪 MODO TEST: Hibernación en 2 min ({hora_test_inicio}), despertar en 4 min ({hora_test_fin})")

        # ========================================
        # ✅ PRODUCCIÓN: Jobs con timezone correcto
        # ========================================
        # Job para INICIAR hibernación a las 22:00 Argentina
        application.job_queue.run_daily(
            handle_hibernation_start,
            time=datetime.strptime("22:00", "%H:%M").time(),
            timezone=AR_TZ,  # ← FIX: Timezone Argentina
            name="hibernation_start"
        )

        # Job para TERMINAR hibernación a las 06:00 Argentina
        application.job_queue.run_daily(
            handle_hibernation_end,
            time=datetime.strptime("06:00", "%H:%M").time(),
            timezone=AR_TZ,  # ← FIX: Timezone Argentina
            name="hibernation_end"
        )

        # ========================================
        # 🧪 PARA TEST: Reemplaza los jobs de arriba por estos (descomentar)
        # ========================================
        # application.job_queue.run_daily(
        #     handle_hibernation_start,
        #     time=hora_test_inicio,
        #     timezone=AR_TZ,
        #     name="hibernation_start"
        # )
        #
        # application.job_queue.run_daily(
        #     handle_hibernation_end,
        #     time=hora_test_fin,
        #     timezone=AR_TZ,
        #     name="hibernation_end"
        # )

        logger.info("✅ Jobs de hibernación programados (22:00-06:00 Argentina)")
    except Exception as e:
        logger.error(f"❌ Error programando jobs de hibernación: {e}")

    logger.info("✅ Extensiones del bot inicializadas")


# ============================================================================
# MODIFICACIÓN DE JOBS EXISTENTES
# ============================================================================

# IMPORTANTE: Los jobs existentes deben verificar hibernación al inicio:
#
# async def sync_telegram_job(context: ContextTypes.DEFAULT_TYPE) -> None:
#     """Sync job - SE PAUSA durante hibernación."""
#     if bot_hibernating:
#         return  # Salir inmediatamente si está hibernando
#     
#     # ... resto del código original ...
#
# async def procesar_cola_imagenes_pendientes(context: ContextTypes.DEFAULT_TYPE) -> None:
#     """Procesamiento de cola - SE PAUSA durante hibernación."""
#     if bot_hibernating:
#         return  # Salir inmediatamente si está hibernando
#     
#     # ... resto del código original ...
#
# NOTA: update_host_heartbeat DEBE seguir ejecutándose para mantener el host activo.


# Variable global para acceder al bot en callbacks
_global_app = None


async def _ensure_bot_ready(bot) -> None:
    """Ensure PTB bot transport is initialized before sending messages.

    Guards against: RuntimeError('This HTTPXRequest is not initialized!').
    """
    try:
        if getattr(bot, '_initialized', False):
            return
        if hasattr(bot, 'initialize'):
            await bot.initialize()
    except Exception:
        return

async def host_event_callback(event_type: str, message: str):
    """
    Callback para notificaciones de eventos de host.
    """
    try:
        if _global_app and BOT_OWNER_ID:
            await _ensure_bot_ready(_global_app.bot)
            await _global_app.bot.send_message(
                chat_id=int(BOT_OWNER_ID),
                text=f"🔐 <b>EVENTO DE HOST</b>\n\n{message}",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"📬 Notificación de host enviada: {event_type}")
    except Exception as e:
        logger.error(f"Error enviando notificación de host: {e}")


upload_sessions: Dict[int, Dict[str, Any]] = {}
active_transactions: Dict[int, Dict[str, Any]] = {}

STAGE_WAITING_ID = "WAITING_ID"
STAGE_WAITING_TYPE = "WAITING_TYPE"

# =============================================================================
# POS TYPES CACHE (evita bloqueo por Google Sheets)
# =============================================================================
POS_TYPES_CACHE_TTL_SECONDS = int(os.getenv("POS_TYPES_CACHE_TTL_SECONDS", "14400"))  # 4h
_pos_types_cache: Dict[str, Any] = {"expires_at": 0.0, "types": []}
_pos_types_lock = asyncio.Lock()

async def get_pos_types_cached(*, force: bool = False) -> List[str]:
    """
    Devuelve la lista de tipos de PDV, cacheada por TTL.

    Motivo:
        sheets.get_pos_types() es síncrono (Google Sheets) y puede demorar varios segundos.
        En handlers async (PTB), eso bloquea el event loop y provoca timeouts HTTP hacia Telegram.
    """
    now = time.time()
    cached: List[str] = _pos_types_cache.get("types") or []
    if not force and cached and now < float(_pos_types_cache.get("expires_at") or 0.0):
        return cached

    async with _pos_types_lock:
        now = time.time()
        cached = _pos_types_cache.get("types") or []
        if not force and cached and now < float(_pos_types_cache.get("expires_at") or 0.0):
            return cached

        try:
            tipos: List[str] = await asyncio.to_thread(sheets.get_pos_types)
        except Exception as e:
            logger.error(f"❌ Error refrescando tipos de PDV: {e}", exc_info=True)
            return cached

        if tipos:
            _pos_types_cache["types"] = tipos
            _pos_types_cache["expires_at"] = now + POS_TYPES_CACHE_TTL_SECONDS
            return tipos

        return cached

async def refresh_pos_types_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    # Warm-up / refresh periódico
    await get_pos_types_cached(force=True)


start_time = time.time()
session_stats = {"procesadas": 0, "aprobadas": 0, "rechazadas": 0}

active_last_prompt: Dict[Tuple[int, int], int] = {}
bot_hibernating = False
host_lock = None
bot_in_monitoring_mode = False


HELP_TEXT = (
    "📘 <b>Ayuda - Bot de Auditoría</b>\n\n"
    "📸 <b>Para vendedores:</b>\n"
    "1) Enviá una foto al grupo\n"
    "2) El bot te pedirá el <b>NRO CLIENTE</b> (solo números)\n"
    "3) Elegí el <b>tipo</b> de PDV en los botones\n"
    "4) Un supervisor aprueba/rechaza la foto\n\n"
    "📊 <b>Comandos disponibles:</b>\n"
    "• /help - Esta ayuda\n"
    "• /ranking - Ver ranking del mes\n"
    "• /stats - Ver tus estadísticas\n\n"
    "⚙️ <b>Comandos Admin:</b>\n"
    "• /status - Estado del bot\n"
    "• /misgrupos - Lista de grupos\n"
    "• /id - ID del chat actual\n"
    "• /set_role {rol} - Asignar rol a usuario\n\n"
    "🔧 <b>Superusuario:</b>\n"
    "• /reset - Limpiar memoria (suave)\n"
    "• /hardreset - Reiniciar bot (completo)"
)


def _uptime_hhmmss() -> str:
    return str(timedelta(seconds=int(time.time() - start_time)))

async def notify_superuser(context: ContextTypes.DEFAULT_TYPE, message: str, parse_mode=ParseMode.HTML) -> bool:
    """Envía una notificación privada al superusuario (BOT_OWNER_ID)."""
    if not BOT_OWNER_ID:
        logger.warning("⚠️ No se puede notificar: BOT_OWNER_ID no configurado")
        return False
    try:
        await _ensure_bot_ready(context.bot)
        await context.bot.send_message(chat_id=int(BOT_OWNER_ID), text=message, parse_mode=parse_mode)
        logger.info(f"📬 Notificación enviada al superusuario")
        return True
    except RuntimeError as e:
        if 'HTTPXRequest is not initialized' in str(e):
            try:
                await _ensure_bot_ready(context.bot)
                await context.bot.send_message(chat_id=int(BOT_OWNER_ID), text=message, parse_mode=parse_mode)
                logger.info('📬 Notificación enviada al superusuario')
                return True
            except Exception as e2:
                logger.error(f"Error al notificar al superusuario: {e2}")
                return False
        logger.error(f"Error al notificar al superusuario: {e}")
        return False
    except Exception as e:
        logger.error(f"Error al notificar al superusuario: {e}")
        return False


# ==========================================
# COMANDOS
# ==========================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    
    # Registrar usuario
    chat_id = update.message.chat.id
    user_id = update.message.from_user.id
    username = update.message.from_user.username or ""
    full_name = update.message.from_user.first_name or "Usuario"
    register_user_interaction(chat_id, user_id, username, full_name)
    
    await update.message.reply_text(
        "¡Hola! Soy el bot de auditoría de PDV.\n"
        "Usa /help para ver cómo funciono."
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Muestra ayuda con imagen de cómo usar el bot.
    Envía la foto de 'Launcher/img/uso_del_bot.png' con caption.
    """
    if not update.message:
        return
    
    # Buscar foto de ayuda en varias ubicaciones posibles
    help_image_path = None
    
    # Base path: directorio del ejecutable o del script
    base_path = (
        os.path.dirname(sys.executable) 
        if getattr(sys, "frozen", False) 
        else os.path.dirname(os.path.abspath(__file__))
    )
    
    # Candidatos de ubicación
    candidates = [
        os.path.join(base_path, "Launcher", "img", "uso_del_bot.png"),
        os.path.join(base_path, "..", "Launcher", "img", "uso_del_bot.png"),
        os.path.join(base_path, "img", "uso_del_bot.png"),
        os.path.join(base_path, "assets", "uso_del_bot.png"),
    ]
    
    for path in candidates:
        if os.path.exists(path):
            help_image_path = path
            logger.info(f"📸 Imagen de ayuda encontrada: {path}")
            break
    
    # Texto de ayuda
    help_text = (
        "📘 <b>Cómo usar el bot</b>\n\n"
        "<b>🛒 Para Vendedores:</b>\n"
        "1️⃣ Tomá una foto del PDV\n"
        "2️⃣ Enviala al grupo\n"
        "3️⃣ El bot te pedirá el <b>NRO CLIENTE</b>\n"
        "4️⃣ Seleccioná el <b>tipo de PDV</b>\n"
        "5️⃣ Un supervisor evalúa tu exhibición\n\n"
        "<b>👁️ Para Supervisores:</b>\n"
        "• Presioná los botones para aprobar/rechazar\n"
        "• Podés agregar comentarios opcionales\n\n"
        "<b>📊 Comandos disponibles:</b>\n"
        "• /stats - Tus estadísticas\n"
        "• /ranking - Ranking del mes\n"
        "• /mirol - Ver tus roles\n"
        "• /help - Esta ayuda\n\n"
        "💡 <b>Tip:</b> Solo los vendedores pueden enviar exhibiciones"
    )
    
    try:
        if help_image_path and os.path.exists(help_image_path):
            # Enviar foto con caption
            with open(help_image_path, 'rb') as photo:
                await update.message.reply_photo(
                    photo=photo,
                    caption=help_text,
                    parse_mode=ParseMode.HTML
                )
        else:
            # Solo texto si no hay foto
            logger.warning("⚠️ No se encontró imagen de ayuda en ninguna ubicación")
            await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)
    
    except Exception as e:
        logger.error(f"Error en /help: {e}")
        # Fallback a texto simple
        await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)



async def cmd_mirol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra los roles del usuario en todos los grupos donde participa."""
    if not update.message:
        return
    
    user_id = update.message.from_user.id
    chat_id = update.message.chat.id
    
    try:
        # Obtener todos los roles (puede retornar [] si la hoja está vacía)
        all_roles = sheets.get_all_group_roles()
        
        # Caso especial: Si no hay NINGÚN rol en el sistema
        if not all_roles:
            await update.message.reply_text(
                "👤 <b>Sistema de Roles</b>\n\n"
                "📋 El sistema de roles está iniciándose.\n\n"
                "🔹 Si enviás una <b>foto</b>, te registraré automáticamente como <b>VENDEDOR</b>.\n"
                "🔹 El administrador podrá asignar roles específicos con /setall_rol.\n\n"
                "💡 <b>Tip:</b> Enviá una foto para comenzar.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Filtrar roles del usuario actual
        user_roles = []
        for role_info in all_roles:
            if role_info.get("user_id") == user_id:
                role_chat_id = role_info.get("chat_id")
                rol = role_info.get("rol", "sin_rol")
                
                # Obtener nombre del grupo
                try:
                    chat = await context.bot.get_chat(role_chat_id)
                    group_name = chat.title or f"Grupo {role_chat_id}"
                except:
                    group_name = f"Grupo {role_chat_id}"
                
                user_roles.append({
                    "group_name": group_name,
                    "rol": rol
                })
        
        if not user_roles:
            # Usuario no tiene roles, pero hay roles en el sistema
            await update.message.reply_text(
                "👤 <b>Tus Roles</b>\n\n"
                "📋 Aún no tenés roles asignados en ningún grupo.\n\n"
                "🔹 Si enviás una <b>foto</b>, te registraré automáticamente como <b>VENDEDOR</b>.\n"
                "🔹 El administrador puede asignarte un rol específico con /setall_rol.\n\n"
                "💡 <b>Tip:</b> Enviá una foto para comenzar.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Construir mensaje con roles
        msg_text = "👤 <b>Tus Roles</b>\n\n"
        
        for role_info in user_roles:
            emoji = {
                "vendedor": "🛒",
                "supervisor": "👁️",
                "observador": "📋",
                "admin": "⚙️"
            }.get(role_info["rol"], "❓")
            
            msg_text += f"{emoji} <b>{role_info['group_name']}</b>\n"
            msg_text += f"   Rol: {role_info['rol'].capitalize()}\n\n"
        
        msg_text += (
            "<b>Significado de los roles:</b>\n"
            "🛒 <b>Vendedor</b>: Puede enviar exhibiciones\n"
            "👁️ <b>Supervisor</b>: Puede evaluar exhibiciones\n"
            "📋 <b>Observador</b>: Solo puede ver\n"
            "⚙️ <b>Admin</b>: Gestión completa"
        )
        
        await update.message.reply_text(msg_text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Error en /mirol: {e}", exc_info=True)
        await update.message.reply_text(
            "👤 <b>Sistema de Roles</b>\n\n"
            "📋 El sistema se está inicializando.\n\n"
            "🔹 Enviá una <b>foto</b> para registrarte automáticamente como <b>VENDEDOR</b>.\n"
            "🔹 El administrador podrá configurar roles con /setall_rol.\n\n"
            "💡 Todo funcionará automáticamente una vez que alguien envíe la primera foto.",
            parse_mode=ParseMode.HTML
        )



async def setup_bot_commands(application: Application) -> None:
    """
    Configura el menú de comandos de Telegram automáticamente.
    Llamar en post_init() después de inicializar el bot.
    """
    try:
        commands = [
            BotCommand("start", "Iniciar el bot"),
            BotCommand("help", "Cómo usar el bot"),
            BotCommand("mirol", "Ver mis roles"),
            BotCommand("stats", "Mis estadísticas"),
            BotCommand("ranking", "Ranking del mes"),
        ]
        
        # Solo superusuario puede ver comandos de administración
        # (No se agregan al menú público, pero existen)
        
        await application.bot.set_my_commands(commands)
        logger.info("✅ Menú de comandos configurado")
    except Exception as e:
        logger.error(f"❌ Error configurando menú de comandos: {e}")



async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    uid = update.message.from_user.id
    is_su = (str(uid) == BOT_OWNER_ID)
    
    role = "Superusuario" if is_su else "Usuario"
    uptime = _uptime_hhmmss()
    
    host_info = "N/A"
    if host_lock:
        if host_lock.is_host:
            host_info = f"✅ HOST ACTIVO\n   {host_lock.identity}"
        elif bot_in_monitoring_mode:
            info = host_lock.get_host_info()
            pos = None
            for q in info.get("queue", []):
                if host_lock._is_same_machine(q["identity"]):
                    pos = q["position"]
                    break
            host_info = f"⏸️ EN COLA (Posición {pos})\n   {host_lock.identity}"
        else:
            host_info = f"⏳ ESPERANDO\n   {host_lock.identity}"
    
    msg = (
        f"🤖 <b>Estado del Bot</b>\n\n"
        f"👤 <b>Tu rol:</b> {role}\n"
        f"⏱️ <b>Uptime:</b> {uptime}\n"
        f"🔐 <b>Host:</b>\n{host_info}\n\n"
        f"📊 <b>Sesión actual:</b>\n"
        f"   • Procesadas: {session_stats['procesadas']}\n"
        f"   • Aprobadas: {session_stats['aprobadas']}\n"
        f"   • Rechazadas: {session_stats['rechazadas']}"
    )
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    chat_id = update.message.chat.id
    chat_type = update.message.chat.type
    await update.message.reply_text(
        f"Chat ID: <code>{chat_id}</code>\nTipo: {chat_type}",
        parse_mode=ParseMode.HTML
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra estadísticas del vendedor - usa snapshot durante hibernación."""
    if not update.message: 
        return
    
    # Registrar usuario
    chat_id = update.message.chat.id
    uid = update.message.from_user.id
    username = update.message.from_user.username or ""
    full_name = update.message.from_user.first_name or "Usuario"
    register_user_interaction(chat_id, uid, username, full_name)
    
    # Durante hibernación, usar snapshot si está disponible
    if bot_hibernating and hibernation_snapshot and uid in hibernation_snapshot.get("stats_cache", {}):
        cached_msg = hibernation_snapshot["stats_cache"][uid]
        await update.message.reply_text(
            f"🌙 <b>Datos del snapshot de hibernación</b>\n"
            f"({hibernation_snapshot['timestamp']})\n\n{cached_msg}",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Modo normal o primera consulta en hibernación
    try:
        report = sheets.get_stats_report(user_id=uid)
        hist = report["historico"]
        mes = report["ultimo_mes"]
        
        msg = (
            f"📊 <b>Tus Estadísticas</b>\n\n"
            f"📅 <b>Histórico total:</b>\n"
            f"   • Aprobadas: {hist['counts']['aprobadas']}\n"
            f"   • Rechazadas: {hist['counts']['rechazadas']}\n"
            f"   • Pendientes: {hist['counts']['pendientes']}\n"
            f"   • Total: {hist['counts']['total']}\n\n"
            f"🗓️ <b>Último mes:</b>\n"
            f"   • Aprobadas: {mes['counts']['aprobadas']}\n"
            f"   • Rechazadas: {mes['counts']['rechazadas']}\n"
            f"   • Pendientes: {mes['counts']['pendientes']}\n"
            f"   • Total: {mes['counts']['total']}"
        )
        
        # Si está hibernando, cachear para futuras consultas
        if bot_hibernating:
            hibernation_snapshot.setdefault("stats_cache", {})[uid] = msg
        
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error en cmd_stats: {e}")
        await update.message.reply_text("❌ Error al obtener estadísticas.")

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    uid = update.message.from_user.id
    if str(uid) != BOT_OWNER_ID:
        await update.message.reply_text("❌ Solo el superusuario puede ejecutar /reset")
        return
    
    upload_sessions.clear()
    active_transactions.clear()
    active_last_prompt.clear()
    
    await update.message.reply_text("✅ Memoria limpiada (reset suave)")
    logger.info(f"Reset ejecutado por SU {uid}")

async def cmd_hardreset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    uid = update.message.from_user.id
    if str(uid) != BOT_OWNER_ID:
        await update.message.reply_text("❌ Solo el superusuario puede ejecutar /hardreset")
        return
    
    await update.message.reply_text("🔄 Reiniciando bot...")
    logger.warning(f"Hard reset solicitado por SU {uid}")
    
    if host_lock and host_lock.is_host:
        logger.info("Liberando host antes de reiniciar...")
        host_lock.release_host()
    
    await asyncio.sleep(1)
    os._exit(0)

async def cmd_misgrupos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    uid = update.message.from_user.id
    if str(uid) != BOT_OWNER_ID:
        await update.message.reply_text("❌ Solo el superusuario puede ver grupos")
        return
    
    try:
        chats = await context.bot.get_updates()
        unique_chats = set()
        for upd in chats:
            if upd.message and upd.message.chat:
                unique_chats.add((upd.message.chat.id, upd.message.chat.title or "Sin título"))
        
        if not unique_chats:
            await update.message.reply_text("📭 No hay chats recientes")
            return
        
        msg = "📋 <b>Grupos/Chats detectados:</b>\n\n"
        for cid, title in sorted(unique_chats):
            msg += f"• <b>{title}</b>\n  ID: <code>{cid}</code>\n\n"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error en cmd_misgrupos: {e}")
        await update.message.reply_text("❌ Error al listar grupos")

async def cmd_ranking(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra ranking del mes - usa snapshot durante hibernación."""
    if not update.message: 
        return
    
    # Registrar usuario
    chat_id = update.message.chat.id
    user_id = update.message.from_user.id
    username = update.message.from_user.username or ""
    full_name = update.message.from_user.first_name or "Usuario"
    register_user_interaction(chat_id, user_id, username, full_name)
    
    # Durante hibernación, usar snapshot
    if bot_hibernating and hibernation_snapshot:
        ranking = hibernation_snapshot.get("ranking", [])
        timestamp = hibernation_snapshot.get("timestamp", "-")
        
        if not ranking:
            await update.message.reply_text("📊 No hay datos de ranking en el snapshot.")
            return
        
        msg = f"🌙 <b>Ranking del snapshot de hibernación</b>\n({timestamp})\n\n"
    else:
        # Modo normal
        try:
            ranking = sheets.get_ranking_report()
        except Exception as e:
            logger.error(f"Error en cmd_ranking: {e}")
            await update.message.reply_text("❌ Error al obtener ranking")
            return
        
        if not ranking:
            await update.message.reply_text("📊 No hay datos de ranking aún.")
            return
        
        msg = "🏆 <b>RANKING DEL MES</b>\n\n"
    
    # Mostrar top 10
    for i, entry in enumerate(ranking[:10], 1):
        emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        
        msg += (
            f"{emoji} <b>{entry['vendedor']}</b>\n"
            f"   💎 Puntos: {entry['puntos']}\n"
            f"   ✅ Aprobadas: {entry['aprobadas']}"
        )
        
        if entry['destacadas'] > 0:
            msg += f" (🔥 {entry['destacadas']} destacadas)"
        
        if entry['rechazadas'] > 0:
            msg += f"\n   ❌ Rechazadas: {entry['rechazadas']}"
        
        msg += f"\n   📊 Total: {entry['total']}\n\n"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def cmd_set_role(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    uid = update.message.from_user.id
    if str(uid) != BOT_OWNER_ID:
        await update.message.reply_text("❌ Solo el superusuario puede asignar roles")
        return

    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            "Uso: /set_role {supervisor|vendedor|observador}\n"
            "Respondé al mensaje de un usuario para asignarle el rol.",
            parse_mode=ParseMode.HTML
        )
        return

    role = context.args[0].lower()
    if role not in ["supervisor", "vendedor", "observador"]:
        await update.message.reply_text("❌ Rol inválido. Usa: supervisor, vendedor u observador")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Responde al mensaje de un usuario para asignarle el rol")
        return

    chat_id = update.message.chat.id
    target_user = update.message.reply_to_message.from_user
    target_id = target_user.id
    target_name = target_user.first_name or "Usuario"
    target_username = target_user.username or ""
    supervisor_name = update.message.from_user.first_name or "Superusuario"

    try:
        success = sheets.set_user_role_in_group(
            chat_id=chat_id,
            user_id=target_id,
            username=target_username,
            full_name=target_name,
            rol=role,
            asignado_por=supervisor_name
        )

        if success:
            # Actualizar cache local inmediatamente
            role_cache[(chat_id, target_id)] = role
            invalidate_role_cache()

            emoji = {"vendedor": "🛒", "supervisor": "👁️", "observador": "📋"}.get(role, "❓")
            await update.message.reply_text(
                f"✅ Rol {emoji} <b>{role}</b> asignado a <b>{target_name}</b> (ID: <code>{target_id}</code>)\n\n"
                f"💡 El cambio es efectivo inmediatamente.",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Rol {role} asignado a {target_id} ({target_name}) por SU en grupo {chat_id}")
        else:
            await update.message.reply_text(
                "❌ Error al guardar el rol en Google Sheets.\n"
                "Intentá de nuevo en unos segundos.",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.error(f"Error en cmd_set_role: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Error al asignar rol: {e}",
            parse_mode=ParseMode.HTML
        )


# ==========================================
# HANDLERS DE FOTOS Y TEXTO
# ==========================================



# ============================================================================
# ESTADO DE SESIÓN PARA /setall_rol
# ============================================================================

# Dict temporal para guardar el estado del flujo de /setall_rol
# Key: user_id del superusuario, Value: dict con estado de la configuración
setall_rol_sessions: Dict[int, Dict[str, Any]] = {}


# ============================================================================
# COMANDO /setall_rol - CONFIGURAR ROLES EN GRUPO
# ============================================================================

async def cmd_setall_rol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Configura roles en el grupo paso a paso.
    Solo superusuario puede ejecutar este comando.
    
    Flujo:
    1. Lista usuarios conocidos del grupo
    2. Itera uno por uno preguntando el rol
    3. Muestra resumen final y pide confirmación
    4. Guarda cambios en GROUP_ROLES
    """
    if not update.message:
        return
    
    user_id = update.message.from_user.id
    chat_id = update.message.chat.id
    
    # Solo superusuario
    if str(user_id) != BOT_OWNER_ID:
        await update.message.reply_text(
            "❌ Solo el superusuario puede configurar roles.\n\n"
            "💡 Pedile al administrador que te asigne un rol.",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Solo en grupos
    if chat_id >= 0:
        await update.message.reply_text(
            "❌ Este comando solo funciona en grupos.",
            parse_mode=ParseMode.HTML
        )
        return
    
    logger.info(f"🎭 /setall_rol iniciado por superusuario en grupo {chat_id}")
    
    try:
        # Obtener usuarios conocidos del grupo
        known_users = sheets.get_known_users_in_group(chat_id)
        
        if not known_users or len(known_users) == 0:
            await update.message.reply_text(
                "👥 <b>Sistema de Roles - Grupo Vacío</b>\n\n"
                "📋 Aún no hay usuarios registrados en este grupo.\n\n"
                "<b>¿Cómo funciona el registro automático?</b>\n\n"
                "🔹 Cualquier persona que <b>escriba en el grupo</b> se registra automáticamente.\n"
                "🔹 Cualquiera que envíe una <b>foto</b> se auto-asigna como <b>VENDEDOR</b>.\n\n"
                "<b>Para comenzar:</b>\n"
                "1️⃣ Pedí que alguien escriba algo en el grupo (ej: \"Hola\")\n"
                "2️⃣ Volvé a ejecutar /setall_rol\n"
                "3️⃣ Configurá los roles de forma interactiva\n\n"
                "💡 <b>Tip:</b> También podés esperar a que alguien envíe una foto - se auto-asignará como vendedor.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Obtener roles actuales (puede ser [] si la hoja está vacía)
        all_roles = sheets.get_all_group_roles()
        roles_map = {}
        
        if all_roles:
            for role_info in all_roles:
                if role_info.get("chat_id") == chat_id:
                    roles_map[role_info.get("user_id")] = role_info.get("rol", "Sin asignar")
        
        # Construir lista de usuarios con sus roles actuales
        users_list = []
        for user in known_users:
            uid = user.get("user_id")
            username = user.get("username", "")
            full_name = user.get("full_name", "")
            current_rol = roles_map.get(uid, "Sin asignar")
            
            users_list.append({
                "user_id": uid,
                "username": username,
                "full_name": full_name or username or f"User {uid}",
                "current_rol": current_rol
            })
        
        # Guardar sesión
        setall_rol_sessions[user_id] = {
            "chat_id": chat_id,
            "users": users_list,
            "current_index": 0,
            "changes": []
        }
        
        # Enviar mensaje informativo antes de comenzar
        await update.message.reply_text(
            f"🎭 <b>Configuración de Roles</b>\n\n"
            f"👥 Usuarios detectados: <b>{len(users_list)}</b>\n\n"
            f"Voy a preguntarte el rol de cada uno, uno por uno.\n"
            f"Luego te mostraré un resumen antes de guardar los cambios.\n\n"
            f"¡Comenzamos! 👇",
            parse_mode=ParseMode.HTML
        )
        
        # Enviar configuración del primer usuario
        await send_role_config_for_user(update, context, user_id, 0)
        
    except Exception as e:
        logger.error(f"Error en /setall_rol: {e}", exc_info=True)
        await update.message.reply_text(
            "⚠️ <b>Error al inicializar el sistema</b>\n\n"
            "Esto puede ocurrir si es la primera vez que se usa el bot.\n\n"
            "<b>Solución:</b>\n"
            "1️⃣ Pedí que alguien escriba algo en el grupo\n"
            "2️⃣ O que alguien envíe una foto (se auto-registrará como vendedor)\n"
            "3️⃣ Volvé a ejecutar /setall_rol\n\n"
            "💡 El sistema se inicializará automáticamente con la primera interacción.",
            parse_mode=ParseMode.HTML
        )


async def send_role_config_for_user(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE, 
    user_id: int, 
    index: int
) -> None:
    """Envía la configuración de rol para un usuario específico."""
    session = setall_rol_sessions.get(user_id)
    if not session:
        return
    
    users = session["users"]
    
    if index >= len(users):
        # Terminado, mostrar resumen
        await show_role_config_summary(update, context, user_id)
        return
    
    user = users[index]
    total = len(users)
    user_ref = f"@{user['username']}" if user['username'] else f"ID: {user['user_id']}"

    # Botones de roles
    keyboard = [
        [
            InlineKeyboardButton("🛒 Vendedor", callback_data=f"ROL_vendedor_{user['user_id']}"),
            InlineKeyboardButton("👁️ Supervisor", callback_data=f"ROL_supervisor_{user['user_id']}"),
        ],
        [
            InlineKeyboardButton("📋 Observador", callback_data=f"ROL_observador_{user['user_id']}"),
        ],
        [
            InlineKeyboardButton("⏩ Mantener actual", callback_data=f"ROL_mantener_{user['user_id']}"),
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Emoji del rol actual
    current_emoji = {
        "vendedor": "🛒",
        "supervisor": "👁️",
        "observador": "📋",
        "Sin asignar": "⚠️"
    }.get(user["current_rol"], "❓")
    
    msg_text = (
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎭 <b>CONFIGURAR ROLES</b>\n"
        f"Progreso: {index + 1}/{total}\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 <b>{user['full_name']}</b>\n"
        f"{user_ref}\n\n"
        f"Rol actual: {current_emoji} <b>{user['current_rol']}</b>\n\n"
        f"Seleccioná nuevo rol:"
    )
    
    if update.message:
        await update.message.reply_text(
            msg_text, 
            parse_mode=ParseMode.HTML, 
            reply_markup=reply_markup
        )
    elif update.callback_query:
        await update.callback_query.message.edit_text(
            msg_text, 
            parse_mode=ParseMode.HTML, 
            reply_markup=reply_markup
        )


async def show_role_config_summary(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE, 
    user_id: int
) -> None:
    """Muestra resumen final y pide confirmación."""
    session = setall_rol_sessions.get(user_id)
    if not session:
        return
    
    changes = session["changes"]
    
    if not changes:
        msg_text = (
            "━━━━━━━━━━━━━━━━━━\n"
            "✅ <b>CONFIGURACIÓN COMPLETA</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "No se realizaron cambios."
        )
        
        if update.callback_query:
            await update.callback_query.message.edit_text(msg_text, parse_mode=ParseMode.HTML)
        
        del setall_rol_sessions[user_id]
        return
    
    msg_text = (
        "━━━━━━━━━━━━━━━━━━\n"
        "✅ <b>CONFIGURACIÓN COMPLETA</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"Cambios realizados ({len(changes)}):\n\n"
    )
    
    for change in changes:
        emoji = {
            "vendedor": "🛒",
            "supervisor": "👁️",
            "observador": "📋"
        }.get(change["new_rol"], "❓")
        
        msg_text += f"{emoji} <b>{change['full_name']}</b> → {change['new_rol'].capitalize()}\n"
    
    msg_text += "\n<b>¿Guardar cambios?</b>"
    
    keyboard = [
        [
            InlineKeyboardButton("💾 Guardar", callback_data="ROL_SAVE"),
            InlineKeyboardButton("❌ Cancelar", callback_data="ROL_CANCEL")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.message.edit_text(
            msg_text, 
            parse_mode=ParseMode.HTML, 
            reply_markup=reply_markup
        )


async def handle_role_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Maneja callbacks de botones del comando /setall_rol.
    Debe registrarse con: application.add_handler(CallbackQueryHandler(handle_role_callback, pattern="^ROL_"))
    """
    if not update.callback_query:
        return
    
    q = update.callback_query
    await q.answer()
    
    data = q.data
    user_id = q.from_user.id
    
    # Verificar sesión activa
    session = setall_rol_sessions.get(user_id)
    if not session:
        await q.answer("⚠️ Sesión expirada. Ejecutá /setall_rol de nuevo.", show_alert=True)
        return
    
    if data.startswith("ROL_"):
        parts = data.split("_")
        
        if len(parts) < 2:
            return
        
        action = parts[1]
        
        # Acción: Guardar cambios
        if action == "SAVE":
            await save_role_changes(update, context, user_id)
            return
        
        # Acción: Cancelar
        if action == "CANCEL":
            del setall_rol_sessions[user_id]
            await q.message.edit_text(
                "❌ Configuración cancelada. No se guardaron cambios.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Acción: Rol seleccionado para un usuario
        if len(parts) < 3:
            return
        
        rol_seleccionado = action
        target_user_id = int(parts[2])
        
        # Buscar usuario en sesión
        current_index = session["current_index"]
        users = session["users"]
        
        if current_index >= len(users):
            return
        
        user = users[current_index]
        
        if user["user_id"] != target_user_id:
            await q.answer("⚠️ Error de sincronización", show_alert=True)
            return
        
        # Registrar cambio si no es "mantener" y si es diferente al actual
        if rol_seleccionado != "mantener" and rol_seleccionado != user["current_rol"]:
            session["changes"].append({
                "user_id": user["user_id"],
                "username": user["username"],
                "full_name": user["full_name"],
                "new_rol": rol_seleccionado
            })
        
        # Avanzar al siguiente usuario
        session["current_index"] += 1
        await send_role_config_for_user(update, context, user_id, session["current_index"])


async def save_role_changes(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE, 
    user_id: int
) -> None:
    """Guarda los cambios de roles en GROUP_ROLES."""
    session = setall_rol_sessions.get(user_id)
    if not session:
        return
    
    chat_id = session["chat_id"]
    changes = session["changes"]
    
    if not changes:
        del setall_rol_sessions[user_id]
        return
    
    supervisor_name = update.callback_query.from_user.first_name or "Superusuario"
    
    success_count = 0
    for change in changes:
        try:
            result = sheets.set_user_role_in_group(
                chat_id=chat_id,
                user_id=change["user_id"],
                username=change["username"],
                full_name=change["full_name"],
                rol=change["new_rol"],
                asignado_por=supervisor_name
            )
            
            if result:
                success_count += 1
        except Exception as e:
            logger.error(f"Error guardando rol: {e}")
    
    # Invalidar cache de roles (forzar recarga)
    invalidate_role_cache()
    
    # Mensaje final
    msg_text = (
        "━━━━━━━━━━━━━━━━━━\n"
        "✅ <b>ROLES ACTUALIZADOS</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"Se guardaron <b>{success_count}/{len(changes)}</b> cambios\n\n"
        "💡 Los cambios son efectivos inmediatamente.\n\n"
        "🔄 Cache actualizado."
    )
    
    await update.callback_query.message.edit_text(msg_text, parse_mode=ParseMode.HTML)
    
    del setall_rol_sessions[user_id]
    
    logger.info(f"✅ Roles actualizados: {success_count} cambios en grupo {chat_id}")


# ============================================================================
# REGISTRAR HANDLERS
# ============================================================================
# 
# En la función main(), agregar:
# 
# # Comando /setall_rol
# application.add_handler(CommandHandler("setall_rol", cmd_setall_rol))
# 
# # Callbacks de /setall_rol
# application.add_handler(CallbackQueryHandler(handle_role_callback, pattern="^ROL_"))
#

# ============================================================================
# COMANDOS DE TEST - HIBERNACIÓN (ELIMINAR EN PRODUCCIÓN)
# ============================================================================

async def cmd_test_hibernar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """TEST: Activa hibernación manualmente."""
    if not update.message:
        return

    user_id = update.message.from_user.id

    # Solo superusuario
    if str(user_id) != BOT_OWNER_ID:
        await update.message.reply_text("❌ Solo el superusuario puede usar comandos de test")
        return

    global bot_hibernating

    if bot_hibernating:
        await update.message.reply_text(
            "⚠️ <b>Ya estás hibernando</b>\n\n"
            f"Hora actual: {datetime.now(AR_TZ).strftime('%H:%M:%S')}",
            parse_mode=ParseMode.HTML
        )
        return

    logger.info("🧪 TEST: Activando hibernación manualmente")
    await handle_hibernation_start(context)

    await update.message.reply_text(
        "🌙 <b>HIBERNACIÓN ACTIVADA (TEST)</b>\n\n"
        f"🕐 Hora Argentina: {datetime.now(AR_TZ).strftime('%d/%m/%Y %H:%M:%S')}\n"
        f"📸 Snapshot tomado: {hibernation_snapshot.get('timestamp', '-')}\n\n"
        f"<b>Para desactivar:</b> /test_despertar",
        parse_mode=ParseMode.HTML
    )


async def cmd_test_despertar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """TEST: Desactiva hibernación manualmente."""
    if not update.message:
        return

    user_id = update.message.from_user.id

    # Solo superusuario
    if str(user_id) != BOT_OWNER_ID:
        await update.message.reply_text("❌ Solo el superusuario puede usar comandos de test")
        return

    global bot_hibernating

    if not bot_hibernating:
        await update.message.reply_text(
            "⚠️ <b>No estás hibernando</b>\n\n"
            f"Hora actual: {datetime.now(AR_TZ).strftime('%H:%M:%S')}",
            parse_mode=ParseMode.HTML
        )
        return

    logger.info("🧪 TEST: Desactivando hibernación manualmente")
    await handle_hibernation_end(context)

    await update.message.reply_text(
        "☀️ <b>HIBERNACIÓN DESACTIVADA (TEST)</b>\n\n"
        f"🕐 Hora Argentina: {datetime.now(AR_TZ).strftime('%d/%m/%Y %H:%M:%S')}\n\n"
        f"✅ Bot operativo - Todos los sistemas activos",
        parse_mode=ParseMode.HTML
    )


async def cmd_test_horarios(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """TEST: Muestra diagnóstico de horarios y timezone."""
    if not update.message:
        return

    user_id = update.message.from_user.id

    # Solo superusuario
    if str(user_id) != BOT_OWNER_ID:
        await update.message.reply_text("❌ Solo el superusuario puede usar comandos de test")
        return

    now_system = datetime.now()
    now_ar = datetime.now(AR_TZ)

    # Calcular diferencia
    diff_hours = (now_ar.hour - now_system.hour) % 24

    # Estado de hibernación
    estado_hibernacion = "🌙 SÍ (ACTIVA)" if bot_hibernating else "☀️ NO (OPERATIVO)"

    # Listar jobs de hibernación
    jobs_info = ""
    try:
        jobs = context.application.job_queue.jobs()
        for job in jobs:
            if "hibernation" in job.name:
                # Obtener próxima ejecución
                next_run = job.next_t
                if next_run:
                    next_run_str = next_run.strftime('%d/%m/%Y %H:%M:%S')
                else:
                    next_run_str = "No programado"

                jobs_info += f"• <b>{job.name}</b>\n  Próximo: {next_run_str}\n\n"
    except Exception as e:
        jobs_info = f"Error listando jobs: {e}\n"

    msg = (
        f"🕐 <b>DIAGNÓSTICO DE HORARIOS</b>\n\n"
        f"<b>Hora del Sistema:</b>\n"
        f"{now_system.strftime('%d/%m/%Y %H:%M:%S %Z')}\n\n"
        f"<b>Hora Argentina (AR_TZ):</b>\n"
        f"{now_ar.strftime('%d/%m/%Y %H:%M:%S %Z')}\n\n"
        f"<b>Diferencia:</b> {diff_hours} hora(s)\n\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>Estado del Bot:</b>\n"
        f"• Hibernando: {estado_hibernacion}\n"
        f"• Hora inicio: 22:00 ARG\n"
        f"• Hora fin: 06:00 ARG\n\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>Jobs Programados:</b>\n\n"
        f"{jobs_info}"
        f"<b>Snapshot:</b>\n"
        f"• Timestamp: {hibernation_snapshot.get('timestamp', 'Sin snapshot')}\n"
        f"• Vendedores: {len(hibernation_snapshot.get('ranking', []))}\n\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>Comandos de Test:</b>\n"
        f"• /test_hibernar - Activar hibernación\n"
        f"• /test_despertar - Desactivar hibernación\n"
        f"• /test_horarios - Este diagnóstico"
    )

    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.photo:
        return
    
    if host_lock and not host_lock.is_host:
        logger.debug("No soy host, ignorando foto")
        return
    
    chat_id = update.message.chat.id
    user_id = update.message.from_user.id
    username = update.message.from_user.username or ""
    full_name = update.message.from_user.first_name or "Usuario"

    register_user_interaction(chat_id, user_id, username, full_name)

    if bot_hibernating:
        logger.debug(f"Foto ignorada durante hibernación de {username}")
        return

    rol = get_cached_role(chat_id, user_id)
    
    # LÓGICA PERMISIVA: Auto-asignar "vendedor" si no tiene rol
    if rol not in ["vendedor", "supervisor", "admin"]:
        logger.info(f"🆕 Usuario nuevo detectado enviando foto: {full_name} (rol actual: {rol}). Auto-asignando 'vendedor'...")
        
        try:
            # Guardar en Sheets
            success = sheets.set_user_role_in_group(
                chat_id=chat_id,
                user_id=user_id,
                username=username,  # ✅ Agregar parámetro faltante
                full_name=full_name,  # ✅ Agregar parámetro faltante
                rol="vendedor",
                asignado_por="Auto-Permissive"
            )
            
            if success:
                # Actualizar cache local INMEDIATAMENTE
                role_cache[(chat_id, user_id)] = "vendedor"  # ✅ Usar int, no str
                rol = "vendedor"  # Actualizar variable local
                
                # Feedback positivo al usuario
                try:
                    await update.message.reply_text(
                        f"👋 <b>Bienvenido {full_name}</b>\n"
                        f"Te he registrado automáticamente como <b>VENDEDOR</b>.\n"
                        f"Procesando tu exhibición...",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
        except Exception as e:
            logger.error(f"⚠️ Error en auto-asignación de rol: {e}")
            # Si falla sheets, seguimos procesando igual (resilience)
            rol = "vendedor"
    
    # Ahora verificar permisos (post auto-asignación)
    if rol == "observador":
        logger.debug(f"📸 Foto de {username} IGNORADA - rol observador no puede enviar exhibiciones")
        return
    
    message_id = update.message.message_id
    
    file_id = update.message.photo[-1].file_id
    
    # Verificar semáforo
    estado_sem = sheets.get_semaforo_estado()
    if estado_sem["estado"] == "DISTRIBUYENDO":
        sheets.encolar_imagen_pendiente(
            chat_id=chat_id,
            message_id=message_id,
            user_id=user_id,
            file_id=file_id,
            username=username
        )
        logger.info(f"📸 Foto encolada (semáforo ocupado) de {username}")
        try:
            await update.message.reply_text(
                "⏳ Sistema ocupado distribuyendo. Tu foto se procesará en breve.",
                reply_to_message_id=message_id
            )
        except:
            pass
        return
    
    # Anti-fraude (DESACTIVADO)
    if False and antifraud.is_duplicate_photo(user_id, file_id):
        logger.warning(f"⚠️ Foto duplicada detectada de {user_id}")
        try:
            await update.message.reply_text(
                "⚠️ Esta foto ya fue enviada recientemente.",
                reply_to_message_id=message_id
            )
        except:
            pass
        return
    
    # ============================================================================
    # LÓGICA DE RÁFAGA: Permitir múltiples fotos en 8 segundos (max 5)
    # ============================================================================
    now = time.time()
    session_exists = user_id in upload_sessions
    
    # Si ya existe sesión, verificar si es ráfaga válida
    if session_exists:
        session = upload_sessions[user_id]
        last_photo_time = session.get("last_photo_time", 0)
        photo_count = len(session.get("photos", []))
        time_since_last = now - last_photo_time
        
        # RÁFAGA VÁLIDA: < 8 seg desde última foto Y < 5 fotos Y stage = WAITING_ID
        is_valid_burst = (
            time_since_last < 8 and 
            photo_count < 5 and 
            session.get("stage") == STAGE_WAITING_ID
        )
        
        if is_valid_burst:
            # Agregar foto a sesión existente SILENCIOSAMENTE
            session["photos"].append({
                "file_id": file_id,
                "message_id": message_id
            })
            session["last_photo_time"] = now
            logger.info(f"📸 Foto adicional agregada a ráfaga: {username} ({photo_count + 1} fotos)")
            return  # No pedir número, no enviar mensaje
        
        # Si no es ráfaga válida, resetear sesión (se creará nueva abajo)
        logger.info(f"🔄 Fin de ráfaga o límite alcanzado: {username} (fotos={photo_count}, tiempo={time_since_last:.1f}s)")
    
    # Detectar sesiones colgadas o viejas para resetear
    is_stuck = session_exists and upload_sessions[user_id].get("stage") == STAGE_WAITING_TYPE
    is_old = session_exists and (now - upload_sessions[user_id].get("last_photo_time", 0) > 8)
    
    if not session_exists or is_stuck or is_old:
        # Crear nueva sesión O resetear si está colgada en WAITING_TYPE
        if is_stuck:
            logger.warning(f"🔄 Sesión colgada detectada para {username} (user_id={user_id}). Reseteando...")
            
            # Obtener message_id de la FOTO ANTERIOR para responderle
            old_photos = upload_sessions[user_id].get("photos", [])
            if old_photos:
                old_message_id = old_photos[0].get("message_id")
                try:
                    # Responder a la foto ANTERIOR (no a la nueva)
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="⚠️ <b>Tu carga anterior quedó incompleta.</b>\n\n"
                             "Por favor, <b>volvé a enviar la imagen</b> para procesarla.",
                        parse_mode=ParseMode.HTML,
                        reply_to_message_id=old_message_id
                    )
                except Exception as e:
                    logger.error(f"Error respondiendo a foto anterior: {e}")
        
        upload_sessions[user_id] = {
            "chat_id": chat_id,
            "chat_title": (getattr(update.message.chat, "title", None) or getattr(update.message.chat, "full_name", None) or getattr(update.message.chat, "username", None) or str(chat_id)),
            "vendor_id": user_id,
            "stage": STAGE_WAITING_ID,
            "photos": [],
            "nro_cliente": None,
            "tipo_pdv": None,
            "created_at": time.time(),
            "last_photo_time": time.time(),  # ← Timestamp para ráfaga
        }
    
    upload_sessions[user_id]["photos"].append({
        "file_id": file_id,
        "message_id": message_id
    })
    
    # Actualizar timestamp de última foto
    upload_sessions[user_id]["last_photo_time"] = now
    
    # Pequeño delay para asegurar que la sesión esté completamente creada
    # antes de que el usuario pueda responder (evita race conditions)
    await asyncio.sleep(0.5)
    
    # Pedir NRO_CLIENTE (mencionar cantidad de fotos si hay múltiples)
    photo_count = len(upload_sessions[user_id]["photos"])
    if photo_count > 1:
        mensaje = f"📸 <b>{photo_count} fotos recibidas.</b> Por favor, envía el <b>NRO CLIENTE</b> (solo números):"
    else:
        mensaje = f"📸 Foto recibida. Por favor, envía el <b>NRO CLIENTE</b> (solo números):"
    
    try:
        await update.message.reply_text(
            mensaje,
            parse_mode=ParseMode.HTML,
            reply_to_message_id=message_id
        )
    except Exception as e:
        logger.error(f"Error enviando respuesta: {e}")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler de texto - registra usuarios conocidos y respeta hibernación."""
    if not update.message or not update.message.text:
        return

    chat_id = update.message.chat.id
    user_id = update.message.from_user.id
    username = update.message.from_user.username or ""
    full_name = update.message.from_user.first_name or "Usuario"

    register_user_interaction(chat_id, user_id, username, full_name)

    if bot_hibernating:
        return

    if host_lock and not host_lock.is_host:
        return

    text = update.message.text.strip()
    message_id = update.message.message_id

    session = upload_sessions.get(user_id)
    if not session:
        logger.debug(f"📝 Texto ignorado de {username}: no hay sesión activa")
        return

    session.setdefault("stage", STAGE_WAITING_ID)
    
    logger.info(f"📝 Procesando texto de {username}: '{text[:50]}' | Stage: {session.get('stage')}")

    # Esperando NRO_CLIENTE
    if session.get("stage") == STAGE_WAITING_ID:
        # ✅ VALIDACIÓN DE NÚMERO DE CLIENTE
        clean_text = text.lower().replace("cliente", "").replace("#", "").replace("nro", "").strip()

        if not clean_text.isnumeric():
            await update.message.reply_text(
                "⚠️ Por favor, envía <b>SOLO NÚMEROS</b> para el Nro de Cliente.",
                parse_mode=ParseMode.HTML,
                reply_to_message_id=message_id
            )
            return 

        nro_cliente = clean_text
        session["nro_cliente"] = nro_cliente
        session["stage"] = STAGE_WAITING_TYPE

        logger.info(f"✅ NRO_CLIENTE guardado: {nro_cliente} | Obteniendo tipos de PDV...")

        # ✅ BOTONES DINÁMICOS
        try:
            tipos_disponibles = await get_pos_types_cached()
            if not tipos_disponibles:
                logger.error("❌ No se obtuvieron tipos de PDV desde Sheets")
                await update.message.reply_text(
                    "❌ Error al cargar tipos de PDV.\n"
                    "Por favor, reintentá enviando tu foto nuevamente.",
                    parse_mode=ParseMode.HTML,
                    reply_to_message_id=message_id
                )
                # Limpiar sesión corrupta
                del upload_sessions[user_id]
                return
            
            logger.info(f"📋 Tipos de PDV obtenidos: {len(tipos_disponibles)}")
        except Exception as e:
            logger.error(f"❌ Exception obteniendo tipos de PDV: {e}", exc_info=True)
            await update.message.reply_text(
                "❌ Error al cargar tipos de PDV.\n"
                "Por favor, reintentá enviando tu foto nuevamente.",
                parse_mode=ParseMode.HTML,
                reply_to_message_id=message_id
            )
            # Limpiar sesión corrupta
            del upload_sessions[user_id]
            return

        botones_lista = []
        for tipo in tipos_disponibles:
            clean_code = "".join(c for c in tipo if c.isalnum()).upper()
            botones_lista.append(
                InlineKeyboardButton(tipo, callback_data=f"TYPE_{clean_code}_{user_id}")
            )

        keyboard = [botones_lista[i:i + 2] for i in range(0, len(botones_lista), 2)]

        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
            await update.message.reply_text(
                f"✅ NRO CLIENTE: <code>{nro_cliente}</code>\n\n"
                f"Ahora seleccioná el <b>tipo de PDV</b>:",
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
            logger.info(f"✅ Botones de tipo PDV enviados correctamente a {username}")
        except Exception as e:
            logger.error(f"❌ Error enviando botones a {username}: {e}", exc_info=True)
            await update.message.reply_text(
                "❌ Error al mostrar opciones de tipo de PDV.\n"
                "Por favor, reintentá enviando tu foto nuevamente.",
                parse_mode=ParseMode.HTML,
                reply_to_message_id=message_id
            )
            # Limpiar sesión corrupta
            del upload_sessions[user_id]


    # ==========================================
    # CALLBACK BUTTONS
    # ==========================================

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.callback_query: 
        return
    
    if host_lock and not host_lock.is_host:
        await update.callback_query.answer("⚠️ Este bot no es el host activo", show_alert=True)
        return
    
    q = update.callback_query
    await q.answer()
    
    data = q.data
    uid = q.from_user.id
    
    # Botón de tipo PDV
    if data.startswith("TYPE_"):
        parts = data.split("_")
        if len(parts) < 3:
            await q.answer("⚠️ Datos inválidos.", show_alert=True)
            return
        
        # ✅ OBTENER NOMBRE "BONITO" DEL TIPO
        clean_code = parts[1]
        tipo_pdv_display = clean_code # Fallback
        
        # Buscar el nombre original en la lista de Sheets
        all_types = await get_pos_types_cached()
        for t in all_types:
            code_from_sheet = "".join(c for c in t if c.isalnum()).upper()
            if code_from_sheet == clean_code:
                tipo_pdv_display = t # Ej: "Comercio con Ingreso"
                break
        
        uploader_id = int(parts[2])
        
        if uid != uploader_id:
            await q.answer("❌ Esta no es tu sesión.", show_alert=True)
            return
        
        session = upload_sessions.get(uploader_id)
        if not isinstance(session, dict) or session.get("stage") != STAGE_WAITING_TYPE:
            await q.answer("⚠️ Sesión expirada.", show_alert=True)
            return
        
        # Guardamos el nombre bonito tanto para display como para la DB (mejora calidad de datos)
        session["tipo_pdv"] = tipo_pdv_display
        nro_cliente = session["nro_cliente"]
        photos = session["photos"]
        chat_id = session["chat_id"]
        uploader_name = q.from_user.first_name or "Usuario"
        
        group_title = session.get("chat_title") or str(chat_id)
        # ✅ SUBIDA INMEDIATA A DRIVE Y REGISTRO "PENDIENTE"
        procesadas_count = 0
        referencias_subidas = []
        
        for photo_data in photos:
            file_id = photo_data["file_id"]
            message_id = photo_data["message_id"]
            
            try:
                # 1. Descargar
                file = await context.bot.get_file(file_id)
                file_bytes = await file.download_as_bytearray()
                
                # 2. Subir a Drive
                result = sheets.upload_image_to_drive(
                    file_bytes=bytes(file_bytes),
                    filename=f"{nro_cliente}_{clean_code}_{int(time.time())}.jpg",
                    user_id=uploader_id,
                    username=uploader_name
                    , group_title=group_title
                )
                
                if result and result.drive_link:
                    # 3. Registrar en Sheets (Genera el estado "Pendiente")
                    uuid_ref = sheets.log_raw(
                        user_id=uploader_id,
                        username=uploader_name,
                        nro_cliente=nro_cliente,
                        tipo_pdv=tipo_pdv_display, # Guardamos el nombre bonito
                        drive_link=result.drive_link
                        , group_title=group_title
                        , chat_id=chat_id
                    )
                    
                    if uuid_ref:
                        referencias_subidas.append({
                            "uuid": uuid_ref,
                            "message_id": message_id,
                            "drive_link": result.drive_link
                        })
                        procesadas_count += 1
            except Exception as e:
                logger.error(f"Error procesando subida inmediata: {e}")

        # ✅ LIMPIEZA Y MENSAJE DE EVALUACIÓN
        if procesadas_count > 0:
            try:
                # 1. Borrar los botones de selección
                await q.edit_message_reply_markup(reply_markup=None)
                
                # 2. (ELIMINADO) Ya no enviamos el mensaje de "Exhibición guardada"
                # await context.bot.send_message(...) 
                
                # 3. Enviar UN SOLO mensaje de EVALUACIÓN (para todas las fotos)
                # Usamos la primera foto como referencia visual
                primera_ref = referencias_subidas[0]
                
                # Texto con cantidad de fotos si hay múltiples
                if procesadas_count > 1:
                    fotos_text = f"📸 <b>{procesadas_count} fotos subidas</b>\n\n"
                else:
                    fotos_text = ""
                
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Aprobar", callback_data=f"APR_{primera_ref['uuid']}_{uploader_id}"),
                        InlineKeyboardButton("❌ Rechazar", callback_data=f"REC_{primera_ref['uuid']}_{uploader_id}")
                    ],
                    [
                        InlineKeyboardButton("🔥 Destacado", callback_data=f"DES_{primera_ref['uuid']}_{uploader_id}")
                    ]
                ]
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                # Mensaje con info de la exhibición
                msg_text = (
                    f"📋 <b>Nueva exhibición</b>\n\n"
                    f"{fotos_text}"
                    f"👤 <b>Vendedor:</b> {uploader_name}\n"
                    f"🏪 <b>Cliente:</b> {nro_cliente}\n"
                    f"📍 <b>Tipo:</b> {tipo_pdv_display}\n"
                    f"🔗 <a href='{primera_ref['drive_link']}'>Ver en Drive</a>\n\n"
                    f"<b>Evaluar:</b>"
                )
                
                # Enviar mensaje respondiendo a la PRIMERA foto
                sent_msg = await context.bot.send_message(
                    chat_id=chat_id,
                    text=msg_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                    reply_to_message_id=primera_ref["message_id"]
                )
                
                # Actualizar telegram_refs para TODAS las fotos (para tracking)
                for ref_data in referencias_subidas:
                    sheets.update_telegram_refs(
                        uuid_ref=ref_data["uuid"], 
                        chat_id=int(chat_id), 
                        msg_id=int(sent_msg.message_id)
                    )
                
                # Guardar transacción activa (solo una vez)
                active_transactions[sent_msg.message_id] = {
                    "uuid": primera_ref["uuid"],
                    "uploader_id": uploader_id,
                    "ref_msg": primera_ref["message_id"],
                    "total_fotos": procesadas_count  # ← Info adicional
                }

            except Exception as e:
                logger.error(f"Error en post-procesamiento: {e}")
        else:
            try:
                await q.edit_message_text("❌ Error al subir las fotos a Drive.")
            except:
                pass

        # Limpiar sesión
        del upload_sessions[uploader_id]
        return
    
    # Botones de aprobación/rechazo
    if data.startswith("CANCEL_"):
        parts = data.split("_")
        if len(parts) < 2: await q.answer("⚠️ Datos inválidos.", show_alert=True); return
        ref_msg_id = int(parts[1])
        t = active_transactions.get(ref_msg_id)
        if not t: await q.answer("⚠️ Transacción no encontrada.", show_alert=True); return
        if str(uid) != str(t["uploader_id"]) and str(uid) != BOT_OWNER_ID: await q.answer("❌ Solo el uploader o SU pueden cancelar.", show_alert=True); return
        try: await q.edit_message_text(f"{q.message.text_html.split('<b>Evaluar:</b>')[0]}<b>❌ CANCELADO</b>", parse_mode=ParseMode.HTML); pass
        except: pass
        active_transactions.pop(ref_msg_id, None)

    elif data.startswith(("APR_", "REC_", "DES_")):
        parts = data.split("_")
        if len(parts) < 3:
            await q.answer("⚠️ Datos inválidos.", show_alert=True)
            return

        action = parts[0]
        uuid_ref = parts[1]
        uploader_id = parts[2]
        
        if str(uid) == str(uploader_id) and (not BOT_OWNER_ID or str(uid) != BOT_OWNER_ID):
            await q.answer("❌ No puedes auto-aprobarte.", show_alert=True)
            return

        if action == "APR":
            status = "Aprobado"
            icon = "✅"
        elif action == "DES":
            status = "Destacado"
            icon = "🔥"
        else:  # REC
            status = "Rechazado"
            icon = "❌"

        # ✅ SOLO ACTUALIZAMOS EL ESTADO (La foto ya está en Drive)
        result = sheets.update_status_by_uuid(
            uuid_ref=uuid_ref,
            new_status=status,
            supervisor_name=q.from_user.first_name,
            comments="" 
        )
        
        if result == "LOCKED":
            await q.answer("⚠️ Ya evaluado externamente.", show_alert=True)
            txt = q.message.text_html.split("\n\n")[0] if q.message and q.message.text_html else ""
            await q.edit_message_text(f"{txt}\n\n⚠️ <b>Ya gestionado</b>", parse_mode=ParseMode.HTML, reply_markup=None)
            return
            
        if result == "ERROR":
            await q.answer("❌ Error de conexión.", show_alert=True)
            return

        # Stats de sesión
        if status == "Aprobado":
            session_stats['aprobadas'] += 1
        elif status == "Destacado":
            session_stats['aprobadas'] += 1
        else:
            session_stats['rechazadas'] += 1

        # Mensaje Final
        txt = q.message.text_html.split("\n\n")[0] if q.message and q.message.text_html else ""
        
        if status == "Destacado":
             mensaje_final = (
                f"{txt}\n\n"
                f"🔥 <b>¡EXHIBICIÓN DESTACADA!</b> 🔥\n"
                f"✨ Evaluada por <b>{q.from_user.first_name}</b>\n"
                f"🚀 <b>Ejecución Perfecta</b> • ¡Sumaste 2 puntos extra!"
            )
        else:
             mensaje_final = f"{txt}\n\n{icon} <b>{status}</b> por {q.from_user.first_name}"
        
        await q.edit_message_text(
            mensaje_final,
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )


# ==========================================
# ERROR HANDLER
# ==========================================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja errores globales del bot."""
    error = context.error
    logger.error(f"Error capturado por error_handler: {error}")
    
    if isinstance(error, BadRequest):
        logger.warning(f"BadRequest ignorado: {error}")
        return
    
    import traceback
    logger.error("Traceback completo:")
    logger.error("".join(traceback.format_exception(None, error, error.__traceback__)))


# ==========================================
# JOBS PERIÓDICOS
# ==========================================

async def sync_telegram_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sincroniza datos con Sheets cada 30s."""
    if bot_hibernating:
        return
    if host_lock and not host_lock.is_host:
        return

    try:
        # 1. Buscar acciones pendientes de sincronizar
        actions = sheets.get_unsynced_actions()
        if not actions:
            return

        logger.info(f"🔄 Sincronizando {len(actions)} evaluaciones desde el Visor...")

        rows_to_mark = []

        for action in actions:
            chat_id = action.get("chat_id")
            msg_id = action.get("msg_id")
            estado = action.get("estado")
            comentario = action.get("comentarios") or ""

            # Formateo de iconos
            icon = "✅" if estado == "Aprobado" else "❌" if estado == "Rechazado" else "🔥"

            # Reconstrucción del mensaje
            vendedor = action.get("vendedor", "Vendedor")
            cliente = action.get("cliente", "Cliente")
            tipo = action.get("tipo", "PDV")

            if estado == "Destacado":
                final_status_text = (
                    f"🔥 <b>¡EXHIBICIÓN DESTACADA!</b> 🔥\n"
                    f"🚀 <b>Ejecución Perfecta</b> • ¡Sumaste 2 puntos extra!"
                )
            else:
                final_status_text = f"{icon} <b>{estado}</b>"

            # Agregar comentario si existe
            if comentario:
                # Limpiar el prefijo "Evaluado por..." si ya viene del visor
                clean_comment = comentario.split("|")[-1].replace("Nota:", "").strip()
                if clean_comment and clean_comment != "Evaluado por":
                    final_status_text += f"\n\n📝 <b>Nota:</b> <i>{clean_comment}</i>"

            msg_base = (
                f"📋 <b>Exhibición Evaluada</b>\n\n"
                f"👤 <b>Vendedor:</b> {vendedor}\n"
                f"🏪 <b>Cliente:</b> {cliente}\n"
                f"📍 <b>Tipo:</b> {tipo}\n\n"
                f"{final_status_text}"
            )

            try:
                # Intentamos editar el mensaje original de los botones
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=msg_base,
                    parse_mode=ParseMode.HTML,
                    reply_markup=None
                )
                rows_to_mark.extend(action["row_nums"])
                logger.info(f"✅ Mensaje {msg_id} editado exitosamente.")

            except BadRequest as e:
                # Si el mensaje es muy viejo (>48h) o no se encuentra, marcamos como sincronizado
                # para que no lo intente procesar en cada ciclo.
                logger.warning(f"⚠️ No se pudo editar mensaje {msg_id}: {e}")
                rows_to_mark.extend(action["row_nums"])
            except Exception as e:
                logger.error(f"❌ Error inesperado en Telegram: {e}")

        # 3. Marcar en Sheets como sincronizado
        if rows_to_mark:
            sheets.mark_as_synced_rows(rows_to_mark)

    except Exception as e:
        logger.error(f"Error general en sync_telegram_job: {e}")

async def procesar_cola_imagenes_pendientes(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Procesa cola de imágenes cada 30s."""
    if bot_hibernating:
        return
    if host_lock and not host_lock.is_host: return
    try:
        pendientes = sheets.get_imagenes_pendientes()
        if not pendientes: return
        logger.info(f"📋 Procesando {len(pendientes)} imágenes pendientes...")
        for img in pendientes[:5]:
            try: sheets.marcar_imagen_procesada(img["row_num"])
            except Exception as e: logger.error(f"Error procesando imagen pendiente: {e}")
        sheets.limpiar_cola_imagenes()
    except Exception as e: logger.error(f"Error en procesar_cola_imagenes_pendientes: {e}")


async def cleanup_expired_sessions(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Limpia sesiones de upload que llevan más de 10 minutos sin completarse.
    Previene memory leaks de sesiones zombies.
    """
    if bot_hibernating:
        return
    if host_lock and not host_lock.is_host:
        return
    
    try:
        now = time.time()
        to_delete = []
        
        for user_id, session in upload_sessions.items():
            created_at = session.get("created_at", now)
            age_seconds = now - created_at
            
            # Eliminar sesiones > 10 minutos (600 segundos)
            if age_seconds > 600:
                to_delete.append(user_id)
                logger.info(f"🧹 Limpiando sesión expirada: user_id={user_id}, edad={int(age_seconds/60)} min")
        
        for user_id in to_delete:
            del upload_sessions[user_id]
        
        if to_delete:
            logger.info(f"🧹 Total de sesiones expiradas limpiadas: {len(to_delete)}")
            
    except Exception as e:
        logger.error(f"Error en cleanup_expired_sessions: {e}")

async def update_host_heartbeat(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Actualiza heartbeat cada 60s y maneja takeovers.
    """
    global bot_in_monitoring_mode
    if not host_lock: return
    try:
        info = host_lock.get_host_info()
        current_host = info.get("current_host")
        is_me_host = host_lock._is_same_machine(current_host) if current_host else False
        
        if host_lock.is_host and not is_me_host:
            logger.warning("⚠️ ¡Host perdido! Cambiando a modo monitoreo...")
            host_lock.is_host = False
            bot_in_monitoring_mode = True
            await notify_superuser(context, f"⚠️ <b>HOST PERDIDO</b>\n\n🔄 Bot pasó a modo monitoreo\nIs🖥️ {host_lock.identity}\nIs👑 Nuevo host: {current_host or 'Ninguno'}")
        
        if host_lock.is_host:
            host_lock.check_and_takeover_if_dead()
            if not host_lock.is_host:
                logger.info("✅ Traspaso completado, pasando a modo monitoreo")
                bot_in_monitoring_mode = True
        elif bot_in_monitoring_mode:
            ws = host_lock._get_or_create_host_control_sheet()
            if ws:
                data = ws.get_all_values()
                for i in range(2, len(data)):
                    if len(data[i]) > 0 and host_lock._is_same_machine(data[i][0]):
                        ws.update(f"G{i+1}", [[host_lock._get_timestamp()]])
                        logger.debug(f"Heartbeat actualizado en cola (fila {i+1})")
                        break
            if host_lock.check_and_takeover_if_dead() and host_lock.is_host:
                logger.info("👑 ¡Takeover exitoso! Ahora soy host")
                bot_in_monitoring_mode = False
                await notify_superuser(context, f"👑 <b>TAKEOVER EXITOSO</b>\n\n✅ Este bot es ahora el host\nIs🖥️ {host_lock.identity}")
    except Exception as e:
        logger.error(f"Error en update_host_heartbeat: {e}")

async def send_periodic_status(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not host_lock or not host_lock.is_host: return
    msg = f"📊 <b>Status Periódico</b>\n\nIs⏱️ Uptime: {_uptime_hhmmss()}\nIs👑 Host: {host_lock.identity}\nIs✅ Funcionando correctamente"
    await notify_superuser(context, msg)

# ==========================================
# MAIN
# ==========================================


if __name__ == "__main__":
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except Exception:
        local_ip = "Desconocida"
    user = getpass.getuser()
    pid = os.getpid()

    print("\n" + "█" * 60)
    print("🕵️  IDENTIDAD DE ESTA INSTANCIA")
    print(f"    🖥️  Host:      {hostname}")
    print(f"    👤  Usuario:   {user}")
    print(f"    🔢  PID:       {pid}")
    print(f"    🌐  IP Local:  {local_ip}")
    print("█" * 60 + "\n")

    token = cfg.get_telegram_config().get("bot_token")
    if not token or token == "aa":
        print("❌ Sin Token")
        raise SystemExit(1)

    def _restart_process() -> None:
        """Reinicia el proceso actual para entrar en modo HOST sin reciclar estado en memoria."""
        exe = sys.executable
        args = [exe] + sys.argv[1:]
        logger.warning("🔁 Reiniciando proceso para iniciar polling como HOST...")
        os.execv(exe, args)

    if HostLock:
        logger.info("=" * 80)
        logger.info("🔐 INICIANDO SISTEMA DE HOST LOCK")
        logger.info("=" * 80)
        host_lock = HostLock(sheets, notify_callback=host_event_callback)
        result = host_lock.try_acquire_host()
        if not result["success"]:
            logger.error(f"❌ {result['message']}")
            print(f"\n❌ {result['message']}\n")
            raise SystemExit(1)

        if not result["is_host"]:
            logger.warning(f"⏳ {result['message']}")
            print(
                f"\n⏳ {result['message']}\n"
                f"👑 Host actual: {result['current_host']}\n"
                f"Is📋 Tu posición en cola: {result['queue_position']}\n"
                f"💡 Bot en MODO MONITOREO (sin polling de Telegram)\n"
            )
            bot_in_monitoring_mode = True
        else:
            logger.info("=" * 80)
            logger.info(f"👑 HOST LOCK ADQUIRIDO: {host_lock.identity}")
            logger.info("=" * 80)
            bot_in_monitoring_mode = False
    else:
        logger.warning("⚠️ Sistema de host lock deshabilitado")

    # -----------------------------------------------------------------
    # ✅ FIX CRÍTICO (logs 2026-01-30): evitar "Conflict: getUpdates"
    # Si esta instancia NO es host, NO debe iniciar run_polling().
    # Se queda en monitoreo y solo intenta takeover; si se vuelve host, reinicia.
    # -----------------------------------------------------------------
    if host_lock and bot_in_monitoring_mode:
        logger.warning("⏸️ MODO MONITOREO: NO se inicia polling de Telegram en esta instancia.")
        try:
            while True:
                try:
                    host_lock.check_and_takeover_if_dead()
                    if host_lock.is_host:
                        logger.info("👑 TAKEOVER: esta instancia ahora es HOST.")
                        _restart_process()
                except Exception as e:
                    logger.error(f"Error en modo monitoreo HostLock: {e}")
                time.sleep(15)
        except KeyboardInterrupt:
            logger.warning("🛑 Interrumpido por usuario (monitoring mode).")
        raise SystemExit(0)

    # A partir de acá, SOLO corre el host
    app = ApplicationBuilder().token(token).build()
    _global_app = app

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("mirol", cmd_mirol))
    app.add_handler(CommandHandler("setall_rol", cmd_setall_rol))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("hardreset", cmd_hardreset))
    app.add_handler(CommandHandler("misgrupos", cmd_misgrupos))
    app.add_handler(CommandHandler("ranking", cmd_ranking))
    app.add_handler(CommandHandler("set_role", cmd_set_role))

    # ========================================
    # 🧪 COMANDOS DE TEST - ELIMINAR EN PRODUCCIÓN
    # ========================================
    app.add_handler(CommandHandler("test_hibernar", cmd_test_hibernar))
    app.add_handler(CommandHandler("test_despertar", cmd_test_despertar))
    app.add_handler(CommandHandler("test_horarios", cmd_test_horarios))
    # ========================================

    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(CallbackQueryHandler(handle_role_callback, pattern="^ROL_"))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    app.job_queue.run_repeating(sync_telegram_job, interval=30, first=(datetime.now(AR_TZ) + timedelta(seconds=5)).replace(tzinfo=AR_TZ))
    app.job_queue.run_repeating(procesar_cola_imagenes_pendientes, interval=30, first=15)
    app.job_queue.run_repeating(cleanup_expired_sessions, interval=300, first=60)  # Cada 5 min
    if host_lock:
        app.job_queue.run_repeating(update_host_heartbeat, interval=60, first=10)
    app.job_queue.run_repeating(send_periodic_status, interval=14400, first=60)
    app.job_queue.run_repeating(refresh_pos_types_job, interval=POS_TYPES_CACHE_TTL_SECONDS, first=10)


    print("🚀 BOT ONLINE (HOST)")

    async def post_init(application):
        await _ensure_bot_ready(application.bot)
        await semaforo.start()
        logger.info("🚦 Monitor de semáforo iniciado")
        host_status = (
            f"👑 HOST: {host_lock.identity}"
            if host_lock and host_lock.is_host
            else (f"⏸️ MONITOREO: {host_lock.identity}" if bot_in_monitoring_mode else "N/A")
        )
        await notify_superuser(
            application,
            f"🚀 <b>BOT INICIADO</b>\n\n"
            f"Is🖥️ {host_status}\n"
            f"Is🕐 Hora: {datetime.now(AR_TZ).strftime('%H:%M:%S')}\n"
            f"Is✅ Todos los sistemas operativos",
        )
        await setup_bot_commands(application)
        await post_init_extensions(application)

    app.post_init = post_init

    async def on_shutdown(application):
        logger.warning("🛑 Bot cerrándose...")
        if hasattr(semaforo, "stop"):
            try:
                await semaforo.stop()
            except Exception as e:
                logger.warning(f"⚠️ No se pudo detener semáforo: {e}")
        if host_lock and host_lock.is_host:
            logger.info("🔓 Liberando host lock...")
            host_lock.release_host()
        await notify_superuser(
            application,
            f"🛑 <b>BOT DETENIDO</b>\n\n"
            f"Is🕐 Hora: {datetime.now(AR_TZ).strftime('%H:%M:%S')}\n"
            f"Is📊 Uptime: {_uptime_hhmmss()}\n"
            f"Isℹ️ Apagado normal",
        )

    app.post_shutdown = on_shutdown

    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        logger.info("[DEBUG] Event loop creado")

    app.run_polling()