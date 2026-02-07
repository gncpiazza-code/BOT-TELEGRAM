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

if "REQUESTS_CA_BUNDLE" in os.environ:
    del os.environ['REQUESTS_CA_BUNDLE']
if "CURL_CA_BUNDLE" in os.environ:
    del os.environ['CURL_CA_BUNDLE']

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
import telegram
from telegram.error import BadRequest, TimedOut, NetworkError
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackQueryHandler,
)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

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
    from config_manager import ConfigManager

try:
    from logger_config import setup_logging, get_logger, log_exception
    setup_logging(log_level=logging.INFO, detailed=True)
    logger = get_logger(__name__)
    
    print("="*80)
    print("🤖 BOT HOST INICIANDO...")
    print("="*80)
    sys.stdout.flush()
    logger.info("="*80)
    logger.info("🤖 BOT HOST INICIANDO...")
    logger.info("="*80)
except ImportError:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(name)-15s | %(funcName)-20s | L%(lineno)-4d | %(message)s",
        level=logging.INFO
    )
    logger = logging.getLogger("HostBot")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    print("⚠️ logger_config.py no encontrado, usando logging básico")


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

# Variable global para acceder al bot en callbacks
_global_app = None

async def host_event_callback(event_type: str, message: str):
    """
    Callback para notificaciones de eventos de host.
    """
    try:
        if _global_app and BOT_OWNER_ID:
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
        await context.bot.send_message(chat_id=int(BOT_OWNER_ID), text=message, parse_mode=parse_mode)
        logger.info(f"📬 Notificación enviada al superusuario")
        return True
    except Exception as e:
        logger.error(f"Error al notificar al superusuario: {e}")
        return False


# ==========================================
# COMANDOS
# ==========================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    await update.message.reply_text(
        "¡Hola! Soy el bot de auditoría de PDV.\n"
        "Usa /help para ver cómo funciono."
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)

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
    if not update.message: return
    uid = update.message.from_user.id
    
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
    if not update.message: return
    
    try:
        ranking = sheets.get_ranking_report()
        
        if not ranking:
            await update.message.reply_text("📊 No hay datos de ranking aún.")
            return
        
        msg = "🏆 <b>RANKING DEL MES</b>\n\n"
        
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
    except Exception as e:
        logger.error(f"Error en cmd_ranking: {e}")
        await update.message.reply_text("❌ Error al obtener ranking")

async def cmd_set_role(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    uid = update.message.from_user.id
    if str(uid) != BOT_OWNER_ID:
        await update.message.reply_text("❌ Solo el superusuario puede asignar roles")
        return
    
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("Uso: /set_role {supervisor|vendedor}")
        return
    
    role = context.args[0].lower()
    if role not in ["supervisor", "vendedor"]:
        await update.message.reply_text("❌ Rol inválido. Usa: supervisor o vendedor")
        return
    
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Responde al mensaje de un usuario para asignarle el rol")
        return
    
    target_user = update.message.reply_to_message.from_user
    target_id = target_user.id
    target_name = target_user.first_name
    
    await update.message.reply_text(
        f"✅ Rol <b>{role}</b> asignado a {target_name} (ID: {target_id})",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Rol {role} asignado a {target_id} por SU")


# ==========================================
# HANDLERS DE FOTOS Y TEXTO
# ==========================================

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.photo: 
        return
    
    if host_lock and not host_lock.is_host:
        logger.debug("No soy host, ignorando foto")
        return
    
    chat_id = update.message.chat.id
    user_id = update.message.from_user.id
    username = update.message.from_user.first_name or "Usuario"
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
    
    # Rate limiting
    key = (chat_id, user_id)
    now = time.time()
    last_time = active_last_prompt.get(key, 0)
    
    if now - last_time < 3:
        logger.debug(f"Rate limit: {user_id} en {chat_id}")
        return
    
    active_last_prompt[key] = now
    
    # Guardar sesión
    if user_id not in upload_sessions:
        upload_sessions[user_id] = {
            "chat_id": chat_id,
            "chat_title": (getattr(update.message.chat, "title", None) or getattr(update.message.chat, "full_name", None) or getattr(update.message.chat, "username", None) or str(chat_id)),
            "vendor_id": user_id,
            "stage": STAGE_WAITING_ID,
            "photos": [],
            "nro_cliente": None,
            "tipo_pdv": None,

        }
    
    upload_sessions[user_id]["photos"].append({
        "file_id": file_id,
        "message_id": message_id
    })
    
    # Pedir NRO_CLIENTE
    try:
        await update.message.reply_text(
            f"📸 Foto recibida. Por favor, envía el <b>NRO CLIENTE</b> (solo números):",
            parse_mode=ParseMode.HTML,
            reply_to_message_id=message_id
        )
    except Exception as e:
        logger.error(f"Error enviando respuesta: {e}")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text: 
        return
    
    if host_lock and not host_lock.is_host:
        return
    
    text = update.message.text.strip()
    chat_id = update.message.chat.id
    user_id = update.message.from_user.id
    message_id = update.message.message_id
    
    session = upload_sessions.get(user_id)
    if not session:
        return

    session.setdefault("stage", STAGE_WAITING_ID)
    
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
        
        # ✅ BOTONES DINÁMICOS
        tipos_disponibles = sheets.get_pos_types()
        
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
        except Exception as e:
            logger.error(f"Error enviando botones: {e}")


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
        all_types = sheets.get_pos_types()
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
        if not session or session["stage"] != STAGE_WAITING_TYPE:
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
                
                # 3. Enviar mensaje de EVALUACIÓN respondiendo a la FOTO ORIGINAL
                for ref_data in referencias_subidas:
                    keyboard = [
                        [
                            InlineKeyboardButton("✅ Aprobar", callback_data=f"APR_{ref_data['uuid']}_{uploader_id}"),
                            InlineKeyboardButton("❌ Rechazar", callback_data=f"REC_{ref_data['uuid']}_{uploader_id}")
                        ],
                        [
                            InlineKeyboardButton("🔥 Destacado", callback_data=f"DES_{ref_data['uuid']}_{uploader_id}")
                        ]
                    ]
                    
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    # Usamos el nombre bonito en el mensaje
                    msg_text = (
                        f"📋 <b>Nueva exhibición</b>\n\n"
                        f"👤 <b>Vendedor:</b> {uploader_name}\n"
                        f"🏪 <b>Cliente:</b> {nro_cliente}\n"
                        f"📍 <b>Tipo:</b> {tipo_pdv_display}\n"
                        f"🔗 <a href='{ref_data['drive_link']}'>Ver en Drive</a>\n\n"
                        f"<b>Evaluar:</b>"
                    )
                    
                    sent_msg = await context.bot.send_message(
                        chat_id=chat_id,
                        text=msg_text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=reply_markup,
                        reply_to_message_id=ref_data["message_id"] # ✅ RESPONDE A LA FOTO
                    )
                    
                    sheets.update_telegram_refs(uuid_ref=ref_data["uuid"], chat_id=int(chat_id), msg_id=int(sent_msg.message_id))
                    active_transactions[sent_msg.message_id] = {
                        "uuid": ref_data["uuid"],
                        "uploader_id": uploader_id,
                        "ref_msg": ref_data["message_id"]
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
        if ref_msg_id in active_transactions: del active_transactions[t['ref_msg']]

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

async def handle_hibernation_start(context: ContextTypes.DEFAULT_TYPE) -> None:
    global bot_hibernating
    if not host_lock or not host_lock.is_host: return
    bot_hibernating = True
    logger.info("🌙 Modo hibernación ACTIVADO (23:00-07:00)")
    await notify_superuser(context, "🌙 <b>Modo Hibernación</b>\n\nBot en modo reducido hasta las 07:00")

async def handle_hibernation_end(context: ContextTypes.DEFAULT_TYPE) -> None:
    global bot_hibernating
    if not host_lock or not host_lock.is_host: return
    bot_hibernating = False
    logger.info("☀️ Modo hibernación DESACTIVADO (07:00)")
    await notify_superuser(context, "☀️ <b>Modo Normal</b>\n\nBot operando normalmente")


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":
    hostname = socket.gethostname()
    try: local_ip = socket.gethostbyname(hostname)
    except: local_ip = "Desconocida"
    user = getpass.getuser()
    pid = os.getpid()
    
    print("\n" + "█" * 60)
    print(f"🕵️  IDENTIDAD DE ESTA INSTANCIA")
    print(f"    🖥️  Host:      {hostname}")
    print(f"    👤  Usuario:   {user}")
    print(f"    🔢  PID:       {pid}")
    print(f"    🌐  IP Local:  {local_ip}")
    print("█" * 60 + "\n")
    
    token = cfg.get_telegram_config().get("bot_token")
    if not token or token == "aa": print("❌ Sin Token"); raise SystemExit(1)
    
    if HostLock:
        logger.info("="*80); logger.info("🔐 INICIANDO SISTEMA DE HOST LOCK"); logger.info("="*80)
        host_lock = HostLock(sheets, notify_callback=host_event_callback)
        result = host_lock.try_acquire_host()
        if not result["success"]: logger.error(f"❌ {result['message']}"); print(f"\n❌ {result['message']}\n"); raise SystemExit(1)
        if not result["is_host"]:
            logger.warning(f"⏳ {result['message']}"); print(f"\n⏳ {result['message']}\n👑 Host actual: {result['current_host']}\nIs📋 Tu posición en cola: {result['queue_position']}\n💡 Bot en MODO MONITOREO...")
            bot_in_monitoring_mode = True
        else:
            logger.info("="*80); logger.info(f"👑 HOST LOCK ADQUIRIDO: {host_lock.identity}"); logger.info("="*80)
            bot_in_monitoring_mode = False
    else:
        logger.warning("⚠️ Sistema de host lock deshabilitado")
    
    app = ApplicationBuilder().token(token).build()
    _global_app = app
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("hardreset", cmd_hardreset))
    app.add_handler(CommandHandler("misgrupos", cmd_misgrupos))
    app.add_handler(CommandHandler("ranking", cmd_ranking))
    app.add_handler(CommandHandler("set_role", cmd_set_role))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)
    
    app.job_queue.run_repeating(sync_telegram_job, interval=30, first=5)
    app.job_queue.run_repeating(procesar_cola_imagenes_pendientes, interval=30, first=15)
    if host_lock: app.job_queue.run_repeating(update_host_heartbeat, interval=60, first=10)
    app.job_queue.run_repeating(send_periodic_status, interval=14400, first=60)
    app.job_queue.run_daily(handle_hibernation_start, time=datetime.strptime("23:00", "%H:%M").time())
    app.job_queue.run_daily(handle_hibernation_end, time=datetime.strptime("07:00", "%H:%M").time())
    
    print("🚀 BOT ONLINE")
    
    async def post_init(application):
        await semaforo.start(); logger.info("🚦 Monitor de semáforo iniciado")
        host_status = f"👑 HOST: {host_lock.identity}" if host_lock and host_lock.is_host else (f"⏸️ MONITOREO: {host_lock.identity}" if bot_in_monitoring_mode else "N/A")
        await notify_superuser(application, f"🚀 <b>BOT INICIADO</b>\n\nIs🖥️ {host_status}\nIs🕐 Hora: {datetime.now().strftime('%H:%M:%S')}\nIs✅ Todos los sistemas operativos")
    app.post_init = post_init
    
    async def on_shutdown(application):
        logger.warning("🛑 Bot cerrándose...")
        if host_lock and host_lock.is_host: logger.info("🔓 Liberando host lock..."); host_lock.release_host()
        await notify_superuser(application, f"🛑 <b>BOT DETENIDO</b>\n\nIs🕐 Hora: {datetime.now().strftime('%H:%M:%S')}\nIs📊 Uptime: {_uptime_hhmmss()}\nIsℹ️ Apagado normal")
    app.post_shutdown = on_shutdown
    
    try: asyncio.get_event_loop()
    except RuntimeError: loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop); logger.info("[DEBUG] Event loop creado")
    
    app.run_polling()