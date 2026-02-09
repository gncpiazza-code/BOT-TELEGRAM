# -*- coding: utf-8 -*-
"""
PARCHE PARA host_bot.py - PARTE 1: CACHE DE ROLES Y HIBERNACIÓN

UBICACIÓN: Agregar estas variables y funciones DESPUÉS de los imports
y ANTES de las funciones de comando (antes de async def cmd_start).

DEPENDENCIAS:
- Requires: from zoneinfo import ZoneInfo
- Requires: import time
- Requires: from typing import Dict, Tuple, Optional, Any
"""

from datetime import datetime
from zoneinfo import ZoneInfo
import time
from typing import Dict, Tuple, Optional, Any

AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

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

bot_hibernating = False
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
        # Job para INICIAR hibernación a las 22:00
        application.job_queue.run_daily(
            handle_hibernation_start,
            time=datetime.strptime("22:00", "%H:%M").time(),
            name="hibernation_start"
        )
        
        # Job para TERMINAR hibernación a las 06:00
        application.job_queue.run_daily(
            handle_hibernation_end,
            time=datetime.strptime("06:00", "%H:%M").time(),
            name="hibernation_end"
        )
        
        logger.info("✅ Jobs de hibernación programados (22:00-06:00)")
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
