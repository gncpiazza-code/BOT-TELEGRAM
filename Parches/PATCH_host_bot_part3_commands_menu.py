# -*- coding: utf-8 -*-
"""
PARCHE PARA host_bot.py - PARTE 3: COMANDOS /mirol, /help Y MENÚ

UBICACIÓN: Agregar/modificar estos comandos en la sección de comandos.
"""

import os
import sys

# ============================================================================
# COMANDO /mirol - VER MIS ROLES
# ============================================================================

async def cmd_mirol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra los roles del usuario en todos los grupos donde participa."""
    if not update.message:
        return
    
    user_id = update.message.from_user.id
    
    try:
        # Obtener todos los roles
        all_roles = sheets.get_all_group_roles()
        
        # Filtrar roles del usuario actual
        user_roles = []
        for role_info in all_roles:
            if role_info["user_id"] == user_id:
                chat_id = role_info["chat_id"]
                rol = role_info["rol"]
                
                # Obtener nombre del grupo
                try:
                    chat = await context.bot.get_chat(chat_id)
                    group_name = chat.title or f"Grupo {chat_id}"
                except:
                    group_name = f"Grupo {chat_id}"
                
                user_roles.append({
                    "group_name": group_name,
                    "rol": rol
                })
        
        if not user_roles:
            await update.message.reply_text(
                "👤 <b>Tus Roles</b>\n\n"
                "No tenés roles asignados en ningún grupo.\n\n"
                "💡 Contactá al administrador para que te asigne un rol con /setall_rol",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Construir mensaje
        msg_text = "👤 <b>Tus Roles</b>\n\n"
        
        for role_info in user_roles:
            emoji = {
                "vendedor": "🛒",
                "supervisor": "👁️",
                "observador": "📋"
            }.get(role_info["rol"], "❓")
            
            msg_text += f"{emoji} <b>{role_info['group_name']}</b>\n"
            msg_text += f"   Rol: {role_info['rol'].capitalize()}\n\n"
        
        msg_text += (
            "<b>Significado de los roles:</b>\n"
            "🛒 <b>Vendedor</b>: Puede enviar exhibiciones\n"
            "👁️ <b>Supervisor</b>: Puede evaluar exhibiciones\n"
            "📋 <b>Observador</b>: Solo puede ver"
        )
        
        await update.message.reply_text(msg_text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Error en /mirol: {e}")
        await update.message.reply_text("❌ Error al obtener tus roles")


# ============================================================================
# COMANDO /help MODIFICADO (con foto)
# ============================================================================

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


# ============================================================================
# ACTUALIZACIÓN AUTOMÁTICA DEL MENÚ DE COMANDOS
# ============================================================================

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


# ============================================================================
# MODIFICAR post_init PARA AGREGAR CONFIGURACIÓN
# ============================================================================
#
# En la función post_init(), AL FINAL, agregar:
#
#     # Configurar menú de comandos
#     await setup_bot_commands(application)
#
#     # Inicializar extensiones (roles, hibernación)
#     await post_init_extensions(application)
#


# ============================================================================
# MODIFICACIÓN: cmd_stats (CON HIBERNACIÓN)
# ============================================================================

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra estadísticas del vendedor - usa snapshot durante hibernación."""
    if not update.message: 
        return
    
    uid = update.message.from_user.id
    
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


# ============================================================================
# MODIFICACIÓN: cmd_ranking (CON HIBERNACIÓN)
# ============================================================================

async def cmd_ranking(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra ranking del mes - usa snapshot durante hibernación."""
    if not update.message: 
        return
    
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


# ============================================================================
# IMPORTS NECESARIOS
# ============================================================================
#
# Agregar estos imports al inicio del archivo:
#
# from telegram import BotCommand
# import os
# import sys
#
