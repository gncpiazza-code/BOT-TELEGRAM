# -*- coding: utf-8 -*-
"""
PARCHE PARA host_bot.py - PARTE 2: COMANDO /setall_rol

UBICACIÓN: Agregar estas funciones en la sección de comandos.

DEPENDENCIAS:
- Requires: from telegram import InlineKeyboardButton, InlineKeyboardMarkup
- Requires: from telegram.constants import ParseMode
"""

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
        
        if not known_users:
            await update.message.reply_text(
                "👥 <b>No hay usuarios detectados en este grupo</b>\n\n"
                "Los usuarios aparecerán aquí cuando escriban en el grupo o envíen fotos.\n\n"
                "💡 Pediles que escriban algo en el grupo para registrarlos.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Obtener roles actuales
        all_roles = sheets.get_all_group_roles()
        roles_map = {}
        for role_info in all_roles:
            if role_info["chat_id"] == chat_id:
                roles_map[role_info["user_id"]] = role_info["rol"]
        
        # Construir lista de usuarios con sus roles actuales
        users_list = []
        for user in known_users:
            uid = user["user_id"]
            username = user["username"]
            full_name = user["full_name"]
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
        
        # Enviar configuración del primer usuario
        await send_role_config_for_user(update, context, user_id, 0)
        
    except Exception as e:
        logger.error(f"Error en /setall_rol: {e}")
        await update.message.reply_text(
            "❌ Error al cargar usuarios del grupo.",
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
        f"{'@' + user['username'] if user['username'] else f'ID: {user[\"user_id\"]}'}\n\n"
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
