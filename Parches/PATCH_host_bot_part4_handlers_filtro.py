# -*- coding: utf-8 -*-
"""
PARCHE PARA host_bot.py - PARTE 4: FILTRO DE VENDEDOR Y HANDLERS

Esta es la parte MÁS CRÍTICA del cambio.

MODIFICACIONES:
1. handle_photo() - Agregar filtro de vendedor y registro de usuario
2. handle_text() - Agregar registro de usuario
3. Otros handlers - Agregar registro de usuario donde aplique
4. Jobs - Agregar verificación de hibernación
"""

# ============================================================================
# MODIFICACIÓN CRÍTICA: handle_photo() - FILTRO DE VENDEDOR
# ============================================================================

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handler de fotos - SOLO VENDEDORES pueden enviar exhibiciones.
    
    CAMBIOS:
    1. Registrar usuario en KNOWN_USERS (auto-registro)
    2. Verificar hibernación
    3. FILTRO CRÍTICO: Solo procesar si el rol es "vendedor"
    4. Resto del código sin cambios
    """
    if not update.message or not update.message.photo: 
        return
    
    # Extraer info del usuario
    chat_id = update.message.chat.id
    user_id = update.message.from_user.id
    username = update.message.from_user.username or ""
    full_name = update.message.from_user.first_name or "Usuario"
    
    # 📝 PASO 1: Registrar interacción del usuario (auto-registro en KNOWN_USERS)
    register_user_interaction(chat_id, user_id, username, full_name)
    
    # 🌙 PASO 2: Verificar hibernación
    if bot_hibernating:
        logger.debug(f"Foto ignorada durante hibernación de {username}")
        return
    
    # 🏠 PASO 3: Verificar si soy host (sin cambios)
    if host_lock and not host_lock.is_host:
        logger.debug("No soy host, ignorando foto")
        return
    
    # 🔒 PASO 4: FILTRO CRÍTICO - Solo vendedores pueden enviar exhibiciones
    rol = get_cached_role(chat_id, user_id)
    
    if rol != "vendedor":
        logger.debug(f"📸 Foto de {username} ({rol}) IGNORADA - solo vendedores pueden enviar exhibiciones")
        return
    
    # ✅ A PARTIR DE AQUÍ: Código existente SIN CAMBIOS
    # El vendedor tiene permisos, procesar como siempre
    
    logger.info(f"📸 Foto recibida de vendedor {username} (ID: {user_id})")
    
    message_id = update.message.message_id
    file_id = update.message.photo[-1].file_id
    
    # Verificar semáforo (código existente)
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
    
    # Rate limiting (código existente)
    key = (chat_id, user_id)
    now = time.time()
    last_time = active_last_prompt.get(key, 0)
    
    if now - last_time < 3:
        logger.debug(f"Rate limit: {user_id} en {chat_id}")
        return
    
    active_last_prompt[key] = now
    
    # Guardar sesión (código existente)
    if user_id not in upload_sessions:
        upload_sessions[user_id] = {
            "chat_id": chat_id,
            "chat_title": (getattr(update.message.chat, "title", None) or 
                          getattr(update.message.chat, "full_name", None) or 
                          getattr(update.message.chat, "username", None) or 
                          str(chat_id)),
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
    
    # Pedir NRO_CLIENTE (código existente)
    try:
        await update.message.reply_text(
            f"📸 Foto recibida. Por favor, envía el <b>NRO CLIENTE</b> (solo números):",
            parse_mode=ParseMode.HTML,
            reply_to_message_id=message_id
        )
    except Exception as e:
        logger.error(f"Error enviando respuesta: {e}")


# ============================================================================
# MODIFICACIÓN: handle_text() - REGISTRO DE KNOWN USERS
# ============================================================================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handler de texto - REGISTRA usuarios conocidos.
    
    CAMBIOS:
    1. Registrar usuario en KNOWN_USERS (auto-registro)
    2. Verificar hibernación
    3. Resto del código sin cambios
    """
    if not update.message or not update.message.text: 
        return
    
    # Extraer info del usuario
    chat_id = update.message.chat.id
    user_id = update.message.from_user.id
    username = update.message.from_user.username or ""
    full_name = update.message.from_user.first_name or "Usuario"
    
    # 📝 PASO 1: Registrar interacción del usuario
    register_user_interaction(chat_id, user_id, username, full_name)
    
    # 🌙 PASO 2: Verificar hibernación
    if bot_hibernating:
        return
    
    # 🏠 PASO 3: Verificar si soy host
    if host_lock and not host_lock.is_host:
        return
    
    # ✅ A PARTIR DE AQUÍ: Código existente SIN CAMBIOS
    text = update.message.text.strip()
    message_id = update.message.message_id
    
    session = upload_sessions.get(user_id)
    if not session:
        return

    session.setdefault("stage", STAGE_WAITING_ID)
    
    # ... resto del código existente de handle_text ...


# ============================================================================
# MODIFICACIÓN: OTROS HANDLERS - REGISTRO AUTOMÁTICO
# ============================================================================
#
# IMPORTANTE: Agregar registro automático en TODOS los handlers que procesan
# mensajes de usuarios en grupos.
#
# Ejemplo para cualquier handler:
#
# async def algún_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     if not update.message:
#         return
#     
#     chat_id = update.message.chat.id
#     user_id = update.message.from_user.id
#     username = update.message.from_user.username or ""
#     full_name = update.message.from_user.first_name or "Usuario"
#     
#     # Registrar interacción
#     register_user_interaction(chat_id, user_id, username, full_name)
#     
#     # ... resto del código ...
#


# ============================================================================
# MODIFICACIÓN: JOBS - PAUSAR DURANTE HIBERNACIÓN
# ============================================================================

async def sync_telegram_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Job de sincronización - SE PAUSA durante hibernación.
    
    CAMBIO: Agregar verificación al inicio.
    """
    # 🌙 VERIFICAR HIBERNACIÓN
    if bot_hibernating:
        return  # Salir inmediatamente si está hibernando
    
    # ✅ Código existente SIN CAMBIOS
    if host_lock and not host_lock.is_host:
        return
    
    try:
        # ... resto del código existente ...
        pass
    except Exception as e:
        logger.error(f"Error en sync job: {e}")


async def procesar_cola_imagenes_pendientes(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Job de procesamiento de cola - SE PAUSA durante hibernación.
    
    CAMBIO: Agregar verificación al inicio.
    """
    # 🌙 VERIFICAR HIBERNACIÓN
    if bot_hibernating:
        return  # Salir inmediatamente si está hibernando
    
    # ✅ Código existente SIN CAMBIOS
    if host_lock and not host_lock.is_host:
        return
    
    try:
        # ... resto del código existente ...
        pass
    except Exception as e:
        logger.error(f"Error procesando cola: {e}")


# NOTA: update_host_heartbeat NO debe verificar hibernación
# porque debe seguir ejecutándose para mantener el host activo.


# ============================================================================
# RESUMEN DE CAMBIOS MÍNIMOS EN HANDLERS
# ============================================================================
#
# Para CUALQUIER handler de mensajes en grupos, agregar estas 4 líneas
# al INICIO (después del if not update.message):
#
#     chat_id = update.message.chat.id
#     user_id = update.message.from_user.id
#     username = update.message.from_user.username or ""
#     full_name = update.message.from_user.first_name or "Usuario"
#     register_user_interaction(chat_id, user_id, username, full_name)
#
# Esto asegura que todos los usuarios queden registrados en KNOWN_USERS
# para poder asignarles roles con /setall_rol.
#
