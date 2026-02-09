# -*- coding: utf-8 -*-
"""
PARCHE PARA sheets_manager.py - SISTEMA DE ROLES POR GRUPO

INSTRUCCIONES:
1. Agregar estas estructuras en _check_structure_safe() dentro del dict "structures":
   - "GROUP_ROLES": ["CHAT_ID", "USER_ID", "USERNAME", "FULL_NAME", "ROL", "ASIGNADO_POR", "FECHA"]
   - "KNOWN_USERS": ["CHAT_ID", "USER_ID", "USERNAME", "FULL_NAME", "FIRST_SEEN", "LAST_SEEN"]

2. Agregar estos métodos al FINAL de la clase SheetsManager (antes de cerrar la clase)
"""

from datetime import datetime
from typing import Dict, List, Any, Tuple
from zoneinfo import ZoneInfo

AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")


# ============================================================================
# MÉTODOS PARA AGREGAR A LA CLASE SheetsManager
# ============================================================================

def _create_group_roles_sheet(self):
    """Crea la pestaña GROUP_ROLES para roles por grupo."""
    try:
        ws = self.spreadsheet.add_worksheet(title="GROUP_ROLES", rows=500, cols=7)
        ws.update("A1:G1", [[
            "CHAT_ID", "USER_ID", "USERNAME", "FULL_NAME", 
            "ROL", "ASIGNADO_POR", "FECHA"
        ]])
        
        # Formato de header
        ws.format("A1:G1", {
            "backgroundColor": {"red": 0.2, "green": 0.3, "blue": 0.5},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER"
        })
        
        logger.info("✅ Pestaña GROUP_ROLES creada")
        return ws
    except Exception as e:
        logger.error(f"Error creando GROUP_ROLES: {e}")
        return None


def _create_known_users_sheet(self):
    """Crea la pestaña KNOWN_USERS para registro automático de usuarios."""
    try:
        ws = self.spreadsheet.add_worksheet(title="KNOWN_USERS", rows=500, cols=6)
        ws.update("A1:F1", [[
            "CHAT_ID", "USER_ID", "USERNAME", "FULL_NAME", 
            "FIRST_SEEN", "LAST_SEEN"
        ]])
        
        # Formato de header
        ws.format("A1:F1", {
            "backgroundColor": {"red": 0.15, "green": 0.25, "blue": 0.35},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER"
        })
        
        logger.info("✅ Pestaña KNOWN_USERS creada")
        return ws
    except Exception as e:
        logger.error(f"Error creando KNOWN_USERS: {e}")
        return None


def get_all_group_roles(self) -> List[Dict[str, Any]]:
    """
    Obtiene TODOS los roles de TODOS los grupos.
    Para cache: se llama 1 vez cada 24hs.
    
    Returns:
        Lista de dicts con chat_id, user_id, rol
    """
    ws = self._get_ws("GROUP_ROLES")
    if not ws:
        # Si no existe, crearla
        ws = self._create_group_roles_sheet()
        if not ws:
            return []
    
    try:
        records = self._gspread_call(
            lambda: ws.get_all_records(),
            op='GROUP_ROLES:get_all',
            cache_key='group_roles:all',
            cache_ttl=600,  # 10 min (se invalida al usar /setall_rol)
            retries=2
        )
        
        result = []
        for r in records:
            chat_id_str = str(r.get("CHAT_ID", "")).strip()
            user_id_str = str(r.get("USER_ID", "")).strip()
            rol = str(r.get("ROL", "")).strip().lower()
            
            if chat_id_str and user_id_str and rol:
                try:
                    result.append({
                        "chat_id": int(float(chat_id_str)),
                        "user_id": int(float(user_id_str)),
                        "rol": rol
                    })
                except ValueError:
                    continue
        
        logger.info(f"📊 Cargados {len(result)} roles desde GROUP_ROLES")
        return result
        
    except Exception as e:
        logger.error(f"Error obteniendo roles: {e}")
        return []


def get_user_role_in_group(self, chat_id: int, user_id: int, default: str = "observador") -> str:
    """
    Obtiene el rol de un usuario en un grupo específico.
    
    Args:
        chat_id: ID del grupo
        user_id: ID del usuario
        default: Rol por defecto si no existe (default: "observador")
    
    Returns:
        Rol del usuario: "vendedor", "supervisor", "observador"
    """
    ws = self._get_ws("GROUP_ROLES")
    if not ws:
        return default
    
    try:
        # Buscar por chat_id + user_id
        all_vals = self._gspread_call(
            lambda: ws.get_all_values(),
            op='GROUP_ROLES:search',
            cache_key=f'role:{chat_id}:{user_id}',
            cache_ttl=300,
            retries=2
        )
        
        for row in all_vals[1:]:  # Skip header
            if len(row) < 5:
                continue
            
            try:
                row_chat = int(float(str(row[0]).strip()))
                row_user = int(float(str(row[1]).strip()))
                row_rol = str(row[4]).strip().lower()
                
                if row_chat == chat_id and row_user == user_id and row_rol:
                    return row_rol
            except (ValueError, IndexError):
                continue
        
        return default
        
    except Exception as e:
        logger.error(f"Error buscando rol: {e}")
        return default


def set_user_role_in_group(
    self, 
    chat_id: int, 
    user_id: int, 
    username: str,
    full_name: str,
    rol: str, 
    asignado_por: str
) -> bool:
    """
    Asigna o actualiza el rol de un usuario en un grupo.
    
    Args:
        chat_id: ID del grupo
        user_id: ID del usuario
        username: Username del usuario (sin @)
        full_name: Nombre completo
        rol: Rol a asignar ("vendedor", "supervisor", "observador")
        asignado_por: Quién asignó el rol
    
    Returns:
        True si se guardó correctamente
    """
    ws = self._get_ws("GROUP_ROLES")
    if not ws:
        ws = self._create_group_roles_sheet()
        if not ws:
            return False
    
    try:
        timestamp = datetime.now(AR_TZ).strftime("%d/%m/%Y %H:%M:%S")
        
        # Buscar si ya existe
        all_vals = ws.get_all_values()
        existing_row = None
        
        for i, row in enumerate(all_vals[1:], start=2):
            if len(row) < 2:
                continue
            try:
                row_chat = int(float(str(row[0]).strip()))
                row_user = int(float(str(row[1]).strip()))
                
                if row_chat == chat_id and row_user == user_id:
                    existing_row = i
                    break
            except (ValueError, IndexError):
                continue
        
        data = [
            str(chat_id),
            str(user_id),
            username,
            full_name,
            rol.lower(),
            asignado_por,
            timestamp
        ]
        
        if existing_row:
            # Actualizar fila existente
            ws.update(f"A{existing_row}:G{existing_row}", [data])
            logger.info(f"🔄 Rol actualizado: {full_name} → {rol} en grupo {chat_id}")
        else:
            # Agregar nueva fila
            ws.append_row(data, value_input_option="USER_ENTERED")
            logger.info(f"✅ Rol asignado: {full_name} → {rol} en grupo {chat_id}")
        
        # Invalidar cache
        self._local_cache.pop(f'role:{chat_id}:{user_id}', None)
        self._local_cache.pop('group_roles:all', None)
        
        return True
        
    except Exception as e:
        logger.error(f"Error asignando rol: {e}")
        return False


def get_known_users_in_group(self, chat_id: int) -> List[Dict[str, Any]]:
    """
    Obtiene lista de usuarios conocidos en un grupo.
    
    Returns:
        Lista de dicts con user_id, username, full_name, last_seen
    """
    ws = self._get_ws("KNOWN_USERS")
    if not ws:
        ws = self._create_known_users_sheet()
        if not ws:
            return []
    
    try:
        all_vals = self._gspread_call(
            lambda: ws.get_all_values(),
            op='KNOWN_USERS:get_group',
            cache_key=f'known_users:{chat_id}',
            cache_ttl=300,
            retries=2
        )
        
        result = []
        for row in all_vals[1:]:  # Skip header
            if len(row) < 6:
                continue
            
            try:
                row_chat = int(float(str(row[0]).strip()))
                if row_chat != chat_id:
                    continue
                
                result.append({
                    "user_id": int(float(str(row[1]).strip())),
                    "username": str(row[2]).strip(),
                    "full_name": str(row[3]).strip(),
                    "first_seen": str(row[4]).strip(),
                    "last_seen": str(row[5]).strip(),
                })
            except (ValueError, IndexError):
                continue
        
        return result
        
    except Exception as e:
        logger.error(f"Error obteniendo known users: {e}")
        return []


def register_known_user(
    self, 
    chat_id: int, 
    user_id: int, 
    username: str, 
    full_name: str
) -> bool:
    """
    Registra o actualiza un usuario conocido (auto-registro).
    Actualiza LAST_SEEN si ya existe.
    
    Returns:
        True si se registró/actualizó correctamente
    """
    ws = self._get_ws("KNOWN_USERS")
    if not ws:
        ws = self._create_known_users_sheet()
        if not ws:
            return False
    
    try:
        timestamp = datetime.now(AR_TZ).strftime("%d/%m/%Y %H:%M:%S")
        
        # Buscar si ya existe
        all_vals = ws.get_all_values()
        existing_row = None
        
        for i, row in enumerate(all_vals[1:], start=2):
            if len(row) < 2:
                continue
            try:
                row_chat = int(float(str(row[0]).strip()))
                row_user = int(float(str(row[1]).strip()))
                
                if row_chat == chat_id and row_user == user_id:
                    existing_row = i
                    break
            except (ValueError, IndexError):
                continue
        
        if existing_row:
            # Actualizar LAST_SEEN (columna F)
            ws.update(f"F{existing_row}", [[timestamp]])
            # Actualizar username/full_name por si cambió
            ws.update(f"C{existing_row}:D{existing_row}", [[username, full_name]])
        else:
            # Agregar nuevo usuario
            data = [
                str(chat_id),
                str(user_id),
                username,
                full_name,
                timestamp,  # FIRST_SEEN
                timestamp   # LAST_SEEN
            ]
            ws.append_row(data, value_input_option="USER_ENTERED")
            logger.info(f"👤 Usuario registrado: {full_name} (@{username}) en grupo {chat_id}")
        
        # Invalidar cache
        self._local_cache.pop(f'known_users:{chat_id}', None)
        
        return True
        
    except Exception as e:
        logger.error(f"Error registrando known user: {e}")
        return False


# ============================================================================
# NOTA SOBRE MÉTODOS ANTIGUOS
# ============================================================================
# 
# Los métodos get_user_role() y set_user_role() originales están DEPRECADOS.
# Pueden quedar temporalmente para compatibilidad, pero deberían eliminarse
# después de migrar todos los grupos a GROUP_ROLES usando /setall_rol.
#
