# -*- coding: utf-8 -*-
# file: host_lock.py
"""
Sistema de host con GESTIÓN AVANZADA DE COLA Y TRASPASO.
- Takeover automático
- Traspaso programado con countdown
- Traspaso directo a bot específico
- Gestión de cola (mover, eliminar, limpiar)
- Sistema de estados (HOST, READY, WAITING, TRANSFERRING, OFFLINE)
- Intentos múltiples con fallback
- Historial de traspasos
"""

import os
import socket
import getpass
import time
import random
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

try:
    from logger_config import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger("HostLock")

try:
    from zoneinfo import ZoneInfo
    AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
except Exception:
    from datetime import timezone
    AR_TZ = timezone(timedelta(hours=-3))


class HostLock:
    """
    Sistema de host con gestión avanzada de cola y traspaso programado.
    """
    
    HOST_DEAD_TIMEOUT = 90  # 90 segundos sin heartbeat = muerto
    OFFLINE_TIMEOUT = 180   # 3 minutos sin heartbeat = offline (para limpieza)
    MAX_TRANSFER_ATTEMPTS = 3  # Intentos máximos de traspaso

    # Cuota de Google Sheets (429): throttle y backoff
    MIN_CLEANUP_INTERVAL = 60   # seg entre limpiezas pesadas
    MAX_QUOTA_BACKOFF = 120     # seg max de cooldown por 429
    HEADERS_CHECK_TTL = 600     # seg entre chequeos de headers
    
    # Estados posibles
    STATUS_HOST = "HOST"
    STATUS_READY = "READY"
    STATUS_WAITING = "WAITING"
    STATUS_TRANSFERRING = "TRANSFERRING"
    STATUS_OFFLINE = "OFFLINE"
    
    def __init__(self, sheets_manager, notify_callback=None):
        self.sheets = sheets_manager
        self.notify_callback = notify_callback
        
        self.hostname = socket.gethostname()
        self.user = getpass.getuser()
        self.pid = os.getpid()
        try:
            self.local_ip = socket.gethostbyname(self.hostname)
        except:
            self.local_ip = "Desconocida"
        
        self.machine_id = f"{self.user}@{self.hostname}"
        self.identity = f"{self.machine_id} (PID:{self.pid})"
        
        self.is_host = False
        self.last_heartbeat = None
        
        # Cache
        self._last_read_timestamp = 0
        self._last_read_data = None
        self.CACHE_TTL = 30

        # Throttle/backoff por cuota (429)
        self._cooldown_until = 0.0
        self._quota_backoff = 1.5
        self._last_cleanup_ts = 0.0

        # Cache de worksheet + headers
        self._host_ws = None
        self._headers_checked_at = 0.0
        
        logger.info(f"🔐 HostLock: {self.identity}")
        self._log_to_console("INIT", "Inicializado")
    
    # ========================================
    # MÉTODOS PRINCIPALES (existentes)
    # ========================================
    
    def try_acquire_host(self, force: bool = False) -> Dict[str, Any]:
        """Intenta tomar el host."""
        try:
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return self._error_result("No se pudo acceder a HOST_CONTROL")
            
            data = self._get_cached_data(ws)
            
            # Verificar si ya estamos en cola
            my_position = self._get_my_queue_position(data)
            if my_position > 0:
                logger.debug(f"Ya estamos en cola posición {my_position}")
                self._update_my_status(ws, self.STATUS_WAITING, my_position)
                return {
                    "success": True,
                    "is_host": False,
                    "message": f"⏳ Ya en cola ({my_position})",
                    "current_host": data[1][0] if len(data) >= 2 else None,
                    "queue_position": my_position
                }
            
            # Limpieza (throttle + cache para evitar 429)
            cleaned = self._cleanup_all(ws, data=data, force=force)
            data = self._get_cached_data(ws, force_refresh=cleaned or force)
            
            if len(data) < 2:
                return self._become_host(ws)
            
            current_host = data[1][0] if len(data[1]) > 0 else ""
            
            if not current_host:
                logger.info("✅ Sin host, tomando control")
                return self._become_host(ws)
            
            if self._is_same_machine(current_host):
                logger.info("♻️ Recuperando sesión")
                self.is_host = True
                self._update_heartbeat(ws)
                self._update_my_status(ws, self.STATUS_HOST)
                self._log_to_console("HOST_RECOVERED", "Recuperado")
                return {
                    "success": True,
                    "is_host": True,
                    "message": "♻️ Sesión recuperada",
                    "current_host": current_host,
                    "queue_position": None
                }
            
            if force:
                logger.warning("⚠️ FORZANDO host")
                return self._become_host(ws, forced=True)
            
            return self._join_queue(ws, current_host)
            
        except Exception as e:
            logger.error(f"Error en try_acquire_host: {e}")
            return self._error_result(str(e))
    
    def release_host(self) -> bool:
        """
        Libera el host NORMALMENTE (sin traspasar).
        Usado cuando el bot se cierra normalmente.
        """
        try:
            if not self.is_host:
                return True
            
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return False
            
            logger.info("🔓 Liberando host (cierre normal)")
            self._log_to_console("HOST_RELEASED", "Cierre normal")
            
            # Limpiar fila del host
            ws.update("A2:K2", [["", "", "", "", "", "", "", "", "", "", ""]])
            self._invalidate_cache()
            
            self.is_host = False
            
            # NO traspasar automáticamente - el siguiente tomará control vía takeover
            self._notify_event("HOST_RELEASED", f"🔓 Host liberado (cierre normal)\n   {self.identity}\n   ⏳ Siguiente en cola tomará control vía takeover")
            
            return True
            
        except Exception as e:
            logger.error(f"Error en release_host: {e}")
            return False
    
    def update_heartbeat(self) -> bool:
        """Actualiza heartbeat."""
        if not self.is_host:
            return False
        
        try:
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return False
            
            return self._update_heartbeat(ws)
            
        except Exception as e:
            logger.error(f"Error en update_heartbeat: {e}")
            return False
    
    def check_and_takeover_if_dead(self) -> bool:
        """
        Verifica si el host está muerto y toma control.
        También ejecuta traspasos programados si es el momento.
        """
        try:
            if self.is_host:
                # Si soy host, verificar si hay traspaso programado
                transfer_status = self.get_transfer_status()
                if transfer_status.get("scheduled") and transfer_status.get("ready_to_execute"):
                    logger.info("⏰ Ejecutando traspaso programado...")
                    success = self.execute_scheduled_transfer()
                    if success:
                        logger.info("✅ Traspaso ejecutado exitosamente")
                        return False  # Ya no soy host
                    else:
                        logger.warning("⚠️ Traspaso falló, manteniendo host")
                
                # Solo actualizar heartbeat
                return self.update_heartbeat()
            
            # Si no soy host, intentar takeover
            result = self.try_acquire_host()
            
            if result["is_host"]:
                logger.info("👑 TAKEOVER AUTOMÁTICO!")
                self._notify_event("TAKEOVER", f"⚡ TAKEOVER AUTOMÁTICO\n👑 Nuevo host: {self.identity}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error en check_and_takeover_if_dead: {e}")
            return False
    
    def get_host_info(self) -> Dict[str, Any]:
        """Obtiene info del host y cola con estados."""
        try:
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return self._empty_host_info()
            
            data = self._get_cached_data(ws)
            
            if len(data) < 2:
                return self._empty_host_info()
            
            current_host = data[1][0] if len(data[1]) > 0 else None
            
            host_details = {}
            if current_host:
                host_details = {
                    "identity": data[1][0] if len(data[1]) > 0 else "",
                    "hostname": data[1][1] if len(data[1]) > 1 else "",
                    "user": data[1][2] if len(data[1]) > 2 else "",
                    "ip": data[1][3] if len(data[1]) > 3 else "",
                    "pid": data[1][4] if len(data[1]) > 4 else "",
                    "started_at": data[1][5] if len(data[1]) > 5 else "",
                    "last_heartbeat": data[1][6] if len(data[1]) > 6 else "",
                    "status": data[1][7] if len(data[1]) > 7 else self.STATUS_HOST,
                }
            
            queue = []
            for i in range(2, len(data)):
                if len(data[i]) > 0 and data[i][0]:
                    status = data[i][7] if len(data[i]) > 7 else self.STATUS_WAITING
                    last_hb = data[i][6] if len(data[i]) > 6 else ""
                    
                    # Calcular si está offline
                    is_offline = False
                    if last_hb:
                        try:
                            hb_time = datetime.strptime(last_hb, "%d/%m/%Y %H:%M:%S")
                            hb_time = hb_time.replace(tzinfo=AR_TZ)
                            now = datetime.now(AR_TZ)
                            diff = (now - hb_time).total_seconds()
                            if diff > self.OFFLINE_TIMEOUT:
                                is_offline = True
                                status = self.STATUS_OFFLINE
                        except:
                            pass
                    
                    queue.append({
                        "identity": data[i][0],
                        "hostname": data[i][1] if len(data[i]) > 1 else "",
                        "user": data[i][2] if len(data[i]) > 2 else "",
                        "ip": data[i][3] if len(data[i]) > 3 else "",
                        "pid": data[i][4] if len(data[i]) > 4 else "",
                        "joined_at": data[i][5] if len(data[i]) > 5 else "",
                        "last_heartbeat": last_hb,
                        "status": status,
                        "is_offline": is_offline,
                        "position": i - 1
                    })
            
            is_host = self._is_same_machine(current_host) if current_host else False
            
            return {
                "current_host": current_host,
                "host_details": host_details,
                "queue": queue,
                "is_host": is_host,
                "my_identity": self.identity
            }
            
        except Exception as e:
            logger.error(f"Error en get_host_info: {e}")
            return self._empty_host_info()
    
    # ========================================
    # NUEVOS MÉTODOS - GESTIÓN DE COLA
    # ========================================
    
    def move_in_queue(self, identity: str, new_position: int) -> Dict[str, Any]:
        """
        Mueve un bot a una nueva posición en la cola.
        Solo el host puede hacer esto.
        """
        try:
            if not self.is_host:
                return {"success": False, "message": "❌ Solo el host puede reordenar"}
            
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return {"success": False, "message": "❌ Error accediendo a Sheets"}
            
            data = self._get_cached_data(ws, force_refresh=True)
            
            # Encontrar el bot en la cola
            bot_row = None
            current_position = None
            for i in range(2, len(data)):
                if len(data[i]) > 0 and data[i][0] == identity:
                    bot_row = i + 1  # Row number (1-indexed)
                    current_position = i - 1  # Queue position (0-indexed from position 1)
                    break
            
            if bot_row is None:
                return {"success": False, "message": f"❌ Bot no encontrado: {identity}"}
            
            # Validar nueva posición
            queue_size = len(data) - 2  # Restar header y host
            if new_position < 1 or new_position > queue_size:
                return {"success": False, "message": f"❌ Posición inválida: {new_position}"}
            
            if current_position == new_position:
                return {"success": True, "message": "✅ Ya está en esa posición"}
            
            # Calcular nueva fila (sheet row number)
            new_row = new_position + 2  # +2 porque: +1 para header, +1 para host
            
            # Obtener datos de la fila a mover
            bot_data = data[bot_row - 1]
            
            # Insertar en nueva posición
            ws.insert_row(bot_data, new_row)
            
            # Borrar fila antigua (ajustar índice si se movió hacia arriba)
            old_row_adjusted = bot_row + 1 if new_row < bot_row else bot_row
            ws.delete_rows(old_row_adjusted)
            
            # Actualizar posiciones en columna K
            self._update_queue_positions(ws)
            
            self._invalidate_cache()
            
            logger.info(f"📊 Cola reordenada: {identity} → pos {new_position}")
            self._log_to_console("QUEUE_REORDER", f"{identity} → pos {new_position}")
            self._notify_event("QUEUE_REORDER", f"📊 Cola reordenada\n   Bot: {identity}\n   Nueva posición: {new_position}")
            
            return {"success": True, "message": f"✅ Movido a posición {new_position}"}
            
        except Exception as e:
            logger.error(f"Error en move_in_queue: {e}")
            return {"success": False, "message": f"❌ Error: {e}"}
    
    def remove_from_queue(self, identity: str) -> Dict[str, Any]:
        """
        Elimina un bot de la cola.
        Solo el host puede hacer esto.
        """
        try:
            if not self.is_host:
                return {"success": False, "message": "❌ Solo el host puede eliminar"}
            
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return {"success": False, "message": "❌ Error accediendo a Sheets"}
            
            data = self._get_cached_data(ws, force_refresh=True)
            
            # Encontrar el bot
            bot_row = None
            for i in range(2, len(data)):
                if len(data[i]) > 0 and data[i][0] == identity:
                    bot_row = i + 1
                    break
            
            if bot_row is None:
                return {"success": False, "message": f"❌ Bot no encontrado: {identity}"}
            
            # Eliminar fila
            ws.delete_rows(bot_row)
            
            # Actualizar posiciones
            self._update_queue_positions(ws)
            
            self._invalidate_cache()
            
            logger.info(f"🗑️ Bot eliminado de cola: {identity}")
            self._log_to_console("QUEUE_REMOVE", f"Eliminado: {identity}")
            self._notify_event("QUEUE_REMOVE", f"🗑️ Bot eliminado de cola\n   {identity}")
            
            return {"success": True, "message": f"✅ Eliminado: {identity}"}
            
        except Exception as e:
            logger.error(f"Error en remove_from_queue: {e}")
            return {"success": False, "message": f"❌ Error: {e}"}
    
    def cleanup_dead_bots(self) -> Dict[str, Any]:
        """
        Limpia bots sin heartbeat (offline).
        Solo el host puede hacer esto.
        """
        try:
            if not self.is_host:
                return {"success": False, "message": "❌ Solo el host puede limpiar", "removed": 0}
            
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return {"success": False, "message": "❌ Error accediendo a Sheets", "removed": 0}
            
            data = self._get_cached_data(ws, force_refresh=True)
            now = datetime.now(AR_TZ)
            
            to_remove = []
            
            for i in range(2, len(data)):
                if len(data[i]) > 0 and data[i][0]:
                    last_hb = data[i][6] if len(data[i]) > 6 else ""
                    
                    if last_hb:
                        try:
                            hb_time = datetime.strptime(last_hb, "%d/%m/%Y %H:%M:%S")
                            hb_time = hb_time.replace(tzinfo=AR_TZ)
                            diff = (now - hb_time).total_seconds()
                            
                            if diff > self.OFFLINE_TIMEOUT:
                                to_remove.append({
                                    "row": i + 1,
                                    "identity": data[i][0],
                                    "seconds": int(diff)
                                })
                        except:
                            pass
            
            # Eliminar en orden inverso para no desajustar índices
            for item in reversed(to_remove):
                ws.delete_rows(item["row"])
                logger.info(f"🧹 Limpiado bot offline: {item['identity']} ({item['seconds']}s sin HB)")
            
            if to_remove:
                self._update_queue_positions(ws)
                self._invalidate_cache()
                
                removed_identities = "\n   ".join([f"• {item['identity']}" for item in to_remove])
                self._notify_event("QUEUE_CLEANUP", f"🧹 Limpieza de cola\n   Bots eliminados ({len(to_remove)}):\n   {removed_identities}")
            
            return {
                "success": True,
                "message": f"✅ Limpiados {len(to_remove)} bots",
                "removed": len(to_remove),
                "details": to_remove
            }
            
        except Exception as e:
            logger.error(f"Error en cleanup_dead_bots: {e}")
            return {"success": False, "message": f"❌ Error: {e}", "removed": 0}
    
    def get_queue_details(self) -> List[Dict[str, Any]]:
        """
        Obtiene detalles completos de la cola con estados.
        """
        try:
            info = self.get_host_info()
            return info.get("queue", [])
        except Exception as e:
            logger.error(f"Error en get_queue_details: {e}")
            return []
    
    # ========================================
    # NUEVOS MÉTODOS - TRASPASO PROGRAMADO
    # ========================================
    
    def schedule_transfer(self, minutes: int, target_identity: str = None) -> Dict[str, Any]:
        """
        Programa un traspaso para dentro de X minutos.
        Si target_identity es None, pasa al siguiente en cola.
        """
        try:
            if not self.is_host:
                return {"success": False, "message": "❌ Solo el host puede programar traspasos"}
            
            if minutes < 1:
                return {"success": False, "message": "❌ Mínimo 1 minuto"}
            
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return {"success": False, "message": "❌ Error accediendo a Sheets"}
            
            # Calcular timestamp
            transfer_time = datetime.now(AR_TZ) + timedelta(minutes=minutes)
            transfer_timestamp = transfer_time.strftime("%d/%m/%Y %H:%M:%S")
            
            # Si hay target específico, validar que exista en cola
            if target_identity:
                data = self._get_cached_data(ws, force_refresh=True)
                found = False
                for i in range(2, len(data)):
                    if len(data[i]) > 0 and data[i][0] == target_identity:
                        found = True
                        break
                
                if not found:
                    return {"success": False, "message": f"❌ Bot no encontrado en cola: {target_identity}"}
            
            # Escribir en columnas I y J
            ws.update("I2:J2", [[transfer_timestamp, target_identity or ""]])
            self._invalidate_cache()
            
            target_msg = f"a {target_identity}" if target_identity else "al siguiente en cola"
            logger.info(f"⏰ Traspaso programado: {minutes} min {target_msg}")
            self._log_to_console("TRANSFER_SCHEDULED", f"{minutes}min {target_msg}")
            self._notify_event("TRANSFER_SCHEDULED", f"⏰ Traspaso programado\n   En: {minutes} minutos\n   Destino: {target_identity or 'Siguiente en cola'}\n   Hora: {transfer_time.strftime('%H:%M:%S')}")
            
            return {
                "success": True,
                "message": f"✅ Traspaso programado en {minutes} min",
                "transfer_at": transfer_timestamp,
                "target": target_identity
            }
            
        except Exception as e:
            logger.error(f"Error en schedule_transfer: {e}")
            return {"success": False, "message": f"❌ Error: {e}"}
    
    def cancel_scheduled_transfer(self) -> Dict[str, Any]:
        """Cancela un traspaso programado."""
        try:
            if not self.is_host:
                return {"success": False, "message": "❌ Solo el host puede cancelar"}
            
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return {"success": False, "message": "❌ Error accediendo a Sheets"}
            
            # Limpiar columnas I y J
            ws.update("I2:J2", [["", ""]])
            self._invalidate_cache()
            
            logger.info("❌ Traspaso cancelado")
            self._log_to_console("TRANSFER_CANCELLED", "Cancelado por host")
            self._notify_event("TRANSFER_CANCELLED", "❌ Traspaso cancelado\n   Por decisión del host")
            
            return {"success": True, "message": "✅ Traspaso cancelado"}
            
        except Exception as e:
            logger.error(f"Error en cancel_scheduled_transfer: {e}")
            return {"success": False, "message": f"❌ Error: {e}"}
    
    def get_transfer_status(self) -> Dict[str, Any]:
        """Obtiene el estado del traspaso programado."""
        try:
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return {"scheduled": False}
            
            data = self._get_cached_data(ws)
            
            if len(data) < 2 or len(data[1]) < 10:
                return {"scheduled": False}
            
            transfer_at_str = data[1][8] if len(data[1]) > 8 else ""
            target = data[1][9] if len(data[1]) > 9 else ""
            
            if not transfer_at_str:
                return {"scheduled": False}
            
            try:
                transfer_time = datetime.strptime(transfer_at_str, "%d/%m/%Y %H:%M:%S")
                transfer_time = transfer_time.replace(tzinfo=AR_TZ)
                now = datetime.now(AR_TZ)
                
                remaining_seconds = (transfer_time - now).total_seconds()
                
                return {
                    "scheduled": True,
                    "transfer_at": transfer_at_str,
                    "target": target or "Siguiente en cola",
                    "remaining_seconds": max(0, remaining_seconds),
                    "ready_to_execute": remaining_seconds <= 0
                }
            except:
                return {"scheduled": False}
            
        except Exception as e:
            logger.error(f"Error en get_transfer_status: {e}")
            return {"scheduled": False}
    
    def execute_scheduled_transfer(self) -> bool:
        """
        Ejecuta el traspaso programado.
        Intenta hasta MAX_TRANSFER_ATTEMPTS veces.
        """
        try:
            if not self.is_host:
                logger.warning("No soy host, no puedo ejecutar traspaso")
                return False
            
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                logger.error("No se pudo acceder a HOST_CONTROL")
                return False
            
            data = self._get_cached_data(ws, force_refresh=True)
            
            # Obtener target
            target = data[1][9] if len(data[1]) > 9 else ""
            
            attempts = 0
            success = False
            
            while attempts < self.MAX_TRANSFER_ATTEMPTS and not success:
                attempts += 1
                
                # Encontrar bot destino
                if target:
                    # Traspaso directo a bot específico
                    next_bot = None
                    next_row = None
                    
                    for i in range(2, len(data)):
                        if len(data[i]) > 0 and data[i][0] == target:
                            next_bot = data[i]
                            next_row = i + 1
                            break
                    
                    if not next_bot:
                        logger.warning(f"Intento {attempts}: Bot {target} no encontrado, buscando siguiente...")
                        target = ""  # Cambiar a modo "siguiente en cola"
                        data = self._get_cached_data(ws, force_refresh=True)
                        continue
                else:
                    # Traspaso al siguiente en cola
                    next_bot = None
                    next_row = None
                    
                    for i in range(2, len(data)):
                        if len(data[i]) > 0 and data[i][0]:
                            # Verificar heartbeat
                            last_hb = data[i][6] if len(data[i]) > 6 else ""
                            if last_hb:
                                try:
                                    hb_time = datetime.strptime(last_hb, "%d/%m/%Y %H:%M:%S")
                                    hb_time = hb_time.replace(tzinfo=AR_TZ)
                                    now = datetime.now(AR_TZ)
                                    diff = (now - hb_time).total_seconds()
                                    
                                    if diff <= self.OFFLINE_TIMEOUT:
                                        next_bot = data[i]
                                        next_row = i + 1
                                        break
                                    else:
                                        logger.warning(f"Intento {attempts}: Bot en pos {i-1} está offline ({diff}s), saltando...")
                                except:
                                    pass
                
                if not next_bot:
                    logger.warning(f"Intento {attempts}: No hay siguiente bot válido")
                    if attempts < self.MAX_TRANSFER_ATTEMPTS:
                        time.sleep(2)
                        data = self._get_cached_data(ws, force_refresh=True)
                        continue
                    else:
                        break
                
                # Ejecutar traspaso
                try:
                    next_identity = next_bot[0]
                    logger.info(f"🔄 Intento {attempts}: Traspasando a {next_identity}")
                    
                    # Actualizar fila del host con datos del siguiente
                    timestamp = self._get_timestamp()
                    ws.update("A2:K2", [[
                        next_bot[0],  # identity
                        next_bot[1] if len(next_bot) > 1 else "",  # hostname
                        next_bot[2] if len(next_bot) > 2 else "",  # user
                        next_bot[3] if len(next_bot) > 3 else "",  # ip
                        next_bot[4] if len(next_bot) > 4 else "",  # pid
                        next_bot[5] if len(next_bot) > 5 else timestamp,  # started_at
                        timestamp,  # last_heartbeat
                        self.STATUS_HOST,  # status
                        "",  # transfer_scheduled_at (limpiar)
                        "",  # transfer_to (limpiar)
                        ""   # queue_position (limpiar)
                    ]])
                    
                    # Eliminar de cola
                    ws.delete_rows(next_row)
                    
                    # Actualizar posiciones
                    self._update_queue_positions(ws)
                    
                    self._invalidate_cache()
                    
                    # Log en historial
                    self._log_transfer_history(self.identity, next_identity, "SCHEDULED", True)
                    
                    logger.info(f"✅ Traspaso exitoso a {next_identity}")
                    self._log_to_console("TRANSFER_SUCCESS", f"A {next_identity}")
                    self._notify_event("TRANSFER_SUCCESS", f"🔄 Traspaso completado\n   De: {self.identity}\n   A: {next_identity}\n   Intentos: {attempts}/{self.MAX_TRANSFER_ATTEMPTS}")
                    
                    self.is_host = False
                    success = True
                    break
                    
                except Exception as e:
                    logger.error(f"Intento {attempts} falló: {e}")
                    if attempts < self.MAX_TRANSFER_ATTEMPTS:
                        time.sleep(2)
                        data = self._get_cached_data(ws, force_refresh=True)
            
            if not success:
                logger.error(f"❌ Traspaso falló después de {self.MAX_TRANSFER_ATTEMPTS} intentos")
                
                # Limpiar campos de traspaso programado
                ws.update("I2:J2", [["", ""]])
                self._invalidate_cache()
                
                self._log_transfer_history(self.identity, target or "AUTO", "SCHEDULED", False)
                self._notify_event("TRANSFER_FAILED", f"❌ Traspaso falló\n   De: {self.identity}\n   Intentos: {self.MAX_TRANSFER_ATTEMPTS}\n   ⚠️ Manteniendo host actual")
                
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error en execute_scheduled_transfer: {e}")
            return False
    
    def transfer_to(self, target_identity: str) -> Dict[str, Any]:
        """
        Traspaso directo inmediato a un bot específico.
        """
        try:
            if not self.is_host:
                return {"success": False, "message": "❌ Solo el host puede traspasar"}
            
            ws = self._get_or_create_host_control_sheet()
            if not ws:
                return {"success": False, "message": "❌ Error accediendo a Sheets"}
            
            data = self._get_cached_data(ws, force_refresh=True)
            
            # Encontrar bot destino
            target_bot = None
            target_row = None
            
            for i in range(2, len(data)):
                if len(data[i]) > 0 and data[i][0] == target_identity:
                    target_bot = data[i]
                    target_row = i + 1
                    break
            
            if not target_bot:
                return {"success": False, "message": f"❌ Bot no encontrado: {target_identity}"}
            
            # Verificar que esté vivo
            last_hb = target_bot[6] if len(target_bot) > 6 else ""
            if last_hb:
                try:
                    hb_time = datetime.strptime(last_hb, "%d/%m/%Y %H:%M:%S")
                    hb_time = hb_time.replace(tzinfo=AR_TZ)
                    now = datetime.now(AR_TZ)
                    diff = (now - hb_time).total_seconds()
                    
                    if diff > self.OFFLINE_TIMEOUT:
                        return {"success": False, "message": f"⚠️ Bot está offline ({int(diff)}s sin HB)"}
                except:
                    pass
            
            # Ejecutar traspaso
            logger.info(f"⚡ Traspaso directo a {target_identity}")
            
            timestamp = self._get_timestamp()
            ws.update("A2:K2", [[
                target_bot[0],
                target_bot[1] if len(target_bot) > 1 else "",
                target_bot[2] if len(target_bot) > 2 else "",
                target_bot[3] if len(target_bot) > 3 else "",
                target_bot[4] if len(target_bot) > 4 else "",
                target_bot[5] if len(target_bot) > 5 else timestamp,
                timestamp,
                self.STATUS_HOST,
                "",
                "",
                ""
            ]])
            
            ws.delete_rows(target_row)
            self._update_queue_positions(ws)
            self._invalidate_cache()
            
            self._log_transfer_history(self.identity, target_identity, "DIRECT", True)
            
            logger.info(f"✅ Traspaso directo exitoso")
            self._log_to_console("TRANSFER_DIRECT", f"A {target_identity}")
            self._notify_event("TRANSFER_DIRECT", f"⚡ Traspaso directo\n   De: {self.identity}\n   A: {target_identity}")
            
            self.is_host = False
            
            return {"success": True, "message": f"✅ Traspasado a {target_identity}"}
            
        except Exception as e:
            logger.error(f"Error en transfer_to: {e}")
            return {"success": False, "message": f"❌ Error: {e}"}
    
    # ========================================
    # MÉTODOS AUXILIARES
    # ========================================
    
    def _update_queue_positions(self, ws):
        """Actualiza la columna K (QUEUE_POSITION) para todos en cola."""
        try:
            data = self._get_cached_data(ws, force_refresh=True)
            
            updates = []
            for i in range(2, len(data)):  # Empezar desde fila 3 (primera en cola)
                if len(data[i]) > 0 and data[i][0]:
                    position = i - 1  # Posición en cola (1, 2, 3, ...)
                    updates.append({
                        "range": f"K{i+1}",
                        "values": [[str(position)]]
                    })
            
            if updates:
                ws.batch_update(updates)
            
        except Exception as e:
            logger.error(f"Error actualizando posiciones: {e}")
    
    def _update_my_status(self, ws, status: str, queue_position: int = None):
        """Actualiza el estado de este bot en Sheets."""
        try:
            data = self._get_cached_data(ws, force_refresh=True)
            
            for i in range(1, len(data)):
                if len(data[i]) > 0 and self._is_same_machine(data[i][0]):
                    updates = [{"range": f"H{i+1}", "values": [[status]]}]
                    
                    if queue_position is not None:
                        updates.append({"range": f"K{i+1}", "values": [[str(queue_position)]]})
                    
                    ws.batch_update(updates)
                    break
                    
        except Exception as e:
            logger.debug(f"Error actualizando status: {e}")
    
    def _log_transfer_history(self, from_identity: str, to_identity: str, reason: str, success: bool):
        """Registra el traspaso en el historial."""
        try:
            ws = self._get_or_create_transfer_history_sheet()
            if not ws:
                return
            
            timestamp = self._get_timestamp()
            status = "SUCCESS" if success else "FAILED"
            
            ws.append_row([
                timestamp,
                from_identity,
                to_identity,
                reason,
                status
            ])
            
        except Exception as e:
            logger.debug(f"Error registrando historial: {e}")
    
    def _get_or_create_transfer_history_sheet(self):
        """Obtiene o crea la hoja TRANSFER_HISTORY."""
        try:
            ws = self.sheets._get_ws("TRANSFER_HISTORY")
            if ws:
                return ws
            
            logger.info("📄 Creando TRANSFER_HISTORY...")
            
            if not self.sheets.spreadsheet:
                return None
            
            ws = self.sheets.spreadsheet.add_worksheet(
                title="TRANSFER_HISTORY",
                rows=200,
                cols=5
            )
            
            ws.update("A1:E1", [[
                "TIMESTAMP", "FROM", "TO", "REASON", "STATUS"
            ]])
            
            ws.format("A1:E1", {
                "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.6},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "horizontalAlignment": "CENTER"
            })
            
            self.sheets._ws_cache["TRANSFER_HISTORY"] = ws
            return ws
            
        except Exception as e:
            logger.error(f"Error TRANSFER_HISTORY: {e}")
            return None
    
    def _get_or_create_host_control_sheet(self):
        """Obtiene o crea HOST_CONTROL con columnas ampliadas.

        Importante: este método se llama muy seguido (GUI, heartbeat, takeover).
        Para evitar exceder cuota de lecturas, cachea el worksheet y evita
        leer headers en cada invocación.
        """
        try:
            if self._host_ws is not None:
                return self._host_ws

            ws = self.sheets._get_ws("HOST_CONTROL")
            if ws:
                self._host_ws = ws
                self._ensure_host_headers(ws)
                return ws

            logger.info("HOST_CONTROL no existe, creando...")
            ws = self.sheets._create_ws("HOST_CONTROL", rows=200, cols=11)
            if not ws:
                return None

            ws.update("A1:K1", [[
                "IDENTITY", "HOSTNAME", "USER", "IP", "PID",
                "STARTED_AT", "LAST_HEARTBEAT", "STATUS",
                "TRANSFER_SCHEDULED_AT", "TRANSFER_TO", "QUEUE_POSITION"
            ]])
            ws.update("A2:K2", [["", "", "", "", "", "", "", "", "", "", ""]])
            self._host_ws = ws
            self._headers_checked_at = time.time()
            logger.info("✅ HOST_CONTROL creado")
            return ws
        except Exception as e:
            logger.error(f"Error creando/obteniendo HOST_CONTROL: {e}")
            return None

    def _become_host(self, ws, forced: bool = False) -> Dict[str, Any]:
        """Toma el host."""
        try:
            timestamp = self._get_timestamp()
            
            ws.update("A2:K2", [[
                self.identity,
                self.hostname,
                self.user,
                self.local_ip,
                str(self.pid),
                timestamp,
                timestamp,
                self.STATUS_HOST,
                "",  # transfer_scheduled_at
                "",  # transfer_to
                ""   # queue_position
            ]])
            
            self._invalidate_cache()
            
            self.is_host = True
            self.last_heartbeat = time.time()
            
            event = "HOST_FORCED" if forced else "HOST_ACQUIRED"
            logger.info(f"👑 Host asignado: {self.identity}")
            self._log_to_console(event, "Asignado")
            self._notify_event("HOST_ACQUIRED", f"👑 Nuevo host asignado\n   {self.identity}")
            
            return {
                "success": True,
                "is_host": True,
                "message": "👑 Host asignado",
                "current_host": self.identity,
                "queue_position": None
            }
            
        except Exception as e:
            logger.error(f"Error en _become_host: {e}")
            return self._error_result(f"Error: {e}")
    
    def _join_queue(self, ws, current_host: str) -> Dict[str, Any]:
        """Agrega a cola."""
        try:
            timestamp = self._get_timestamp()
            
            # Calcular posición
            data = self._get_cached_data(ws, force_refresh=True)
            position = len(data) - 1  # -1 porque contamos desde 1
            
            ws.append_row([
                self.identity,
                self.hostname,
                self.user,
                self.local_ip,
                str(self.pid),
                timestamp,
                timestamp,  # last_heartbeat inicial
                self.STATUS_WAITING,
                "",  # transfer_scheduled_at
                "",  # transfer_to
                str(position)  # queue_position
            ])
            
            self._invalidate_cache()
            
            logger.info(f"📋 Cola posición {position}")
            self._log_to_console("QUEUE_JOIN", f"Posición {position}")
            self._notify_event("QUEUE_JOIN", f"📋 Nuevo bot en cola\n   {self.identity}\n📍 Posición: {position}")
            
            return {
                "success": True,
                "is_host": False,
                "message": f"📋 En cola ({position})",
                "current_host": current_host,
                "queue_position": position
            }
            
        except Exception as e:
            logger.error(f"Error en _join_queue: {e}")
            return self._error_result(f"Error: {e}")
    
    def _update_heartbeat(self, ws) -> bool:
        """Actualiza heartbeat."""
        try:
            timestamp = self._get_timestamp()
            ws.update("G2", [[timestamp]])
            self._invalidate_cache()
            self.last_heartbeat = time.time()
            return True
        except Exception as e:
            logger.error(f"Error heartbeat: {e}")
            return False
    
    def _get_timestamp(self) -> str:
        """Timestamp actual."""
        return datetime.now(AR_TZ).strftime("%d/%m/%Y %H:%M:%S")
    
    def _extract_machine_id(self, identity: str) -> Optional[str]:
        """Extrae 'user@host' de un identity string como 'user@host (PID:1234)'."""
        if not identity:
            return None
        # Formato: "user@hostname (PID:12345)"
        parts = identity.strip().split(" (PID:")
        return parts[0].strip() if parts[0].strip() else None

    def _is_same_machine(self, identity: str) -> bool:
        """Verifica si el identity es de esta máquina."""
        if not identity:
            return False
        return self.machine_id in identity
    
    def _get_my_queue_position(self, data) -> int:
        """Obtiene posición en cola (0 si no está)."""
        try:
            for i in range(2, len(data)):
                if len(data[i]) > 0 and self._is_same_machine(data[i][0]):
                    return i - 1
            return 0
        except:
            return 0
    
    def _get_cached_data(self, ws, force_refresh: bool = False):
        """Obtiene HOST_CONTROL con cache y control de cuota (429).

        - Usa cache (CACHE_TTL) para reducir lecturas.
        - Si detecta 429/RESOURCE_EXHAUSTED, entra en cooldown y devuelve cache.
        """
        now = time.time()

        # Si estamos en cooldown, devolvemos cache salvo que no exista.
        if now < self._cooldown_until and self._last_read_data:
            return self._last_read_data or []

        if force_refresh or (now - self._last_read_timestamp) > self.CACHE_TTL:
            try:
                data = self._read_with_backoff(lambda: ws.get_all_values(), op_name="get_all_values", allow_cache=True)
                if data is not None:
                    self._last_read_data = data
                    self._last_read_timestamp = now
            except Exception as e:
                logger.error(f"Error leyendo datos: {e}")
                return self._last_read_data or []

        return self._last_read_data or []

    def _invalidate_cache(self):
        """Invalida el cache."""
        self._last_read_timestamp = 0
        self._last_read_data = None
    

    def _ensure_host_headers(self, ws) -> None:
        """Chequea/actualiza headers de HOST_CONTROL con throttle para no leer siempre."""
        now = time.time()
        if (now - self._headers_checked_at) < self.HEADERS_CHECK_TTL:
            return
        try:
            headers = self._read_with_backoff(lambda: ws.row_values(1), op_name="row_values(1)", allow_cache=True)
            if headers is None:
                return
            if len(headers) < 11:
                ws.update("A1:K1", [[
                    "IDENTITY", "HOSTNAME", "USER", "IP", "PID",
                    "STARTED_AT", "LAST_HEARTBEAT", "STATUS",
                    "TRANSFER_SCHEDULED_AT", "TRANSFER_TO", "QUEUE_POSITION"
                ]])
                logger.info("✅ Headers de HOST_CONTROL actualizados")
        except Exception as e:
            logger.warning(f"No se pudo validar headers (se continúa): {e}")
        finally:
            self._headers_checked_at = now

    def _is_quota_error(self, exc: Exception) -> bool:
        """Detecta errores de cuota/429 de Google Sheets (best effort)."""
        msg = str(exc)
        if "429" in msg or "RESOURCE_EXHAUSTED" in msg or "quota" in msg.lower():
            return True
        if "Read requests per minute per user" in msg:
            return True
        try:
            from gspread.exceptions import APIError  # type: ignore
            if isinstance(exc, APIError):
                resp = getattr(exc, "response", None)
                if getattr(resp, "status_code", None) == 429:
                    return True
        except Exception:
            pass
        return False

    def _apply_quota_cooldown(self) -> None:
        """Aplica cooldown incremental para reducir martilleo ante 429."""
        self._quota_backoff = min(max(self._quota_backoff, 1.5) * 1.6, float(self.MAX_QUOTA_BACKOFF))
        jitter = random.uniform(0.0, 0.35 * self._quota_backoff)
        cooldown = min(self._quota_backoff + jitter, float(self.MAX_QUOTA_BACKOFF))
        self._cooldown_until = max(self._cooldown_until, time.time() + cooldown)
        logger.warning(f"🛑 Sheets quota: cooldown {cooldown:.1f}s (hasta {datetime.now(AR_TZ) + timedelta(seconds=cooldown)})")

    def _read_with_backoff(self, fn, op_name: str, allow_cache: bool = True):
        """Ejecuta lectura con backoff si hay 429. Devuelve None si cae a cache."""
        now = time.time()
        if allow_cache and now < self._cooldown_until and self._last_read_data:
            return None

        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                return fn()
            except Exception as e:
                if self._is_quota_error(e):
                    self._apply_quota_cooldown()
                    if allow_cache and self._last_read_data:
                        return None
                    # pequeña espera para no reintentar inmediatamente
                    time.sleep(min(1.0 * attempt, 2.5))
                    continue
                raise
        return None

    def _cleanup_all(self, ws, data: Optional[List[List[str]]] = None, force: bool = False) -> bool:
        """Limpieza de bots muertos y duplicados.

        Para evitar exceder cuota:
        - throttle por MIN_CLEANUP_INTERVAL (salvo force=True)
        - usa data cacheada cuando está disponible
        Retorna True si realizó cambios en la sheet.
        """
        try:
            now_ts = time.time()
            if not force and (now_ts - self._last_cleanup_ts) < self.MIN_CLEANUP_INTERVAL:
                return False
            self._last_cleanup_ts = now_ts

            if data is None:
                data = self._get_cached_data(ws)

            now = datetime.now(AR_TZ)
            to_delete = []
            seen = set()
            did_change = False

            # Verificar host (fila 2)
            if len(data) >= 2 and len(data[1]) > 6:
                host_hb = data[1][6]
                if host_hb:
                    try:
                        hb_time = datetime.strptime(host_hb, "%d/%m/%Y %H:%M:%S")
                        hb_time = hb_time.replace(tzinfo=AR_TZ)
                        diff = (now - hb_time).total_seconds()

                        if diff > self.HOST_DEAD_TIMEOUT:
                            logger.info(f"🧹 Host muerto ({diff:.0f}s), limpiando...")
                            ws.update("A2:K2", [["", "", "", "", "", "", "", "", "", "", ""]])
                            self._invalidate_cache()
                            return True
                    except Exception:
                        pass

            # Limpiar cola
            for i in range(2, len(data)):
                if len(data[i]) == 0 or not data[i][0]:
                    continue

                identity = data[i][0]
                hb_str = data[i][6] if len(data[i]) > 6 else ""
                status = data[i][7] if len(data[i]) > 7 else ""

                # Duplicados por machine_id (misma máquina)
                machine_key = self._extract_machine_id(identity)
                if machine_key:
                    if machine_key in seen:
                        to_delete.append(i + 1)  # 1-index sheet
                        continue
                    seen.add(machine_key)

                # Offline
                if hb_str:
                    try:
                        hb_time = datetime.strptime(hb_str, "%d/%m/%Y %H:%M:%S").replace(tzinfo=AR_TZ)
                        diff = (now - hb_time).total_seconds()
                        if diff > self.OFFLINE_TIMEOUT:
                            to_delete.append(i + 1)
                            continue
                        # marcar OFFLINE visualmente si pasó HOST_DEAD_TIMEOUT pero no borrar aún
                        if diff > self.HOST_DEAD_TIMEOUT and status not in (self.STATUS_OFFLINE, self.STATUS_HOST):
                            try:
                                ws.update_cell(i + 1, 8, self.STATUS_OFFLINE)
                                did_change = True
                            except Exception:
                                pass
                    except Exception:
                        pass

            # Borrado en reversa para mantener índices
            for row in sorted(set(to_delete), reverse=True):
                try:
                    ws.delete_rows(row)
                    did_change = True
                except Exception as e:
                    logger.warning(f"No se pudo borrar fila {row}: {e}")

            if did_change:
                self._invalidate_cache()
                try:
                    self._update_queue_positions(ws)
                except Exception:
                    pass

            return did_change
        except Exception as e:
            logger.error(f"Error en limpieza: {e}")
            return False

    def _log_to_console(self, event_type: str, message: str):
        """Log a CONSOLE."""
        try:
            ws = self._get_or_create_console_sheet()
            if not ws:
                return
            
            timestamp = self._get_timestamp()
            ws.append_row([
                timestamp,
                event_type,
                self.identity,
                str(self.pid),
                self.local_ip,
                message
            ])
        except:
            pass
    
    def _notify_event(self, event_type: str, message: str):
        """Notifica eventos importantes al superusuario."""
        try:
            if self.notify_callback:
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(self.notify_callback(event_type, message))
                    else:
                        loop.run_until_complete(self.notify_callback(event_type, message))
                except Exception as e:
                    logger.error(f"Error notificando evento {event_type}: {e}")
        except Exception as e:
            logger.debug(f"No se pudo notificar evento {event_type}: {e}")
    
    def _get_or_create_console_sheet(self):
        """Obtiene CONSOLE."""
        try:
            ws = self.sheets._get_ws("CONSOLE")
            if ws:
                return ws
            
            logger.info("📄 Creando CONSOLE...")
            
            if not self.sheets.spreadsheet:
                return None
            
            ws = self.sheets.spreadsheet.add_worksheet(
                title="CONSOLE",
                rows=500,
                cols=6
            )
            
            ws.update("A1:F1", [[
                "TIMESTAMP", "EVENT", "IDENTITY", "PID", "IP", "MESSAGE"
            ]])
            
            ws.format("A1:F1", {
                "backgroundColor": {"red": 0.1, "green": 0.1, "blue": 0.1},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "horizontalAlignment": "CENTER"
            })
            
            self.sheets._ws_cache["CONSOLE"] = ws
            return ws
            
        except Exception as e:
            logger.error(f"Error CONSOLE: {e}")
            return None
    
    def _error_result(self, message: str) -> Dict[str, Any]:
        """Resultado error."""
        return {
            "success": False,
            "is_host": False,
            "message": f"❌ {message}",
            "current_host": None,
            "queue_position": None
        }
    
    def _empty_host_info(self) -> Dict[str, Any]:
        """Info vacía."""
        return {
            "current_host": None,
            "host_details": {},
            "queue": [],
            "is_host": False,
            "my_identity": self.identity
        }