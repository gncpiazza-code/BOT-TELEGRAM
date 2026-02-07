# -*- coding: utf-8 -*-
# file: semaforo_monitor.py
"""
Sistema de monitoreo del semáforo para coordinación con Apps Script.
Este archivo debe estar en la misma carpeta que host_bot.py
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

try:
    from logger_config import get_logger
    logger = get_logger(__name__)
except ImportError:
    logger = logging.getLogger("SemaforoMonitor")


class SemaforoMonitor:
    """
    Monitor que chequea el estado del semáforo cada X segundos
    y coordina la pausa/reanudación del procesamiento de imágenes.
    """
    
    def __init__(self, sheets_manager, intervalo_segundos: int = 5):
        """
        Args:
            sheets_manager: Instancia de SheetsManager
            intervalo_segundos: Cada cuántos segundos verificar (default: 5)
        """
        self.sheets = sheets_manager
        self.intervalo = intervalo_segundos
        
        # Estado interno
        self.estado_actual = "LIBRE"
        self.distribuyendo_desde: Optional[datetime] = None
        self.ultimo_chequeo: Optional[datetime] = None
        
        # Control del monitor
        self._running = False
        self._task: Optional[asyncio.Task] = None
        
        logger.info("🚦 SemaforoMonitor inicializado")
    
    async def start(self):
        """Inicia el monitoreo en background."""
        if self._running:
            logger.warning("Monitor ya está ejecutándose")
            return
        
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(f"✅ Monitor iniciado (intervalo: {self.intervalo}s)")
    
    async def stop(self):
        """Detiene el monitoreo."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Monitor detenido")
    
    async def _monitor_loop(self):
        """Loop principal que chequea el semáforo periódicamente."""
        while self._running:
            try:
                await self._check_semaforo()
                await asyncio.sleep(self.intervalo)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en monitor loop: {e}")
                await asyncio.sleep(self.intervalo)
    
    async def _check_semaforo(self):
        """Chequea el estado actual del semáforo."""
        try:
            # Ejecutar en thread pool para no bloquear el event loop
            loop = asyncio.get_event_loop()
            estado_info = await loop.run_in_executor(None, self.sheets.get_semaforo_estado)
            
            nuevo_estado = estado_info.get("estado", "LIBRE")
            self.ultimo_chequeo = datetime.now()
            
            # Detectar cambios de estado
            if nuevo_estado != self.estado_actual:
                await self._on_estado_changed(self.estado_actual, nuevo_estado, estado_info)
            
            self.estado_actual = nuevo_estado
            
        except Exception as e:
            logger.error(f"Error chequeando semáforo: {e}")
            # En caso de error, asumir LIBRE para no bloquear
            self.estado_actual = "LIBRE"
    
    async def _on_estado_changed(
        self, 
        estado_anterior: str, 
        estado_nuevo: str, 
        estado_info: Dict[str, Any]
    ):
        """Callback cuando cambia el estado del semáforo."""
        
        if estado_nuevo == "DISTRIBUYENDO":
            self.distribuyendo_desde = datetime.now()
            archivos_total = estado_info.get("archivos_total", 0)
            
            logger.warning("="*60)
            logger.warning("🚨 DISTRIBUCIÓN DETECTADA")
            logger.warning(f"   Total archivos: {archivos_total}")
            logger.warning(f"   Procesamiento de imágenes: PAUSADO")
            logger.warning("="*60)
            
        elif estado_nuevo == "LIBRE" and estado_anterior == "DISTRIBUYENDO":
            duracion = None
            if self.distribuyendo_desde:
                duracion = datetime.now() - self.distribuyendo_desde
            
            logger.info("="*60)
            logger.info("✅ DISTRIBUCIÓN COMPLETADA")
            if duracion:
                logger.info(f"   Duración: {duracion.seconds}s")
            logger.info(f"   Procesamiento de imágenes: REANUDADO")
            logger.info("="*60)
            
            # Procesar cola de imágenes pendientes
            await self._procesar_cola_pendientes()
    
    async def _procesar_cola_pendientes(self):
        """Procesa imágenes que llegaron durante la distribución."""
        try:
            loop = asyncio.get_event_loop()
            pendientes = await loop.run_in_executor(None, self.sheets.get_imagenes_pendientes)
            
            if not pendientes:
                logger.info("📭 No hay imágenes pendientes")
                return
            
            logger.info(f"📬 Procesando {len(pendientes)} imágenes pendientes...")
            
            # Aquí se procesarían las imágenes
            # Por ahora solo logueamos (la lógica de procesamiento va en host_bot.py)
            for img in pendientes:
                logger.debug(f"   - Imagen de {img['username']} (chat: {img['chat_id']})")
            
        except Exception as e:
            logger.error(f"Error procesando cola: {e}")
    
    def is_distribuyendo(self) -> bool:
        """
        Verifica si actualmente está en distribución.
        
        Returns:
            True si está distribuyendo, False si está libre
        """
        return self.estado_actual == "DISTRIBUYENDO"
    
    def get_estado(self) -> Dict[str, Any]:
        """
        Obtiene el estado actual del monitor.
        
        Returns:
            {
                "estado": "LIBRE" | "DISTRIBUYENDO",
                "distribuyendo_desde": datetime o None,
                "ultimo_chequeo": datetime o None
            }
        """
        return {
            "estado": self.estado_actual,
            "distribuyendo_desde": self.distribuyendo_desde,
            "ultimo_chequeo": self.ultimo_chequeo
        }
