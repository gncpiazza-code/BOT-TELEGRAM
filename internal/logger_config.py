# -*- coding: utf-8 -*-
# file: logger_config.py
"""
Configuración centralizada de logging para todo el proyecto.
AHORA INCLUYE: Visualización del PID (Process ID) en cada línea para detectar dobles ejecuciones.
"""

import logging
import sys
import os
import socket
from datetime import datetime
from pathlib import Path
from typing import Optional


# ============================================
# COLORES PARA CONSOLA
# ============================================
class ColoredFormatter(logging.Formatter):
    """Formateador con colores para la consola."""
    
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        # Agregar color al nivel
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.RESET}"
        
        # Formato del mensaje
        return super().format(record)


# ============================================
# CONFIGURACIÓN GLOBAL
# ============================================
def setup_logging(
    log_level: int = logging.INFO,
    log_to_file: bool = True,
    log_file: Optional[str] = None,
    detailed: bool = True,
) -> None:
    """
    Configura el sistema de logging para todo el proyecto.
    
    Args:
        log_level: Nivel de logging
        log_to_file: Si True, también guarda logs en archivo
        log_file: Ruta del archivo de log
        detailed: Si True, incluye nombre del módulo, línea y PID
    """
    
    # ✅ NUEVO FORMATO: Incluye PID:%(process)d para rastrear procesos fantasmas
    if detailed:
        # Ejemplo: 2024-01-01 12:00:00 | PID:12345 | INFO | module | func | L10 | msg
        fmt = '%(asctime)s | PID:%(process)-6d | %(levelname)-8s | %(name)-20s | %(funcName)-25s | L%(lineno)-4d | %(message)s'
    else:
        fmt = '%(asctime)s | PID:%(process)-6d | %(levelname)-8s | %(name)-15s | %(message)s'
    
    date_fmt = '%Y-%m-%d %H:%M:%S'
    
    # Configurar root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Limpiar handlers existentes para evitar duplicados
    root_logger.handlers.clear()
    
    # ========================================
    # HANDLER DE CONSOLA (con colores y flush)
    # ========================================
    class FlushingStreamHandler(logging.StreamHandler):
        def emit(self, record):
            try:
                super().emit(record)
                self.flush()  # ✅ Flush inmediato
            except Exception:
                self.handleError(record)
    
    console_handler = FlushingStreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = ColoredFormatter(fmt, datefmt=date_fmt)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # ========================================
    # HANDLER DE ARCHIVO (sin colores)
    # ========================================
    if log_to_file:
        if log_file is None:
            # Crear carpeta logs/ si no existe
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            log_file = str(log_dir / f"app_{datetime.now().strftime('%Y%m%d')}.log")
        
        try:
            file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')
            file_handler.setLevel(logging.DEBUG)  # Archivo captura TODO
            file_formatter = logging.Formatter(fmt, datefmt=date_fmt)
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
            
            # Mensaje inicial técnico en el archivo
            root_logger.info(f"📝 Logs guardándose en: {log_file}")
        except Exception as e:
            # Usar print directo por si falla el logger
            print(f"⚠️ No se pudo crear archivo de log: {e}")
    
    # ========================================
    # SILENCIAR LIBRERÍAS RUIDOSAS
    # ========================================
    # Reducimos ruido, pero dejamos WARNINGS de Telegram por si es error de conexión
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("googleapiclient").setLevel(logging.WARNING)
    
    # ========================================
    # REGISTRO DE IDENTIDAD (INFO EXTRA)
    # ========================================
    # Esto escribe en el log quién está iniciando el proceso
    try:
        hostname = socket.gethostname()
        pid = os.getpid()
        root_logger.info("="*80)
        root_logger.info(f"🚀 LOGGING INICIALIZADO | Host: {hostname} | PID: {pid}")
        root_logger.info("="*80)
    except Exception:
        root_logger.info("🚀 LOGGING INICIALIZADO")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_exception(logger: logging.Logger, exc: Exception, context: str = "") -> None:
    import traceback
    tb_str = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    
    header = f"❌ ERROR en {context}:" if context else "❌ ERROR:"
    logger.error(header)
    logger.error(f"   Tipo: {type(exc).__name__}")
    logger.error(f"   Mensaje: {str(exc)}")
    logger.error(f"   Traceback completo:\n{tb_str}")


if __name__ == "__main__":
    setup_logging(log_level=logging.DEBUG, detailed=True)
    logger = get_logger(__name__)
    logger.info("Prueba de log con PID visible. Verifica el número después de la fecha.")