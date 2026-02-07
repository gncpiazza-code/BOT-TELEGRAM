# -*- coding: utf-8 -*-
# file: host_gui.py
import flet as ft
import subprocess
import sys
import os
import threading
import time
from pathlib import Path
from datetime import datetime, timedelta
import platform

# ==========================================
# CONFIGURACIÓN
USE_MOCK_BOT = False  
# ==========================================

def play_sound(sound_type: str = "info"):
    """Reproduce sonidos nativos del sistema."""
    try:
        if platform.system() == "Windows":
            import winsound
            if sound_type == "error":
                winsound.MessageBeep(winsound.MB_ICONHAND)
            elif sound_type == "warning":
                winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
            elif sound_type == "success":
                winsound.MessageBeep(winsound.MB_OK)
            else:
                winsound.MessageBeep(winsound.MB_ICONASTERISK)
        else:
            print("\a")
    except:
        pass

def _setup_paths():
    script_dir = Path(__file__).resolve().parent
    search_paths = [
        script_dir,
        script_dir / "src",
        script_dir / "CONFIG_GLOBAL",
        script_dir.parent,
        script_dir.parent / "src",
        script_dir.parent / "CONFIG_GLOBAL",
        script_dir.parent / "internal",
        script_dir.parent / "internal" / "src",
        script_dir.parent / "internal" / "CONFIG_GLOBAL",
    ]
    for path in search_paths:
        if path.exists() and str(path) not in sys.path:
            sys.path.insert(0, str(path))

_setup_paths()

try:
    from logger_config import get_logger
    logger = get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger("HostGUI")

# IMPORTS PARA SISTEMA DE HOST
try:
    from config_manager import ConfigManager
    from sheets_manager import SheetsManager
    from host_lock import HostLock
    
    cfg = ConfigManager()
    sheets = SheetsManager()
    host_lock = HostLock(sheets)
    HOST_SYSTEM_ENABLED = True
except ImportError as e:
    logger.warning(f"Sistema de host deshabilitado: {e}")
    HOST_SYSTEM_ENABLED = False
    host_lock = None

bot_process = None
auto_restart_enabled = True
last_queue_state = None

def _windows_creationflags() -> int:
    return subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0

def show_msg(page: ft.Page, message: str, color: str = "red"):
    try:
        snack = ft.SnackBar(content=ft.Text(message), bgcolor=color)
        page.snack_bar = snack
        snack.open = True
        page.update()
    except Exception:
        print(f"[GUI ERROR] {message}")

def _spawn_and_verify(page: ft.Page, args: list, cwd: str = None, env: dict = None, close_current_on_success: bool = True):
    try:
        proc = subprocess.Popen(args, cwd=cwd, env=env, creationflags=_windows_creationflags())
    except Exception as ex:
        show_msg(page, f"Error menu: {ex}", "red")
        return
    time.sleep(0.4)
    if proc.poll() is not None:
        show_msg(page, "Error al iniciar proceso externo.", "red")
        return
    if close_current_on_success:
        page.window_close()

def main(page: ft.Page):
    page.title = "Panel BOT HOST" + (" (TEST)" if USE_MOCK_BOT else "")
    try:
        page.window_width = 1000
        page.window_height = 750
    except:
        pass
        
    page.bgcolor = "#0d1117"
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 20

    # ========================================
    # VARIABLES GLOBALES
    # ========================================
    status_indicator = ft.Container(width=10, height=10, border_radius=10, bgcolor="#f85149")
    status_text = ft.Text("DETENIDO", size=12, weight="bold", color="#f85149")
    
    current_tab_index = [0]
    
    logs_view = ft.ListView(expand=True, spacing=2, auto_scroll=True, padding=10)
    
    host_current_text = ft.Text("Cargando...", size=11, color="#6e7681")
    host_queue_size_text = ft.Text("", size=10, color="#6e7681")
    
    transfer_progress_bar = ft.ProgressBar(width=400, value=0, color="#58a6ff", bgcolor="#21262d", visible=False)
    transfer_time_text = ft.Text("Sin traspaso programado", size=11, color="#6e7681")
    transfer_cancel_btn = ft.TextButton("Cancelar Traspaso", visible=False)
    
    transfer_minutes_dropdown = ft.Dropdown(
        width=120,
        options=[
            ft.dropdown.Option("1", "1 min"),
            ft.dropdown.Option("5", "5 min"),
            ft.dropdown.Option("10", "10 min"),
            ft.dropdown.Option("15", "15 min"),
            ft.dropdown.Option("30", "30 min"),
            ft.dropdown.Option("60", "1 hora"),
        ],
        value="5",
        text_size=12,
        height=40
    )
    
    queue_table_rows = ft.Column(spacing=5, scroll=ft.ScrollMode.AUTO)
    
    confirm_dialog = ft.AlertDialog(
        modal=True,
        title=ft.Text("Confirmar accion"),
        content=ft.Text(""),
        actions=[],
    )
    
    page.dialog = confirm_dialog
    
    pending_action = {"type": None, "data": None}

    # ========================================
    # LOGGING
    # ========================================
    def add_log(message, color="#c9d1d9"):
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            logs_view.controls.append(
                ft.Text(f"[{timestamp}] {message}", color=color, size=11)
            )
            if len(logs_view.controls) > 200:
                logs_view.controls.pop(0)
            page.update()
        except:
            pass

    # ========================================
    # HOST LOCK FUNCTIONS
    # ========================================
    def update_host_info():
        """Actualiza info del host desde Sheets con manejo robusto de errores."""
        if not HOST_SYSTEM_ENABLED or not host_lock:
            host_current_text.value = "Sistema de host deshabilitado"
            host_current_text.color = "#6e7681"
            page.update()
            return
        
        try:
            info = host_lock.get_host_info()
            
            # Actualizar host actual
            try:
                if info.get("current_host"):
                    is_me = info.get("is_host", False)
                    
                    if is_me:
                        host_current_text.value = f"TU ERES EL HOST\n{info.get('my_identity', 'Unknown')}"
                        host_current_text.color = "#3fb950"
                    else:
                        current = info.get('current_host', 'Unknown')
                        host_current_text.value = f"Host activo:\n{current}"
                        host_current_text.color = "#f0883e"
                else:
                    host_current_text.value = "Sin host activo\n(puedes iniciar el bot)"
                    host_current_text.color = "#6e7681"
            except Exception as e:
                logger.error(f"Error actualizando texto de host: {e}")
                host_current_text.value = "Error en datos de host"
                host_current_text.color = "#f85149"
            
            # Actualizar cola
            try:
                queue = info.get("queue", [])
                host_queue_size_text.value = f"Cola: {len(queue)} bot(s) esperando"
                update_queue_table(queue)
            except Exception as e:
                logger.error(f"Error actualizando cola: {e}")
                host_queue_size_text.value = "Error en cola"
            
            # Actualizar estado de traspaso
            try:
                if host_lock.is_host:
                    update_transfer_status()
                else:
                    transfer_progress_bar.visible = False
                    transfer_time_text.value = "Sin traspaso programado"
                    transfer_cancel_btn.visible = False
            except Exception as e:
                logger.error(f"Error actualizando transfer: {e}")
            
            page.update()
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error general en update_host_info: {error_msg}")
            host_current_text.value = f"Error: {error_msg[:40]}..."
            host_current_text.color = "#f85149"
            add_log(f"ERROR: {error_msg}", "#f85149")
            page.update()

    def update_transfer_status():
        """Actualiza el estado del traspaso programado."""
        if not HOST_SYSTEM_ENABLED or not host_lock or not host_lock.is_host:
            transfer_progress_bar.visible = False
            transfer_time_text.value = "Sin traspaso programado"
            transfer_cancel_btn.visible = False
            return
        
        try:
            status = host_lock.get_transfer_status()
            
            if status.get("scheduled"):
                remaining = status.get("remaining_seconds", 0)
                target = status.get("target", "Siguiente en cola")
                
                if remaining > 0:
                    minutes = int(remaining // 60)
                    seconds = int(remaining % 60)
                    transfer_time_text.value = f"Traspaso en: {minutes:02d}:{seconds:02d}\nDestino: {target}"
                    transfer_time_text.color = "#f0883e"
                    
                    transfer_at_str = status.get("transfer_at", "")
                    if transfer_at_str:
                        try:
                            from datetime import datetime as dt
                            try:
                                from zoneinfo import ZoneInfo
                                AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
                            except:
                                from datetime import timezone
                                AR_TZ = timezone(timedelta(hours=-3))
                            
                            transfer_time = dt.strptime(transfer_at_str, "%d/%m/%Y %H:%M:%S")
                            transfer_time = transfer_time.replace(tzinfo=AR_TZ)
                            now = dt.now(AR_TZ)
                            
                            total_seconds = (transfer_time - now).total_seconds() + remaining
                            progress = 1 - (remaining / total_seconds) if total_seconds > 0 else 0
                            transfer_progress_bar.value = max(0, min(1, progress))
                        except Exception as e:
                            logger.debug(f"Error calculando progreso: {e}")
                            transfer_progress_bar.value = 0
                    
                    transfer_progress_bar.visible = True
                    transfer_cancel_btn.visible = True
                else:
                    transfer_time_text.value = "Ejecutando traspaso..."
                    transfer_time_text.color = "#58a6ff"
                    transfer_progress_bar.value = 1
                    transfer_progress_bar.visible = True
                    transfer_cancel_btn.visible = False
            else:
                transfer_progress_bar.visible = False
                transfer_time_text.value = "Sin traspaso programado"
                transfer_time_text.color = "#6e7681"
                transfer_cancel_btn.visible = False
        except Exception as e:
            logger.error(f"Error en update_transfer_status: {e}")

    # ========================================
    # FACTORY FUNCTIONS
    # ========================================
    def make_move_up_handler(identity_str, position_int):
        def handler(e):
            if position_int <= 1:
                return
            new_pos = position_int - 1
            add_log(f"Moviendo {identity_str} a pos {new_pos}", "#58a6ff")
            
            result = host_lock.move_in_queue(identity_str, new_pos)
            
            if result.get("success"):
                play_sound("success")
                add_log(f"OK: {result.get('message')}", "#3fb950")
                update_host_info()
            else:
                play_sound("error")
                show_msg(page, result.get("message", "Error"), "red")
        return handler

    def make_move_down_handler(identity_str, position_int):
        def handler(e):
            new_pos = position_int + 1
            add_log(f"Moviendo {identity_str} a pos {new_pos}", "#58a6ff")
            
            result = host_lock.move_in_queue(identity_str, new_pos)
            
            if result.get("success"):
                play_sound("success")
                add_log(f"OK: {result.get('message')}", "#3fb950")
                update_host_info()
            else:
                play_sound("error")
                show_msg(page, result.get("message", "Error"), "red")
        return handler

    def make_remove_confirm_handler(identity_str):
        def handler(e):
            pending_action["type"] = "remove"
            pending_action["data"] = identity_str
            confirm_dialog.title = ft.Text("Confirmar eliminacion")
            confirm_dialog.content = ft.Text(f"Eliminar de la cola?\n\n{identity_str}")
            confirm_dialog.actions = [
                ft.TextButton("Cancelar", on_click=close_dialog),
                ft.TextButton("Eliminar", on_click=execute_pending_action),
            ]
            confirm_dialog.open = True
            page.update()
        return handler

    def make_transfer_confirm_handler(identity_str):
        def handler(e):
            pending_action["type"] = "transfer"
            pending_action["data"] = identity_str
            confirm_dialog.title = ft.Text("Confirmar traspaso")
            confirm_dialog.content = ft.Text(f"Pasar el host AHORA?\n\n{identity_str}\n\nEl bot actual se detendra.")
            confirm_dialog.actions = [
                ft.TextButton("Cancelar", on_click=close_dialog),
                ft.TextButton("Pasar Host", on_click=execute_pending_action),
            ]
            confirm_dialog.open = True
            page.update()
        return handler

    def execute_pending_action(e):
        """Ejecuta la accion pendiente del dialog."""
        action_type = pending_action["type"]
        action_data = pending_action["data"]
        
        close_dialog()
        
        if action_type == "remove":
            add_log(f"Eliminando {action_data}...", "#f0883e")
            result = host_lock.remove_from_queue(action_data)
            
            if result.get("success"):
                play_sound("success")
                add_log(f"OK: {result.get('message')}", "#3fb950")
                update_host_info()
            else:
                play_sound("error")
                show_msg(page, result.get("message", "Error"), "red")
        
        elif action_type == "transfer":
            add_log(f"Traspasando host a {action_data}...", "#58a6ff")
            result = host_lock.transfer_to(action_data)
            
            if result.get("success"):
                play_sound("success")
                add_log(f"OK: {result.get('message')}", "#3fb950")
                force_kill_bot()
                time.sleep(1)
                update_host_info()
            else:
                play_sound("error")
                show_msg(page, result.get("message", "Error"), "red")
        
        pending_action["type"] = None
        pending_action["data"] = None

    def close_dialog(e=None):
        """Cierra el dialog."""
        confirm_dialog.open = False
        page.update()

    def update_queue_table(queue: list):
        """Actualiza la tabla de cola."""
        global last_queue_state
        
        queue_table_rows.controls.clear()
        
        if not queue:
            queue_table_rows.controls.append(
                ft.Container(
                    content=ft.Text("Cola vacia", size=12, color="#6e7681", text_align=ft.TextAlign.CENTER),
                    padding=20,
                    alignment=ft.alignment.center
                )
            )
            last_queue_state = []
            return
        
        current_identities = [q.get("identity", "") for q in queue]
        if last_queue_state is not None and last_queue_state != current_identities:
            play_sound("info")
            add_log("Cola actualizada", "#58a6ff")
        last_queue_state = current_identities
        
        header = ft.Container(
            content=ft.Row([
                ft.Text("#", size=11, weight="bold", color="#8b949e", width=30),
                ft.Text("IDENTITY", size=11, weight="bold", color="#8b949e", expand=True),
                ft.Text("ESTADO", size=11, weight="bold", color="#8b949e", width=100),
                ft.Text("ACCIONES", size=11, weight="bold", color="#8b949e", width=200),
            ]),
            bgcolor="#161b22",
            padding=10,
            border_radius=5,
        )
        queue_table_rows.controls.append(header)
        
        for q in queue:
            identity = q.get("identity", "Unknown")
            status = q.get("status", "WAITING")
            is_offline = q.get("is_offline", False)
            position = q.get("position", 0)
            
            if is_offline:
                status_icon = "!"
                status_text_val = "Offline"
                status_color = "#f85149"
            elif status == "READY":
                status_icon = "+"
                status_text_val = "Listo"
                status_color = "#3fb950"
            else:
                status_icon = "-"
                status_text_val = "Esperando"
                status_color = "#6e7681"
            
            action_buttons = []
            if host_lock and host_lock.is_host:
                btn_up = ft.IconButton(
                    icon=ft.icons.ARROW_UPWARD,
                    icon_size=16,
                    tooltip="Subir",
                    on_click=make_move_up_handler(identity, position),
                    disabled=(position == 1)
                )
                
                queue_length = len(queue)
                btn_down = ft.IconButton(
                    icon=ft.icons.ARROW_DOWNWARD,
                    icon_size=16,
                    tooltip="Bajar",
                    on_click=make_move_down_handler(identity, position),
                    disabled=(position == queue_length)
                )
                
                btn_remove = ft.IconButton(
                    icon=ft.icons.DELETE,
                    icon_size=16,
                    tooltip="Eliminar",
                    icon_color="#f85149",
                    on_click=make_remove_confirm_handler(identity)
                )
                
                btn_transfer = ft.IconButton(
                    icon=ft.icons.BOLT,
                    icon_size=16,
                    tooltip="Pasar host AHORA",
                    icon_color="#58a6ff",
                    on_click=make_transfer_confirm_handler(identity)
                )
                
                action_buttons = [btn_up, btn_down, btn_remove, btn_transfer]
            
            row = ft.Container(
                content=ft.Row([
                    ft.Text(str(position), size=11, color="#c9d1d9", width=30),
                    ft.Text(identity, size=11, color="#c9d1d9", expand=True),
                    ft.Row([
                        ft.Text(status_icon, size=14),
                        ft.Text(status_text_val, size=11, color=status_color)
                    ], spacing=5, width=100),
                    ft.Row(action_buttons, spacing=5, width=200) if action_buttons else ft.Container(width=200),
                ]),
                bgcolor="#0d1117" if position % 2 == 0 else "#161b22",
                padding=8,
                border_radius=3,
            )
            
            queue_table_rows.controls.append(row)

    def cleanup_dead_bots(e):
        """Limpia bots muertos."""
        add_log("Limpiando bots offline...", "#58a6ff")
        
        result = host_lock.cleanup_dead_bots()
        
        if result.get("success"):
            removed = result.get("removed", 0)
            if removed > 0:
                play_sound("success")
                add_log(f"OK: {removed} bot(s) eliminados", "#3fb950")
            else:
                add_log("OK: No hay bots offline", "#6e7681")
            update_host_info()
        else:
            play_sound("error")
            show_msg(page, result.get("message", "Error"), "red")

    # ========================================
    # TRASPASO
    # ========================================
    def schedule_transfer(e):
        """Programa un traspaso."""
        if not host_lock or not host_lock.is_host:
            show_msg(page, "Solo el host puede programar traspasos", "orange")
            return
        
        minutes = int(transfer_minutes_dropdown.value)
        add_log(f"Programando traspaso en {minutes} min...", "#58a6ff")
        
        result = host_lock.schedule_transfer(minutes)
        
        if result.get("success"):
            play_sound("success")
            add_log(f"OK: {result.get('message')}", "#3fb950")
            update_host_info()
        else:
            play_sound("error")
            show_msg(page, result.get("message", "Error"), "red")

    def cancel_transfer(e):
        """Cancela traspaso."""
        if not host_lock or not host_lock.is_host:
            return
        
        add_log("Cancelando traspaso...", "#f0883e")
        
        result = host_lock.cancel_scheduled_transfer()
        
        if result.get("success"):
            play_sound("info")
            add_log(f"OK: {result.get('message')}", "#6e7681")
            update_host_info()
        else:
            play_sound("error")
            show_msg(page, result.get("message", "Error"), "red")
    
    transfer_cancel_btn.on_click = cancel_transfer

    # ========================================
    # BOT CONTROL
    # ========================================
    def force_kill_bot():
        global bot_process
        if not bot_process: 
            return
        try:
            pid = bot_process.pid
            add_log(f"Terminando proceso PID {pid}...", "#f85149")
            if sys.platform == "win32":
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], creationflags=_windows_creationflags())
            else:
                bot_process.kill()
        except:
            pass
        bot_process = None

    def monitor_output(proc):
        for line in iter(proc.stdout.readline, b""):
            try:
                msg = line.decode("utf-8", errors="replace").strip()
            except:
                msg = str(line)
            if msg:
                c = "#c9d1d9"
                if "ERROR" in msg or "CRITICAL" in msg: 
                    c = "#f85149"
                elif "WARNING" in msg: 
                    c = "#f0883e"
                elif "INFO" in msg: 
                    c = "#3fb950"
                elif "HOST" in msg.upper() or "LOCK" in msg.upper(): 
                    c = "#58a6ff"
                add_log(msg, c)
        handle_process_end()

    def handle_process_end():
        global bot_process, auto_restart_enabled
        try:
            status_indicator.bgcolor = "#f85149"
            status_text.value = "DETENIDO"
            status_text.color = "#f85149"
            btn_start.disabled = False
            btn_stop.disabled = True
            add_log("Bot finalizado", "#f0883e")
            play_sound("warning")
            
            update_host_info()
            page.update()
        except:
            pass
        bot_process = None
        
        if auto_restart_enabled:
            add_log("Auto-restart habilitado, reiniciando en 5s...", "#58a6ff")
            threading.Thread(target=restart_bot_delayed, daemon=True).start()

    def restart_bot_delayed():
        global auto_restart_enabled
        time.sleep(5)
        if auto_restart_enabled:
            add_log("Reiniciando bot automaticamente...", "#3fb950")
            start_bot(None)

    def toggle_auto_restart(e):
        global auto_restart_enabled
        auto_restart_enabled = e.control.value
        status = "HABILITADO" if auto_restart_enabled else "DESHABILITADO"
        add_log(f"Auto-restart: {status}", "#58a6ff")

    def start_bot(e):
        global bot_process
        if bot_process: 
            return
        
        add_log("Iniciando bot...", "#58a6ff")
        play_sound("info")
        
        if HOST_SYSTEM_ENABLED and host_lock:
            try:
                info = host_lock.get_host_info()
                if info.get("current_host") and not info.get("is_host"):
                    add_log(f"Otro host activo: {info['current_host']}", "#f0883e")
                    add_log("El bot se unira a la cola automaticamente", "#f0883e")
            except Exception as e:
                add_log(f"Error verificando host: {e}", "#f85149")
        
        is_frozen = getattr(sys, "frozen", False)
        base_dir = os.path.dirname(os.path.abspath(sys.executable if is_frozen else __file__))
        env_vars = os.environ.copy()
        env_vars["PYTHONIOENCODING"] = "utf-8"

        script_name = "mock_bot.py" if USE_MOCK_BOT else "host_bot.py"
        exe_name = "host_bot.exe"
        target = None
        args = []

        if is_frozen and not USE_MOCK_BOT:
             t = os.path.join(base_dir, exe_name)
             if os.path.exists(t):
                 target = t
                 args = [target]
        
        if not target:
            target = os.path.join(base_dir, script_name)
            args = [sys.executable, "-u", target]

        if not os.path.exists(target):
            add_log(f"Falta archivo: {script_name}", "#f85149")
            play_sound("error")
            return

        try:
            bot_process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, creationflags=_windows_creationflags(), env=env_vars)
            status_indicator.bgcolor = "#3fb950"
            status_text.value = "EJECUTANDO"
            status_text.color = "#3fb950"
            btn_start.disabled = True
            btn_stop.disabled = False
            page.update()
            
            time.sleep(2)
            update_host_info()
            
            threading.Thread(target=monitor_output, args=(bot_process,), daemon=True).start()
            play_sound("success")
        except Exception as ex:
            add_log(f"Error al iniciar: {ex}", "#f85149")
            play_sound("error")

    def stop_bot(e):
        global bot_process
        if bot_process:
            add_log("Deteniendo bot...", "#f0883e")
            force_kill_bot()
            time.sleep(1)
            update_host_info()
            play_sound("info")

    def refresh_host_info(e):
        add_log("Actualizando info de host...", "#58a6ff")
        update_host_info()

    def open_launcher(e):
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            dev_launcher = os.path.join(base_dir, "main_launcher.py")
            if not os.path.exists(dev_launcher):
                 dev_launcher = os.path.join(base_dir, "..", "main_launcher.py")
            
            clean_env = os.environ.copy()
            clean_env.pop("PYTHONPATH", None)
            _spawn_and_verify(page, [sys.executable, dev_launcher], cwd=os.path.dirname(dev_launcher), env=clean_env, close_current_on_success=False)
        except Exception as ex:
            add_log(f"Error abriendo menu: {ex}", "#f85149")

    # ========================================
    # BOTONES
    # ========================================
    btn_start = ft.TextButton(
        "INICIAR",
        on_click=start_bot,
        style=ft.ButtonStyle(
            bgcolor={"": "#238636"},
            color={"": "white"},
        ),
    )
    
    btn_stop = ft.TextButton(
        "DETENER",
        on_click=stop_bot,
        disabled=True,
        style=ft.ButtonStyle(
            bgcolor={"": "#da3633"},
            color={"": "white"},
        ),
    )
    
    btn_refresh_host = ft.TextButton(
        "Actualizar Info",
        on_click=refresh_host_info,
        visible=HOST_SYSTEM_ENABLED,
        style=ft.ButtonStyle(
            bgcolor={"": "#1f6feb"},
            color={"": "white"},
        ),
    )
    
    btn_launcher = ft.TextButton(
        "MENU",
        on_click=open_launcher,
        style=ft.ButtonStyle(
            bgcolor={"": "#6e40c9"},
            color={"": "white"},
        ),
    )
    
    auto_restart_switch = ft.Switch(
        label="Auto-Restart (/hardreset)",
        value=True,
        active_color="#3fb950",
        on_change=toggle_auto_restart
    )

    # ========================================
    # TABS
    # ========================================
    
    tab_estado = ft.Container(
        content=ft.Column([
            ft.Row([
                status_indicator,
                status_text
            ], spacing=10),
            ft.Divider(height=1, color="#21262d"),
            host_current_text,
            ft.Divider(height=1, color="#21262d"),
            ft.Row([
                btn_start,
                btn_stop,
                btn_refresh_host
            ], spacing=10),
            ft.Divider(height=1, color="#21262d"),
            auto_restart_switch,
        ], spacing=15),
        padding=20,
        bgcolor="#161b22",
        border_radius=8,
    )
    
    tab_traspaso = ft.Container(
        content=ft.Column([
            ft.Text("PROGRAMAR TRASPASO", size=13, weight="bold", color="#58a6ff"),
            ft.Divider(height=1, color="#21262d"),
            
            ft.Container(
                content=ft.Column([
                    transfer_time_text,
                    transfer_progress_bar,
                    ft.Row([
                        transfer_minutes_dropdown,
                        ft.TextButton(
                            "Programar",
                            on_click=schedule_transfer,
                            style=ft.ButtonStyle(
                                bgcolor={"": "#238636"},
                                color={"": "white"},
                            ),
                        ),
                        transfer_cancel_btn,
                    ], spacing=10),
                ], spacing=10),
                padding=15,
                bgcolor="#0d1117",
                border_radius=5,
            ),
            
            ft.Divider(height=10, color="#21262d"),
            
            ft.Row([
                ft.Text("GESTION DE COLA", size=13, weight="bold", color="#58a6ff"),
                host_queue_size_text,
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ft.Divider(height=1, color="#21262d"),
            
            ft.Container(
                content=queue_table_rows,
                padding=10,
                bgcolor="#0d1117",
                border_radius=5,
                height=300,
            ),
            
            ft.Row([
                ft.TextButton(
                    "Refrescar",
                    on_click=refresh_host_info,
                    style=ft.ButtonStyle(
                        bgcolor={"": "#1f6feb"},
                        color={"": "white"},
                    ),
                ),
                ft.TextButton(
                    "Limpiar Muertos",
                    on_click=cleanup_dead_bots,
                    style=ft.ButtonStyle(
                        bgcolor={"": "#f0883e"},
                        color={"": "white"},
                    ),
                ),
            ], spacing=10),
            
        ], spacing=15, scroll=ft.ScrollMode.AUTO),
        padding=20,
        bgcolor="#161b22",
        border_radius=8,
        expand=True,
    )
    
    log_container = ft.Container(
        content=logs_view,
        bgcolor="#0d1117",
        border_radius=8,
        expand=True,
        padding=10,
    )
    
    tab_logs = ft.Container(
        content=ft.Column([
            ft.Text("LOGS DEL SISTEMA", size=13, weight="bold", color="#58a6ff"),
            ft.Divider(height=1, color="#21262d"),
            log_container,
        ], spacing=10),
        padding=20,
        bgcolor="#161b22",
        border_radius=8,
        expand=True,
    )

    tabs_content = ft.Container(expand=True)
    
    def change_tab(e):
        tab_index = int(e.control.data)
        current_tab_index[0] = tab_index
        
        if tab_index == 0:
            tabs_content.content = tab_estado
        elif tab_index == 1:
            tabs_content.content = tab_traspaso
        else:
            tabs_content.content = tab_logs
        
        for btn in tab_buttons:
            if btn.data == str(tab_index):
                btn.style.bgcolor = {"": "#1f6feb"}
            else:
                btn.style.bgcolor = {"": "#21262d"}
        
        page.update()
    
    tab_buttons = [
        ft.TextButton(
            "MI ESTADO",
            data="0",
            on_click=change_tab,
            style=ft.ButtonStyle(
                bgcolor={"": "#1f6feb"},
                color={"": "white"},
            ),
        ),
        ft.TextButton(
            "TRASPASO Y COLA",
            data="1",
            on_click=change_tab,
            style=ft.ButtonStyle(
                bgcolor={"": "#21262d"},
                color={"": "white"},
            ),
        ),
        ft.TextButton(
            "LOGS",
            data="2",
            on_click=change_tab,
            style=ft.ButtonStyle(
                bgcolor={"": "#21262d"},
                color={"": "white"},
            ),
        ),
    ]
    
    tabs_row = ft.Row(tab_buttons, spacing=10)
    tabs_content.content = tab_estado

    header = ft.Container(
        content=ft.Column([
            ft.Row([
                ft.Text("Panel BOT HOST", size=16, weight="bold", color="#58a6ff"),
                btn_launcher,
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ft.Divider(height=1, color="#21262d"),
            tabs_row,
        ], spacing=12),
        padding=16,
        bgcolor="#161b22",
        border_radius=8,
    )

    # ========================================
    # MONITOR THREAD
    # ========================================
    def monitor_host_periodically():
        """Thread que actualiza cada 5s."""
        while True:
            time.sleep(5)
            try:
                if current_tab_index[0] == 1:
                    update_host_info()
            except Exception as e:
                logger.error(f"Error en monitor: {e}")

    def window_event(e):
        if e.data == "close":
            force_kill_bot()
            page.window_destroy()

    page.window_prevent_close = True
    page.on_window_event = window_event

    page.add(
        ft.Column([
            header,
            tabs_content,
        ], spacing=15, expand=True)
    )
    
    if HOST_SYSTEM_ENABLED:
        threading.Thread(target=monitor_host_periodically, daemon=True).start()
        add_log("Monitor iniciado (actualiza cada 5s)", "#58a6ff")
        # Hacer primera carga
        threading.Thread(target=lambda: (time.sleep(1), update_host_info()), daemon=True).start()
    else:
        add_log("Sistema de host deshabilitado", "#f0883e")

if __name__ == "__main__":
    ft.app(target=main)