import os
import shutil
import re
from datetime import datetime

# ==========================================
# CONFIGURACIÓN
# ==========================================
# Busca recursivamente los archivos objetivo
def find_file(filename, search_path="."):
    for root, dirs, files in os.walk(search_path):
        if filename in files:
            return os.path.join(root, filename)
    return None

TARGET_SHEETS = find_file("sheets_manager.py")
TARGET_HOST = find_file("host_bot.py")

PATCH_FILES = {
    "sheets": "PATCH_sheets_manager_roles.py",
    "part1": "PATCH_host_bot_part1_cache_hibernation.py",
    "part2": "PATCH_host_bot_part2_setall_rol.py",
    "part3": "PATCH_host_bot_part3_commands_menu.py",
    "part4": "PATCH_host_bot_part4_handlers_filtro.py"
}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def read_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

def write_file(path, content):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)

def backup_file(path):
    if not path or not os.path.exists(path):
        return False
    backup_path = path + ".bak"
    shutil.copy2(path, backup_path)
    log(f"📦 Backup creado: {backup_path}")
    return True

# ==========================================
# LÓGICA DE PARCHEO: SHEETS MANAGER
# ==========================================
def patch_sheets_manager():
    if not TARGET_SHEETS:
        log("❌ No se encontró sheets_manager.py. Saltando.")
        return

    log(f"🔧 Parchando {TARGET_SHEETS}...")
    content = read_file(TARGET_SHEETS)
    patch_content = read_file(PATCH_FILES["sheets"])

    # 1. Inyectar Estructuras en _check_structure_safe
    if '"GROUP_ROLES"' not in content:
        log("   > Inyectando definiciones de columnas (GROUP_ROLES, KNOWN_USERS)...")
        # Buscamos el diccionario de structures
        structures_marker = '"structures": {'
        new_structures = """
            "GROUP_ROLES": ["CHAT_ID", "USER_ID", "USERNAME", "FULL_NAME", "ROL", "ASIGNADO_POR", "FECHA"],
            "KNOWN_USERS": ["CHAT_ID", "USER_ID", "USERNAME", "FULL_NAME", "FIRST_SEEN", "LAST_SEEN"],"""
        
        if structures_marker in content:
            content = content.replace(structures_marker, structures_marker + new_structures)
        else:
            log("   ⚠️ No se encontró el marcador 'structures': {. Verifica manualmente.")

    # 2. Agregar métodos nuevos al final de la clase (antes de que termine el archivo)
    if "def _create_group_roles_sheet" not in content:
        log("   > Agregando nuevos métodos de roles al final del archivo...")
        # Limpiamos el patch para quitar los imports que ya podrían estar o no ser necesarios duplicar
        # Pero aseguramos tener ZoneInfo
        if "from zoneinfo import ZoneInfo" not in content:
            content = "from zoneinfo import ZoneInfo\nAR_TZ = ZoneInfo('America/Argentina/Buenos_Aires')\n" + content
        
        # Agregamos el contenido del patch al final
        # Eliminamos la cabecera del patch si existe (las primeras lineas con comentarios)
        patch_lines = patch_content.splitlines()
        clean_patch = "\n".join([line for line in patch_lines if not line.startswith("# -*-") and "PARCHE PARA" not in line])
        
        content += "\n\n" + clean_patch

    write_file(TARGET_SHEETS, content)
    log("✅ sheets_manager.py actualizado correctamente.")

# ==========================================
# LÓGICA DE PARCHEO: HOST BOT
# ==========================================
def patch_host_bot():
    if not TARGET_HOST:
        log("❌ No se encontró host_bot.py. Saltando.")
        return

    log(f"🔧 Parchando {TARGET_HOST}...")
    content = read_file(TARGET_HOST)
    
    # Leer parches
    p1 = read_file(PATCH_FILES["part1"])
    p2 = read_file(PATCH_FILES["part2"])
    p3 = read_file(PATCH_FILES["part3"])
    p4 = read_file(PATCH_FILES["part4"])

    # 1. Imports y Variables Globales (Parte 1)
    if "role_cache" not in content:
        log("   > Inyectando Cache y Hibernación (Parte 1)...")
        # Inyectar después de los imports existentes (buscamos un punto seguro)
        if "logger =" in content:
            parts = content.split("logger =", 1)
            # Limpiar cabeceras del patch
            p1_clean = "\n".join([l for l in p1.splitlines() if not l.startswith("# -*-") and "PARCHE PARA" not in l])
            content = parts[0] + p1_clean + "\n\nlogger =" + parts[1]
        else:
            content = p1 + "\n" + content # Fallback, poner al principio

    # 2. Nuevos Comandos (Parte 2 y 3)
    if "async def cmd_setall_rol" not in content:
        log("   > Inyectando Comandos /setall_rol, /mirol, /help (Partes 2 y 3)...")
        # Insertar antes de la función main()
        if "def main()" in content:
            parts = content.split("def main()", 1)
            
            p2_clean = "\n".join([l for l in p2.splitlines() if not l.startswith("# -*-") and "PARCHE PARA" not in l])
            p3_clean = "\n".join([l for l in p3.splitlines() if not l.startswith("# -*-") and "PARCHE PARA" not in l])
            
            content = parts[0] + "\n" + p2_clean + "\n" + p3_clean + "\n\ndef main()" + parts[1]

    # 3. Reemplazo Crítico: handle_photo (Parte 4)
    if "register_user_interaction" not in content: # Señal de que no está el patch 4
        log("   > Reemplazando handle_photo por versión con filtro de seguridad...")
        
        # Usamos Regex para encontrar el bloque entero de handle_photo antiguo
        # Busca desde 'async def handle_photo' hasta el siguiente 'async def' o 'def'
        pattern = r"async def handle_photo\(.*?\):.*?(\n\S)"
        
        # Extrar la función nueva del patch
        match_new = re.search(r"(async def handle_photo\(.+?)(?=\n# ===|$)", p4, re.DOTALL)
        if match_new:
            new_handle_photo = match_new.group(1)
            
            # Reemplazar en el contenido original
            # Nota: Esto es arriesgado con regex simple, vamos a intentar comentar la vieja y agregar la nueva
            if "async def handle_photo" in content:
                content = content.replace("async def handle_photo", "# OLD_handle_photo_REPLACED\n# async def handle_photo_OLD")
                
                # Inyectar la nueva función antes de main o en algún lugar seguro
                # Lo ponemos junto a los otros comandos agregados
                content = content.replace("def main()", new_handle_photo + "\n\ndef main()")
        else:
            log("   ⚠️ No se pudo extraer handle_photo del parche 4.")

    # 4. Modificar Jobs para Hibernación
    log("   > Inyectando check de hibernación en Jobs...")
    check_code = "\n    # 🌙 VERIFICAR HIBERNACIÓN\n    if 'bot_hibernating' in globals() and bot_hibernating:\n        return\n"
    
    if "async def sync_telegram_job" in content and "bot_hibernating" not in content.split("async def sync_telegram_job")[1][:200]:
        content = content.replace("async def sync_telegram_job(context: ContextTypes.DEFAULT_TYPE) -> None:", 
                                  "async def sync_telegram_job(context: ContextTypes.DEFAULT_TYPE) -> None:" + check_code)

    if "async def procesar_cola_imagenes_pendientes" in content and "bot_hibernating" not in content.split("async def procesar_cola_imagenes_pendientes")[1][:200]:
        content = content.replace("async def procesar_cola_imagenes_pendientes(context: ContextTypes.DEFAULT_TYPE) -> None:",
                                  "async def procesar_cola_imagenes_pendientes(context: ContextTypes.DEFAULT_TYPE) -> None:" + check_code)

    # 5. Registrar Handlers en Main
    log("   > Registrando nuevos comandos en main()...")
    handlers_code = """
    # --- NUEVOS HANDLERS ROLES ---
    application.add_handler(CommandHandler("setall_rol", cmd_setall_rol))
    application.add_handler(CommandHandler("mirol", cmd_mirol))
    application.add_handler(CommandHandler("ranking", cmd_ranking))
    # -----------------------------
    """
    if "application.add_handler(CommandHandler(\"start\", cmd_start))" in content:
        content = content.replace('application.add_handler(CommandHandler("start", cmd_start))', 
                                  'application.add_handler(CommandHandler("start", cmd_start))' + handlers_code)

    # 6. Inits de Hibernación
    if "post_init_extensions(application)" not in content:
        log("   > Configurando inicio de extensiones en post_init...")
        if "async def post_init(application: Application) -> None:" in content:
             content = content.replace("await setup_bot_commands(application)", 
                                       "await setup_bot_commands(application)\n    await post_init_extensions(application)")

    write_file(TARGET_HOST, content)
    log("✅ host_bot.py actualizado correctamente.")

# ==========================================
# MAIN
# ==========================================
def main():
    log("🚀 Iniciando aplicación automática de parches...")
    
    # Verificar existencia de parches
    missing_patches = [p for p in PATCH_FILES.values() if not os.path.exists(p)]
    if missing_patches:
        log(f"❌ Faltan archivos de parches: {missing_patches}")
        return

    # Backup
    backup_file(TARGET_SHEETS)
    backup_file(TARGET_HOST)

    # Aplicar
    patch_sheets_manager()
    patch_host_bot()

    log("✨ PROCESO COMPLETADO ✨")
    log("Por favor verifica los archivos .py antes de ejecutar el bot.")

if __name__ == "__main__":
    main()