import os
import re
from datetime import datetime

# ==========================================
# CONFIGURACIÓN
# ==========================================
TARGET_FILE = os.path.join("internal", "host_bot.py")

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def read_file(path):
    if not os.path.exists(path):
        log(f"❌ No se encontró el archivo: {path}")
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

def write_file(path, content):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    log(f"💾 Archivo guardado: {path}")

def fix_host_bot():
    content = read_file(TARGET_FILE)
    if not content:
        return

    log("🔧 Analizando host_bot.py...")

    # 1. CORREGIR ERROR DE IMPORTACIÓN (TYPO)
    # Busca "from data ime" o variaciones y lo corrige
    if "from data ime" in content or "import datatime" in content:
        log("   > 🩹 Corrigiendo error de sintaxis en importación...")
        content = re.sub(r'from\s+data\s*ime\s+import\s+datatime', 'from datetime import datetime', content)
        content = content.replace('import datatime', 'import datetime') # Fallback
    
    # 2. ASEGURAR AR_TZ
    # Si no está definido, lo agregamos después de los imports
    if "AR_TZ =" not in content:
        log("   > 🌍 Definiendo AR_TZ (Zona Horaria Argentina)...")
        if "from zoneinfo import ZoneInfo" not in content:
            content = "from zoneinfo import ZoneInfo\n" + content
        
        # Insertar AR_TZ cerca del inicio
        content = content.replace(
            "from datetime import datetime", 
            "from datetime import datetime\nfrom zoneinfo import ZoneInfo\nAR_TZ = ZoneInfo('America/Argentina/Buenos_Aires')"
        )

    # 3. MODIFICAR TODOS LOS TIMESTAMP A AR_TZ
    # Cambia datetime.now() por datetime.now(AR_TZ)
    # El regex busca datetime.now() que NO tenga argumentos dentro
    log("   > ⏰ Estandarizando timestamps a AR_TZ...")
    content = re.sub(r'datetime\.now\(\)', 'datetime.now(AR_TZ)', content)

    # 4. VERIFICAR SETUP_LOGGING Y FORMATO DE FECHA
    # Busca la configuración básica y fuerza un formato limpio
    if "logging.basicConfig" in content:
        log("   > 📝 Ajustando formato de fecha en logging...")
        # Reemplaza o asegura datefmt
        # Esto busca el bloque de basicConfig y asegura que datefmt esté presente y correcto
        if "datefmt=" not in content:
            content = content.replace(
                "logging.basicConfig(", 
                "logging.basicConfig(datefmt='%Y-%m-%d %H:%M:%S', "
            )
        else:
            # Si ya existe, intentamos reemplazarlo por el estándar
            content = re.sub(r"datefmt=['\"].*?['\"]", "datefmt='%Y-%m-%d %H:%M:%S'", content)

    # Backup rápido antes de escribir
    if os.path.exists(TARGET_FILE):
        os.replace(TARGET_FILE, TARGET_FILE + ".bak_fix")
        log("   > 📦 Backup de seguridad creado (.bak_fix)")

    write_file(TARGET_FILE, content)
    log("✅ ¡Correcciones aplicadas con éxito!")

if __name__ == "__main__":
    log("🚀 Iniciando script de corrección...")
    fix_host_bot()
    log("👋 Listo. Intenta iniciar el bot ahora.")