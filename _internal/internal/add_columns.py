# -*- coding: utf-8 -*-
# file: add_audit_columns.py
# Script para agregar columnas de auditoría a RAW_LOGS
# =========================

import sys
from pathlib import Path

# Setup path
here = Path(__file__).resolve().parent
if str(here) not in sys.path:
    sys.path.insert(0, str(here))

try:
    from sheets_manager import SheetsManager
except ImportError as e:
    print(f"❌ Error importando: {e}")
    print("   Asegúrate de ejecutar desde la carpeta del proyecto")
    sys.exit(1)


def add_audit_columns():
    """
    Agrega columnas de auditoría a RAW_LOGS.
    Columnas agregadas:
    - H: AUDIT_STATUS
    - I: RESERVED_BY
    - J: RESERVED_AT
    - K: EVALUATED_BY
    - L: EVALUATED_AT
    - M: EVALUATION_COMMENT
    """
    
    print("="*60)
    print("🔧 AGREGAR COLUMNAS DE AUDITORÍA A RAW_LOGS")
    print("="*60)
    print()
    
    try:
        # Inicializar
        print("📋 Inicializando sheets manager...")
        
        # El SheetsManager de este proyecto se inicializa sin argumentos
        sheets = SheetsManager()
        
        # Obtener hoja
        print("📋 Accediendo a RAW_LOGS...")
        ws = sheets._get_ws("RAW_LOGS")
        if not ws:
            print("❌ No se pudo acceder a RAW_LOGS")
            print("   Verifica que la hoja existe en tu Google Sheets")
            return False
        
        # Leer datos actuales
        print("📋 Leyendo datos actuales...")
        data = ws.get_all_values()
        if len(data) == 0:
            print("❌ RAW_LOGS está vacía")
            return False
        
        print(f"   ✓ {len(data)} filas encontradas")
        
        # Verificar headers actuales
        headers = data[0]
        print(f"   ✓ Columnas actuales: {len(headers)}")
        
        # Verificar si ya existen (después de IS_FRAUD que está en columna L/12)
        if len(headers) >= 18:  # Hasta columna R (18 columnas total)
            if len(headers) > 12 and headers[12] == "AUDIT_STATUS":
                print()
                print("✅ Las columnas ya existen!")
                print("   No es necesario hacer nada.")
                return True
        
        # Definir nuevas columnas
        new_headers = [
            "AUDIT_STATUS",
            "RESERVED_BY",
            "RESERVED_AT",
            "EVALUATED_BY",
            "EVALUATED_AT",
            "EVALUATION_COMMENT"
        ]
        
        print()
        print("📋 Agregando nuevas columnas:")
        for col in new_headers:
            print(f"   • {col}")
        
        # Agregar headers
        headers.extend(new_headers)
        
        # Actualizar todas las filas
        print()
        print("📋 Actualizando filas...")
        
        for i in range(len(data)):
            if i == 0:
                # Headers
                data[i] = headers
            else:
                # Datos: Agregar PENDING + campos vacíos
                current_len = len(data[i])
                target_len = len(headers)
                
                while len(data[i]) < target_len:
                    if len(data[i]) == 12:  # Primera columna nueva después de IS_FRAUD (AUDIT_STATUS)
                        data[i].append("PENDING")
                    else:
                        data[i].append("")
                
                # Progress
                if i % 100 == 0:
                    print(f"   Procesando fila {i}/{len(data)}...")
        
        # Escribir de vuelta
        print()
        print("📋 Guardando cambios en Google Sheets...")
        ws.clear()
        
        # Calcular rango (A=65, R=82 para 18 columnas)
        max_col = chr(65 + len(headers) - 1)
        range_str = f"A1:{max_col}{len(data)}"
        
        ws.update(range_str, data)
        
        print()
        print("="*60)
        print("✅ ¡COLUMNAS AGREGADAS EXITOSAMENTE!")
        print("="*60)
        print()
        print(f"📊 Resumen:")
        print(f"   • Total filas actualizadas: {len(data)}")
        print(f"   • Columnas agregadas: {len(new_headers)}")
        print(f"   • Fotos marcadas como PENDING: {len(data) - 1}")
        print()
        print("🎯 Próximos pasos:")
        print("   1. Verifica en Google Sheets que las columnas se agregaron")
        print("   2. Ejecuta el visor.py")
        print("   3. ¡Disfruta del nuevo sistema de auditoría!")
        print()
        
        return True
        
    except Exception as e:
        print()
        print("="*60)
        print("❌ ERROR AL AGREGAR COLUMNAS")
        print("="*60)
        print()
        print(f"Error: {e}")
        print()
        print("Posibles causas:")
        print("   • Sin permisos de escritura en Google Sheets")
        print("   • Problema de conexión")
        print("   • Service account sin acceso")
        print()
        return False


if __name__ == "__main__":
    import time
    
    print()
    print("⚠️  IMPORTANTE:")
    print("   Este script modificará tu hoja RAW_LOGS en Google Sheets")
    print("   Se agregarán 6 nuevas columnas")
    print()
    
    response = input("¿Deseas continuar? (s/n): ").lower().strip()
    
    if response != 's' and response != 'si' and response != 'sí':
        print()
        print("❌ Operación cancelada")
        sys.exit(0)
    
    print()
    success = add_audit_columns()
    
    if success:
        print("✨ ¡Todo listo!")
        sys.exit(0)
    else:
        print("💔 Algo salió mal")
        sys.exit(1)