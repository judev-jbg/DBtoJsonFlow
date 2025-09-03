import config.setting as st
import json
from json.decoder import JSONDecodeError
from datetime import datetime, date
from libs.transform import getDataFromDatabase
from libs.database import test_connection
from libs.drive_manager import DriveManager
import os

# Configuración (simplificada ya que no hay archivos de entrada)
# OUTPUT_DIR = st.outputPathDataJSON
OUTPUT_DIR_LOCAL = st.ouputDir
# LAST_FULL_FILE = st.ouputFullData
# LAST_FULL_FILE_DRIVE = st.ouputFullDataDrive
VERSION_FILE = "version.json"
CHANGES_FILE = "changes_articles.json"
LAST_FULL_FILE = "last_full_data.json"

# Configuración Google Drive
DRIVE_FOLDER = st.DRIVE_FOLDERS['ARTICULOS_JSON']

def is_first_execution_of_day():
    """Verifica si es la primera ejecución del día"""
    today = date.today().strftime("%Y-%m-%d")
    
    # Archivo para tracking de ejecuciones diarias
    daily_flag_file = os.path.join(OUTPUT_DIR_LOCAL, f"last_execution_{today}.flag")
    
    if os.path.exists(daily_flag_file):
        return False
    else:
        # Crear flag para el día actual
        os.makedirs(OUTPUT_DIR_LOCAL, exist_ok=True)
        with open(daily_flag_file, 'w') as f:
            f.write(str(datetime.now().timestamp()))
        
        # Limpiar flags de días anteriores
        cleanup_old_flags()
        return True

def cleanup_old_flags():
    """Limpia archivos flag de días anteriores"""
    try:
        if not os.path.exists(OUTPUT_DIR_LOCAL):
            return
        
        for filename in os.listdir(OUTPUT_DIR_LOCAL):
            if filename.startswith("last_execution_") and filename.endswith(".flag"):
                flag_date = filename.replace("last_execution_", "").replace(".flag", "")
                if flag_date != date.today().strftime("%Y-%m-%d"):
                    os.remove(os.path.join(OUTPUT_DIR_LOCAL, filename))
    except Exception as e:
        print(f"Error limpiando flags antiguos: {e}")

# def load_existing_changes():
#     """Carga los cambios existentes del archivo changes_articles.json"""
#     changes_file_path = os.path.join(OUTPUT_DIR, CHANGES_FILE)
    
#     if os.path.exists(changes_file_path):
#         try:
#             with open(changes_file_path, 'r', encoding='utf-8') as f:
#                 content = f.read().strip()
#                 if content:
#                     return json.loads(content)
#         except (JSONDecodeError, Exception) as e:
#             print(f"Error cargando cambios existentes: {e}")
    
#     return []

def load_existing_changes_from_local():
    """Carga los cambios existentes del archivo local de respaldo"""
    local_changes_file = os.path.join(OUTPUT_DIR_LOCAL, CHANGES_FILE)
    
    if os.path.exists(local_changes_file):
        try:
            with open(local_changes_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
        except (JSONDecodeError, Exception) as e:
            print(f"Error cargando cambios locales: {e}")
    
    return []

def save_accumulated_changes(new_changes, is_first_execution):
    """Guarda los cambios acumulándolos o reiniciándolos según corresponda"""
    
    if is_first_execution:
        # Primera ejecución del día: reiniciar archivo
        print("Primera ejecución del día: Reiniciando archivo de cambios")
        accumulated_changes = new_changes
    else:
        # Ejecuciones posteriores: acumular cambios
        print("Ejecución posterior: Acumulando cambios")
        existing_changes = load_existing_changes_from_local()
        
        # Crear diccionario para evitar duplicados por referencia
        changes_dict = {change['referencia']: change for change in existing_changes}
        
        # Agregar/actualizar con nuevos cambios
        for change in new_changes:
            changes_dict[change['referencia']] = change
        
        accumulated_changes = list(changes_dict.values())

    # Guardar respaldo local
    local_changes_file = os.path.join(OUTPUT_DIR_LOCAL, CHANGES_FILE)
    os.makedirs(OUTPUT_DIR_LOCAL, exist_ok=True)
    
    with open(local_changes_file , 'w', encoding='utf-8') as f:
        json.dump(accumulated_changes, f, ensure_ascii=False, indent=2)
    
    print(f"Cambios guardados: {len(accumulated_changes)} productos en total")
    return accumulated_changes

def read_incremental_data_from_db():
    """Lee datos incrementales (última hora) desde la base de datos"""
    if not test_connection():
        print("Error: No se puede conectar a la base de datos")
        return []
    
    # Usar consulta incremental (última hora)
    df, success = getDataFromDatabase(use_incremental=True)
    
    if success and len(df) > 0:
        products = df.to_dict(orient='records')
        
        # Añadir timestamp de actualización
        timestamp = int(datetime.now().timestamp() * 1000)
        for product in products:
            product['ultima_actualizacion'] = timestamp
        
        return products
    
    return []

def generate_full_database():
    """Genera el archivo completo de la base de datos"""
    print("Generando archivo completo de base de datos...")
    
    if not test_connection():
        print("Error: No se puede conectar a la base de datos")
        return False
    
    # Usar consulta completa (todos los productos)
    df, success = getDataFromDatabase(use_incremental=False)
    
    if success and len(df) > 0:
        products = df.to_dict(orient='records')
        
        # Añadir timestamp de actualización
        timestamp = int(datetime.now().timestamp() * 1000)
        for product in products:
            product['ultima_actualizacion'] = timestamp
        
        # Guardar respaldo local
        local_full_file = os.path.join(OUTPUT_DIR_LOCAL, LAST_FULL_FILE)
        os.makedirs(OUTPUT_DIR_LOCAL, exist_ok=True)

        with open(local_full_file, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

        print(f"Base de datos completa guardada: {len(products)} productos")
        return products
    
    return None

def generate_version_info(changes_count):
    """Genera información de versión"""
    timestamp = int(datetime.now().timestamp() * 1000)
    version = f"1.0.{timestamp}"
    
    version_info = {
        "version": version,
        "timestamp": timestamp,
        "changes_count": changes_count,
        "data_source": "sql_server_database",
        "execution_time": datetime.now().isoformat(),
        "sync_method": "google_drive_api"
    }

    # Guardar respaldo local
    local_version_file = os.path.join(OUTPUT_DIR_LOCAL, VERSION_FILE)
    os.makedirs(OUTPUT_DIR_LOCAL, exist_ok=True)
    
    with open(local_version_file, 'w', encoding='utf-8') as f:
        json.dump(version_info, f, ensure_ascii=False, indent=2)
    
    return version_info

def upload_files_to_drive(accumulated_changes, full_database, version_info):
    """Sube todos los archivos a Google Drive usando la API"""
    
    print("\n" + "-" * 50)
    print("SUBIENDO ARCHIVOS A GOOGLE DRIVE")
    print("-" * 50)
    
    # Inicializar DriveManager
    drive_manager = DriveManager()
    
    try:
        # Autenticar con Google Drive
        drive_manager.authenticate()
        
        # Probar conexión
        if not drive_manager.test_connection():
            print("❌ Error conectando con Google Drive")
            return False
        
        upload_results = []
        
        # 1. Subir archivo de cambios incrementales
        if accumulated_changes:
            print(f"\n📤 Subiendo cambios incrementales ({len(accumulated_changes)} productos)...")
            result = drive_manager.upload_json_data(
                data=accumulated_changes,
                filename=CHANGES_FILE,
                folder_path=DRIVE_FOLDER
            )
            upload_results.append(("Cambios incrementales", result))
        
        # 2. Subir base de datos completa
        if full_database:
            print(f"\n📤 Subiendo base completa ({len(full_database)} productos)...")
            result = drive_manager.upload_json_data(
                data=full_database,
                filename=LAST_FULL_FILE,
                folder_path=DRIVE_FOLDER
            )
            upload_results.append(("Base completa", result))
        
        # 3. Subir información de versión
        print(f"\n📤 Subiendo información de versión...")
        result = drive_manager.upload_json_data(
            data=version_info,
            filename=VERSION_FILE,
            folder_path=DRIVE_FOLDER
        )
        upload_results.append(("Versión", result))
        
        # Mostrar resultados
        successful_uploads = sum(1 for _, success in upload_results if success)
        total_uploads = len(upload_results)
        
        print(f"\n📊 RESULTADOS DE SUBIDA:")
        for file_type, success in upload_results:
            status = "✅ EXITOSO" if success else "❌ FALLÓ"
            print(f"  {file_type}: {status}")
        
        print(f"\n🎯 Total: {successful_uploads}/{total_uploads} archivos subidos exitosamente")
        
        if successful_uploads == total_uploads:
            print("🎉 ¡Todos los archivos subidos correctamente a Google Drive!")
            return True
        elif successful_uploads > 0:
            print("⚠️ Algunos archivos subidos, pero hubo errores")
            return True
        else:
            print("❌ No se pudieron subir archivos a Google Drive")
            return False
            
    except Exception as e:
        print(f"❌ Error general subiendo archivos: {e}")
        return False

def main():
    """Función principal del proceso"""
        
    print("=" * 60)
    print("INICIANDO PROCESO DE SINCRONIZACIÓN INCREMENTAL")
    print("=" * 60)
    
    # Verificar si es primera ejecución del día
    is_first_execution = is_first_execution_of_day()
    print(f"¿Primera ejecución del día?: {is_first_execution}")
    
    # Leer datos incrementales desde la base de datos (última hora)
    print("\n" + "-" * 40)
    print("OBTENIENDO CAMBIOS INCREMENTALES DESDE SQL SERVER")
    print("-" * 40)
    
    incremental_data = read_incremental_data_from_db()
    
    if len(incremental_data) > 0:
        print(f"\nCambios incrementales obtenidos: {len(incremental_data)} registros")
        
        # Guardar cambios acumulados
        print("\n" + "-" * 40)
        print("PROCESANDO CAMBIOS ACUMULADOS")
        print("-" * 40)
        
        accumulated_changes = save_accumulated_changes(incremental_data, is_first_execution)
        
        # Generar información de versión
        version_info = generate_version_info(len(accumulated_changes))
        
        # Generar base de datos completa (siempre cuando hay cambios)
        print("\n" + "-" * 40)
        print("ACTUALIZANDO BASE DE DATOS COMPLETA")
        print("-" * 40)

        full_database = generate_full_database()
        
        if full_database:
            print("✅ Base de datos completa actualizada exitosamente")
            
            # Subir archivos a Google Drive
            drive_upload_success = upload_files_to_drive(
                accumulated_changes, 
                full_database, 
                version_info
            )
            
            if drive_upload_success:
                print(f"\n🎉 ¡PROCESO COMPLETADO EXITOSAMENTE!")
                print(f"📋 Versión generada: {version_info['version']}")
                print(f"📁 Cambios incrementales: {len(incremental_data)} productos")
                print(f"📊 Total acumulado: {len(accumulated_changes)} productos")
                print(f"💾 Base completa: {len(full_database)} productos")
                print(f"☁️ Archivos sincronizados con Google Drive")
            else:
                print(f"\n⚠️ Proceso completado con errores en Google Drive")
                print(f"💾 Archivos guardados localmente como respaldo")
            
        else:
            print("❌ Error generando base de datos completa")
        
        print(f"\n✅ Proceso completado exitosamente")
        print(f"📋 Versión generada: {version_info['version']}")
        print(f"📁 Cambios incrementales: {len(incremental_data)} productos")
        print(f"📊 Total acumulado: {len(accumulated_changes)} productos")
        
    else:
        print("\n✅ No se detectaron cambios en la última hora.")
        print("📋 No se generaron archivos de actualización.")
    
    print("\n" + "=" * 60)
    print("PROCESO COMPLETADO")
    print("=" * 60)

if __name__ == "__main__":
    main()