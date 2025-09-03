import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
# Configuración de base de datos
DB_CONFIG = {
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_NAME'),
    'username': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'driver': os.getenv('DB_DRIVER'),
    'trusted_connection': 'no'  # cambiar a 'yes' si se usa autenticación Windows
}

ROOT_DIR = os.path.normpath(os.path.dirname(os.path.abspath(__file__)) + os.sep + os.pardir)

# Configuración de archivos
today = datetime.now()
months = f"{today.month:02d}"
days = f"{today.day:02d}"

# Rutas de salida (mantienen la estructura original)
outputPathDataJSON = "G:\\Mi unidad\\ARTICULOS JSON\\"
ouputFullData = ROOT_DIR + "\\output\\last_full_data.json"
ouputDir = ROOT_DIR + "\\output\\"
ouputFullDataDrive = "G:\\Mi unidad\\ARTICULOS JSON\\last_full_data.json"
pathProcess = "\\processed\\" + str(today.year) + "\\" + months

# Configuración de consulta
QUERY_TIMEOUT = 300  # 5 minutos timeout para queries grandes