import pandas as pd
from sqlalchemy import create_engine, exc, text
from sqlalchemy.engine import Engine
import urllib.parse
import config.setting as st

def get_engine() -> Engine:
    """Crea un engine de SQLAlchemy para SQL Server"""
    try:
        if st.DB_CONFIG['trusted_connection'].lower() == 'yes':
            # Autenticación Windows
            params = {
                'driver': st.DB_CONFIG['driver'],
                'trusted_connection': 'yes'
            }
            connection_string = (
                f"mssql+pyodbc://@{urllib.parse.quote_plus(st.DB_CONFIG['server'])}"
                f"/{st.DB_CONFIG['database']}"
            )
        else:
            # Autenticación SQL Server - Usar parámetros en lugar de URL encoding
            params = {
                'driver': st.DB_CONFIG['driver']
            }
            
            # Para instancias con barra invertida, usar el formato completo
            username_encoded = st.DB_CONFIG['username']
            password_encoded = urllib.parse.quote_plus(st.DB_CONFIG['password'])
            server_encoded = st.DB_CONFIG['server']
            
            connection_string = (
                f"mssql+pyodbc://{username_encoded}:{password_encoded}"
                f"@{server_encoded}/{st.DB_CONFIG['database']}"
            )
        
        # Agregar parámetros a la URL
        param_string = "&".join([f"{k}={urllib.parse.quote_plus(v)}" for k, v in params.items()])
        connection_string += f"?{param_string}"
        
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,  # Verifica la conexión antes de usarla
            connect_args={
                'timeout': 10,  # Tiempo de espera para conexión (segundos)
                'login_timeout': 5  # Tiempo para autenticación
            })


        return engine
        
    except exc.DBAPIError as db_error:
        print(f"Error de conexión a la base de datos: {db_error}")
        raise
    except Exception as e:
        print(f"Error creando engine de SQLAlchemy: {e}")
        raise

def execute_query(query: str) -> pd.DataFrame:
    """Ejecuta una consulta y retorna un DataFrame usando SQLAlchemy"""
    try:
        print("#" * 5, " Conectando a la base de datos SQL Server...")
        engine = get_engine()
        
        print("#" * 5, " Ejecutando consulta...")
        df = pd.read_sql_query(query, engine)
        
        print(f"#" * 5, f" Consulta ejecutada exitosamente. Filas obtenidas: {len(df)}")
        return df
        
    except Exception as e:
        print(f"Error ejecutando consulta: {e}")
        raise

def test_connection() -> bool:
    """Prueba la conexión a la base de datos"""
    try:
        engine = get_engine()
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1")).fetchone()
            return result[0] == 1
    except Exception as e:
        print(f"Error en test_connection: {e}")
        return False
