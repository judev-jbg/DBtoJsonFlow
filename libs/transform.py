import pandas as pd
import sys
import os
from libs.database import execute_query

def get_articles_query_incremental():
    """Retorna la consulta SQL para obtener artículos"""
    return """
    SELECT 
        a.idArticulo AS referencia,
        p.referencia_proveedor,
        Descrip AS descripcion,
        ISNULL(CantidadBulto,1) AS cantidad_bulto,
        ISNULL(ca.unidad_venta,1) AS unidad_venta,
        familia,
        ISNULL(stock_actual,0) AS stock_actual,
        ISNULL(precio_actual,0) AS precio_actual,
        ISNULL(ca.descuento,'0000') AS descuento,
        ISNULL(localizacion,'SU') AS localizacion,
        estado
    FROM [dbo].[Articulos] a WITH (NOLOCK)
    LEFT JOIN
        (
        SELECT 
            IdProveedor,
            idArticulo,
            Articulo as referencia_proveedor,
            Norma AS codigo_barras,
            FechaInsertUpdate
        FROM [dbo].[Prov_Articulos] WITH (NOLOCK)
        WHERE IdProveedor <> '410000051'
        ) p
        ON p.idArticulo = a.IdArticulo AND p.IdProveedor = a.IdProveedorPreferencial
    LEFT JOIN
        (
        SELECT
            IdArticulo,
            ISNULL(TipoDescuentoMax, '0000') AS descuento,
            ISNULL(UdVenta,0) AS unidad_venta,
            Pers_NoActivoCentral
        FROM [dbo].[conf_articulos] WITH (NOLOCK)
        ) ca
        ON ca.IdArticulo = a.IdArticulo
    LEFT JOIN
        (
        SELECT
            IdFamilia,
            Descrip AS familia,
            FechaInsertUpdate
        FROM [dbo].[Articulos_Familias] WITH (NOLOCK)
        ) f
        ON f.IdFamilia = a.IdFamilia
    LEFT JOIN
        (
        SELECT
            IdArticulo,
            ISNULL(Stock,0) AS stock_actual,
            FechaInsertUpdate
        FROM [dbo].[Articulos_Stock] WITH (NOLOCK)
        WHERE IdAlmacen = 1
        ) s
        ON s.IdArticulo = a.IdArticulo
    LEFT JOIN
        (
        SELECT
            IdArticulo,
            ISNULL(Precio,0) AS precio_actual,
            FechaInsertUpdate
        FROM [dbo].[Listas_Precios_Cli_Art] WITH (NOLOCK)
        WHERE IdLista = 1
        ) pr
        ON pr.IdArticulo = a.IdArticulo
    LEFT JOIN
        (
        SELECT
            IdArticulo,
            localizacion,
            FechaInsertUpdate
        FROM [dbo].[Articulos_Localizacion]
        WHERE IdAlmacen = 1
        ) l
        ON l.IdArticulo = a.IdArticulo
    WHERE 
        p.referencia_proveedor IS NOT NULL 
        AND ca.Pers_NoActivoCentral = 0
        AND (
            a.FechaInsertUpdate >= DATEADD(MINUTE, -65, GETDATE())
            OR p.FechaInsertUpdate >= DATEADD(MINUTE, -65, GETDATE())
            OR f.FechaInsertUpdate >= DATEADD(MINUTE, -65, GETDATE())
            OR s.FechaInsertUpdate >= DATEADD(MINUTE, -65, GETDATE())
            OR pr.FechaInsertUpdate >= DATEADD(MINUTE, -65, GETDATE())
            OR l.FechaInsertUpdate >= DATEADD(MINUTE, -65, GETDATE())
        )
    ORDER BY a.FechaInsertUpdate DESC
    """

def get_articles_query_full():
    """Retorna la consulta SQL para obtener artículos"""
    return """
    SELECT 
        a.idArticulo AS referencia,
        p.referencia_proveedor,
        Descrip AS descripcion,
        ISNULL(CantidadBulto,1) AS cantidad_bulto,
        ISNULL(ca.unidad_venta,1) AS unidad_venta,
        familia,
        ISNULL(stock_actual,0) AS stock_actual,
        ISNULL(precio_actual,0) AS precio_actual,
        ISNULL(ca.descuento,'0000') AS descuento,
        ISNULL(localizacion,'SU') AS localizacion,
        estado
    FROM [dbo].[Articulos] a WITH (NOLOCK)
    LEFT JOIN
        (
        SELECT 
            IdProveedor,
            idArticulo,
            Articulo as referencia_proveedor,
            Norma AS codigo_barras 
        FROM [dbo].[Prov_Articulos] WITH (NOLOCK)
        WHERE IdProveedor <> '410000051'
        ) p
        ON p.idArticulo = a.IdArticulo AND p.IdProveedor = a.IdProveedorPreferencial
    LEFT JOIN
        (
        SELECT
            IdArticulo,
            ISNULL(TipoDescuentoMax, '0000') AS descuento,
            ISNULL(UdVenta,0) AS unidad_venta,
            Pers_NoActivoCentral
        FROM [dbo].[conf_articulos] WITH (NOLOCK)
        ) ca
        ON ca.IdArticulo = a.IdArticulo
    LEFT JOIN
        (
        SELECT
            IdFamilia,
            Descrip AS familia
        FROM [dbo].[Articulos_Familias] WITH (NOLOCK)
        ) f
        ON f.IdFamilia = a.IdFamilia
    LEFT JOIN
        (
        SELECT
            IdArticulo,
            ISNULL(Stock,0) AS stock_actual
        FROM [dbo].[Articulos_Stock] WITH (NOLOCK)
        WHERE IdAlmacen = 1
        ) s
        ON s.IdArticulo = a.IdArticulo
    LEFT JOIN
        (
        SELECT
            IdArticulo,
            ISNULL(Precio,0) AS precio_actual
        FROM [dbo].[Listas_Precios_Cli_Art] WITH (NOLOCK)
        WHERE IdLista = 1
        ) pr
        ON pr.IdArticulo = a.IdArticulo
    LEFT JOIN
        (
        SELECT
            IdArticulo,
            localizacion
        FROM [dbo].[Articulos_Localizacion]
        WHERE IdAlmacen = 1
        ) l
        ON l.IdArticulo = a.IdArticulo
    WHERE p.referencia_proveedor IS NOT NULL AND ca.Pers_NoActivoCentral = 0
    ORDER BY a.FechaInsertUpdate DESC
    """

def getDataFromDatabase(use_incremental: bool = True):
    """Lee datos desde la base de datos SQL Server"""
    try:
        print("#" * 5, " ¡Proceso de lectura de datos desde SQL Server! ", "#" * 5)
        print("#" * 5, " -Ejecutando consulta en la base de datos...")

        # Elegir consulta según el tipo
        if use_incremental:
            query = get_articles_query_incremental()
            print("#" * 5, " -Usando consulta INCREMENTAL (última hora)")
        else:
            query = get_articles_query_full()
            print("#" * 5, " -Usando consulta COMPLETA (todos los productos)")

        df = execute_query(query)
        
        if len(df) == 0:
            print("#" * 5, " No se encontraron datos en la consulta.")
            return [pd.DataFrame(), 0]
        
        # Limpiar y procesar datos (similar a la función original pero simplificado)
        df = clean_dataframe(df)
        
        # # Agregar información de carga
        # df["loadFileName"] = f"database_{st.today.year}{st.months}{st.days}_query"
        
        print("#" * 5, f" ¡Proceso de lectura exitoso! Registros obtenidos: {len(df)}")
        print("#" * 5, " ---------------------------------------- ", "\n")
        
        return [df, 1]
        
    except Exception as e:
        print("#" * 5, " ¡Ocurrió un problema al recuperar datos de la base de datos!")
        
        # Manejo de errores similar al original
        type_class = str(sys.exc_info()[0]).replace("<class ", "").replace(">", "").replace("'", "")
        type_desc = str(sys.exc_info()[1]).replace("<", "").replace(">", "")
        
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        e_line = exc_tb.tb_lineno
        
        print(f"Error: {type_class} - {type_desc}")
        print(f"Archivo: {fname}, Línea: {e_line}")
        
        # Aquí puedes agregar tu lógica de notificación de errores
        # event_error(...) y send_the_event(...) si las tienes implementadas
        
        return [pd.DataFrame(), 0]

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y procesa el DataFrame obtenido de la base de datos"""
    
    # Crear una copia para trabajar
    df_clean = df.copy()
    
    # Limpiar espacios en blanco
    string_columns = ['referencia', 'descripcion', 'familia', 'descuento', 'localizacion', 'estado']
    for col in string_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
    
    # Convertir columnas numéricas y manejar valores nulos
    numeric_columns = {
        'cantidad_bulto': 1,    # valor por defecto si es nulo o negativo
        'unidad_venta': 1,      # valor por defecto si es nulo o negativo  
        'stock_actual': 0,      # valor por defecto si es nulo o negativo
        'precio_actual': 0      # valor por defecto si es nulo o negativo
    }
    
    for col, default_value in numeric_columns.items():
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].fillna(default_value)
            df_clean[col] = df_clean[col].apply(lambda x: default_value if x < 0 else x)
    
    # Manejar valores por defecto para campos específicos
    if 'descuento' in df_clean.columns:
        df_clean['descuento'] = df_clean['descuento'].fillna('0000')
        
    if 'localizacion' in df_clean.columns:
        df_clean['localizacion'] = df_clean['localizacion'].fillna('SU')
    
    # Eliminar saltos de línea en todas las columnas de texto
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].astype(str).str.replace('\n', '').str.replace('\r', '')
    
    return df_clean