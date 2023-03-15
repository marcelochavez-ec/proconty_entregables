# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 17:28:45 2023

@author: marcelochavez
"""

# =============================================================================
# Importación de las librerías:
# =============================================================================

import pandas as pd
from sqlalchemy import create_engine

# Conexión al servidor PostgreSQL:
fuente = create_engine('postgresql://postgres:marce@localhost:5432/fuente') # DB fuente (inicial)
olap = create_engine('postgresql://postgres:marce@localhost:5432/olap') # DB olap (final)

db_glc = pd.read_sql("select * from stage.db_glc", con = fuente)

def perfilamiento(df):
    # Catálogo de variables:
    catalogo_variables = pd.DataFrame(df.dtypes).rename(columns={0:'tipo_variable'})
    catalogo_variables['len_max'] = [columnData.str.len().max() if columnData.dtype == object else columnData.max() for columnName, columnData in df.iteritems()]
    catalogo_variables['len_min'] = [columnData.str.len().min() if columnData.dtype == object else columnData.min() for columnName, columnData in df.iteritems()]
    catalogo_variables = pd.concat([catalogo_variables, df.isnull().sum()], axis=1).reset_index().rename(columns={'index':'variables',0:'missings'})    
    db_glc["date"] = pd.to_datetime(db_glc['date'], format='%Y%m%d')
    return  df, catalogo_variables

catalogo_variables = perfilamiento(db_glc)

# Reemplazamiento de los valores faltantes en var categóricas:

def reemplazar_valores_faltantes_var_categoricas(df, valor_cadena="Desconocido"):    
    # Seleccionar solo las variables categóricas con valores faltantes
    var_categoricas = df.select_dtypes(include=['object', 'category'])
    var_con_nulos = var_categoricas.columns[var_categoricas.isna().any()].tolist()

    # Reemplazar valores faltantes con la cadena especificada
    for columna in var_con_nulos:
        df[columna].fillna(valor_cadena, inplace=True)

    return df

# Función para reemplazar valores missing en var numéricas con 0:
    
def reemplazar_valores_faltantes_var_numericas(df):
    # Seleccionar solo las var numéricas con valores faltantes
    var_numericas = df.select_dtypes(include=['float', 'int'])
    var_con_nulos = var_numericas.columns[var_numericas.isna().any()].tolist()

    # Reemplazar valores faltantes con 0
    for columna in var_con_nulos:
        df[columna].fillna(0, inplace=True)

    return df

def split_geolocalizacion(df, col_name):
    final_df = df.copy()
    final_df[col_name] = final_df[col_name].str.replace(r'[()]',"")
    final_df[col_name] = final_df[col_name].str.replace("Desconocido","0")
    splited_full_name = final_df[col_name].str.split(",", expand=True)    
    final_df["latitud_comprobacion"] = splited_full_name.get(0).astype(float)
    final_df["longitud_comprobacion"] = splited_full_name.get(1).astype(float)
    return final_df

 # Implementación del Pipeline de Pandas:
     
db_glc_olap = (db_glc.
               pipe(reemplazar_valores_faltantes_var_categoricas).
               pipe(reemplazar_valores_faltantes_var_numericas).
               pipe(split_geolocalizacion, "geolocation"))

db_glc_olap.to_sql('db_glc_olap',
                   olap,
                   schema='olap_db',
                   index=False, 
                   if_exists= "replace")