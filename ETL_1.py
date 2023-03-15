# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 20:42:01 2023

@author: marcelochavez
"""
# =============================================================================
# Carga de librerías:
# =============================================================================
import pandas as pd
from sqlalchemy import Column, Date, Integer, String, Float, create_engine
parse_dates = ['date','time']

# Conexión al servidor PostgreSQL:
fuente = create_engine('postgresql://postgres:marce@localhost:5432/fuente')

# =============================================================================
# El Catálogo global de deslizamientos de tierra (GLC) se desarrolló con el objetivo de identificar
# eventos de deslizamientos de tierra provocados por la lluvia en todo el mundo, independientemente del tamaño,
# los impactos o la ubicación. El GLC considera todos los tipos de movimientos masivos provocados por lluvias, 
# que han sido informados en los medios de comunicación, bases de datos de desastres, informes científicos u otras fuentes.
# =============================================================================

db_glc = pd.read_csv(r"C:\Users\marcelochavez\Documents\PROCONTY\catalog.csv",
                     parse_dates=parse_dates)

db_glc['pk'] = range(1, 1+len(db_glc))

db_glc.rename(columns={"id":"identificador",
                       "state/province":"state_province",
                       "city/town":"city_town"},
              inplace=True)

# =============================================================================
# Perfilamiento de los datos:
# Para lo cual he construido una función sencilla que arroja un reporte con el
# nombre de la variable, el tipo de objeto en el ecosistema de pandas,
# su longitud máxima o mínima dependiendo si es numérica o categórica. Y si
# están presentes valores missing por cada variable
# =============================================================================

# Catálogo de variables del data frame:  

def perfilamiento(df):
    # Catálogo de variables:
    catalogo_variables = pd.DataFrame(df.dtypes).rename(columns={0:'tipo_variable'})
    catalogo_variables['len_max'] = [columnData.str.len().max() if columnData.dtype == object else columnData.max() for columnName, columnData in df.iteritems()]
    catalogo_variables['len_min'] = [columnData.str.len().min() if columnData.dtype == object else columnData.min() for columnName, columnData in df.iteritems()]
    catalogo_variables = pd.concat([catalogo_variables, df.isnull().sum()], axis=1).reset_index().rename(columns={'index':'variables',0:'missings'})    
    db_glc["date"] = pd.to_datetime(db_glc['date'], format='%Y%m%d')
    return  catalogo_variables

catalogo_variables = perfilamiento(db_glc)

# Creación de la tabla fuente en PostgreSQL:

db_glc.to_sql('db_glc',
              fuente,
              schema='fuente_db',
              index=False, 
              if_exists= "replace")