#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - ETL_Join_Dia_v0

#     0. Inicialización 
#     1. Datos
#         1.0 Carga de ficheros (Aire, Clima, Estaciones y Calendario) HOY
#         1.1 Paso a Pandas
#         1.2 Tipos
#             1.2.0 Estaciones
#             1.2.1 Aire
#             1.2.2 Clima
#             1.2.3 Calendario
#         1.3 Darle grupo a estaciones de aire        
#     2. Unir AIRE + CLIMA
#         2.0 MERGE (ESTACION AIRE, DIA) + (DATOS CLIMA )  
#     3. Incluir CALENDARIO   
#     4. Formato  
#     5. Exportar
#     6. Unir a dato final
# 

# # [0] - Inicialización

# In[ ]:


import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
import re as reg
import numpy as np
import datetime


# In[ ]:


spark = SparkSession.builder.appName('join_hoy').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# # [1] Datos

# ## [1.0] - Carga de ficheros (Aire, Clima, Estaciones y Calendario)
#     Se ejecuta a las 2 de la mañana del dia siguiente

# In[ ]:


dia = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


# In[ ]:


df_aire = spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mediadia-" + dia +".csv",inferSchema= True,header=True)
df_clima = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima-' + dia +  '.csv',inferSchema= True,header=True)
df_estaciones = spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + dia +".csv",inferSchema= True,header=True)
df_calendario = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Calendario/Calendario_2001-2020.csv',inferSchema= True,header=True)


# ## [1.1] - Paso a Pandas

# In[ ]:


pd_aire = df_aire.drop("_c0").toPandas()
pd_clima = df_clima.drop("_c0").toPandas()
pd_estaciones = df_estaciones.drop("_c0").toPandas()
pd_calendario = df_calendario.drop("_c0").toPandas()


# ## [1.2] - Tipos
#     
#     CODIGO_CORTO -> STRING/OBJECT
#     ANO -> INTEGER
#     MES -> INTEGER
#     DIA -> INTEGER
#     FECHA -> INTEGER
#     MEDICIONES -> DOUBLE

# ### [1.2.0] -  Estaciones

# In[ ]:


#None to NULO
pd_estaciones = pd_estaciones.replace(('None',None),np.nan)


# In[ ]:


#Columnas estaciones medicion - 2014
regex = reg.compile("E_AEMET_HOY")
c_aemet_hoy = [elem for elem in list(filter(regex.search,df_estaciones.columns))]
c_aemet_hoy


# In[ ]:


#Columnas estaciones medicion a string
for columna in c_aemet_hoy:
    pd_estaciones[columna] = pd_estaciones[columna].astype(str)


# In[ ]:


#pd_estaciones.dtypes


# ###  [1.2.1] - Aire

# In[ ]:


#None to NULO
pd_aire = pd_aire.replace(('None',None),np.nan)


# In[ ]:


#CODIGOS CORTOS A Strig
pd_aire["CODIGO_CORTO"] = pd_aire["CODIGO_CORTO"].astype(str)


# In[ ]:


pd_aire = pd_aire.sort_values(by="FECHA")


# In[ ]:


print("Estaciones que miden datoss actuales de contaminación: ", pd_aire["CODIGO_CORTO"].count())


# In[ ]:


#pd_aire.dtypes


# ### [1.2.2] - Clima

# In[ ]:


#None to NULO
pd_clima = pd_clima.replace(('None',None),np.nan)


# In[ ]:


#Listar las columnas que miden magnitudes
columnas_valoresmagnitudes = list(pd_clima.columns)[5:]


# In[ ]:


#Columnas magnitudes a float
for columna in columnas_valoresmagnitudes:
    #Comas por puntos
    pd_clima[columna]  =  [reg.sub(',','.',str(x)) for x in pd_clima[columna]]
    # String to float
    pd_clima[columna] = pd_clima[columna].astype(float)


# In[ ]:


pd_clima = pd_clima.sort_values(by="FECHA")


# In[ ]:


#pd_clima.dtypes


# ### [1.2.3] - Calendario

# In[ ]:


pd_calendario = pd_calendario.replace(('None',None),np.nan)


# ## [1.3] - Darle grupo a estaciones de AIRE
# 
#     E_AEMET_HOY -> ESTACION AIRE

# In[ ]:


#Sacamos estaciones de aire y codigos de estaciones de clima asociadas
pd_estaciones_aire = pd_estaciones[pd_estaciones["MIDE_AIRE"]>0]


# In[ ]:


#Nos quedamos con las columnas que queremos
c_agrupamiento = ["CODIGO_CORTO"] + c_aemet_hoy
pd_estaciones_aire = pd_estaciones_aire[c_agrupamiento]


# In[ ]:


#Unimos ambos datasets
pd_aire = pd_aire.merge(pd_estaciones_aire, on =["CODIGO_CORTO"])


# In[ ]:


#pd_aire


# # [2] -  UNIR AIRE + CLIMA

# In[ ]:


#Datos hoy
pd_datos_hoy = pd_aire


# ## [2.0] - MERGE (ESTACION AIRE + DATOS CLIMA )

# In[ ]:


# Separamos columnas de info (comunes) y las columnas de datos
columnas = list(pd_clima.columns) 
c_info = columnas[:5]
c_magnitudes = columnas[5:]


# In[ ]:


#Asociamos cada columna de datos de clima (magnitud) a la estación de aire correspondiente.  
for magnitud in c_magnitudes:
    cols = c_info.copy()
    cols.append(magnitud)
    
    pd_clima_magnitud_hoy = pd_clima[cols]
    
    #HOY
    pd_clima_magnitud_hoy = pd_clima_magnitud_hoy.rename(columns={"CODIGO_CORTO":"E_AEMET_HOY_%s"%magnitud})
    pd_datos_hoy = pd_datos_hoy.merge(pd_clima_magnitud_hoy,on = ["ANO", "MES", "DIA","FECHA","E_AEMET_HOY_%s"%magnitud])


# # [3] - Incluir CALENDARIO

# In[ ]:


pd_datos_hoy_y_calendario = pd_datos_hoy.merge(pd_calendario, on = "FECHA")


# In[ ]:


#pd_datos_hoy_y_calendario.dtypes


# # [4] - FORMATO

# In[ ]:


#Borramos las columnas utilizadas para relacionar estaciones de aire y clima
a_borrar = c_aemet_hoy


# In[ ]:


pd_datos_hoy = pd_datos_hoy_y_calendario.drop(columns = a_borrar)


# In[ ]:


cols = pd_datos_hoy.columns.tolist()
cols = [cols[4]] + cols[0:4] +cols[-3:]+ cols[5:-3]
pd_datos_hoy = pd_datos_hoy[cols]


# In[ ]:


#pd_datos_hoy.columns


# In[ ]:


pd_datos_hoy["CODIGO_CORTO"] = pd_datos_hoy["CODIGO_CORTO"].astype(int)


# # [5] - EXPORTAR

# In[ ]:


#EL join se ejecuta a las 2 am del dia siguiente. 1 día de diferencia
nuevo = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
anterior = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")


# In[ ]:


#Versiones
pd_datos_hoy.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/BackUp/Datos-Dia-" + nuevo + ".csv")


# In[ ]:


pd_datos_hoy.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos-Dia-" + nuevo + ".csv")
print("[INFO] - Datos-Dia-", nuevo,".csv --- Created successfully")


# In[ ]:


#Borrar la de ayer
try:
    os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos-Dia-" + anterior + ".csv")
    print("[INFO] - Datos-Dia-", anterior,".csv --- Removed successfully")
except:
    print("[ERROR] - Datos-Dia-", anterior,".csv --- Could not been removed")


# # [6] - UNIR A DATO FINAL

# In[ ]:


pd_datos = pd.read_csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos.csv')


# In[ ]:


pd_datos = pd_datos.drop(columns = ["Unnamed: 0"])


# In[ ]:


pd_datos_final = pd.concat([pd_datos,pd_datos_hoy])


# In[ ]:


#Versiones
pd_datos_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/BackUp/Join_diario/Datos-" + nuevo + ".csv")


# In[ ]:


pd_datos_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos.csv")
print("[INFO] - JOIN DATOS HOY + TOTAL - Successfully")


# # [EXTRA] - CHECKEO

# In[ ]:


#pd_chequeo = pd_datos[['CODIGO_CORTO', 'FECHA','E_AEMET','E_81', 'E_82', 'E_83', 'E_87', 'E_89', '81', '82', '83', '87', '89']]


# In[ ]:


#2014-2018
#pd_chequeo[(pd_datos["FECHA"]==20140101)].sort_values(by="E_AEMET")
#pd_clima[pd_clima["FECHA"]==20140101]

#2019-NOW
#pd_chequeo[(pd_datos["FECHA"]==20190101)]
#pd_clima[pd_clima["FECHA"]==20190101]

