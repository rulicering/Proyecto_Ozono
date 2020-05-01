#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - ETL_Join_Dia_Actual - v0

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
#     EXTRA. CHECKEO
# 

# # [0] - Inicialización

# In[1]:


import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
import re as reg
import numpy as np
import datetime


# In[2]:


spark = SparkSession.builder.appName('contaminacion').getOrCreate()


# # [1] Datos

# ## [1.0] - Carga de ficheros (Aire, Clima, Estaciones y Calendario) HOY

# In[3]:


df_aire = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mediadia.csv',inferSchema= True,header=True)
df_clima = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima-hoy.csv',inferSchema= True,header=True)
df_estaciones = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-hoy.csv',inferSchema= True,header=True)
df_calendario = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Calendario/Calendario_2013-2020.csv',inferSchema= True,header=True)


# ## [1.1] - Paso a Pandas

# In[4]:


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

# In[5]:


#None to NULO
pd_estaciones = pd_estaciones.replace(('None',None),np.nan)


# In[6]:


#Columnas estaciones medicion - 2014
regex = reg.compile("E_AEMET_HOY")
c_aemet_hoy = [elem for elem in list(filter(regex.search,df_estaciones.columns))]
c_aemet_hoy


# In[7]:


#Columnas estaciones medicion a string
for columna in c_aemet_hoy:
    pd_estaciones[columna] = pd_estaciones[columna].astype(str)


# In[8]:


#pd_estaciones.dtypes


# ###  [1.2.1] - Aire

# In[9]:


#None to NULO
pd_aire = pd_aire.replace(('None',None),np.nan)


# In[10]:


#CODIGOS CORTOS A Strig
pd_aire["CODIGO_CORTO"] = pd_aire["CODIGO_CORTO"].astype(str)


# In[11]:


pd_aire = pd_aire.sort_values(by="FECHA")


# In[12]:


print("Estaciones que miden datoss actuales de contaminación: ", pd_aire["CODIGO_CORTO"].count())


# In[13]:


#pd_aire.dtypes


# ### [1.2.2] - Clima

# In[14]:


#None to NULO
pd_clima = pd_clima.replace(('None',None),np.nan)


# In[15]:


#Listar las columnas que miden magnitudes
columnas_valoresmagnitudes = list(pd_clima.columns)[5:]


# In[16]:


#Columnas magnitudes a float
for columna in columnas_valoresmagnitudes:
    #Comas por puntos
    pd_clima[columna]  =  [reg.sub(',','.',str(x)) for x in pd_clima[columna]]
    # String to float
    pd_clima[columna] = pd_clima[columna].astype(float)


# In[17]:


pd_clima = pd_clima.sort_values(by="FECHA")


# In[18]:


#pd_clima.dtypes


# ### [1.2.3] - Calendario

# In[19]:


pd_calendario = pd_calendario.replace(('None',None),np.nan)


# ## [1.3] - Darle grupo a estaciones de AIRE
# 
#     E_AEMET_HOY -> ESTACION AIRE

# In[20]:


#Sacamos estaciones de aire y codigos de estaciones de clima asociadas
pd_estaciones_aire = pd_estaciones[pd_estaciones["MIDE_AIRE"]>0]


# In[21]:


#Nos quedamos con las columnas que queremos
c_agrupamiento = ["CODIGO_CORTO"] + c_aemet_hoy
pd_estaciones_aire = pd_estaciones_aire[c_agrupamiento]


# In[22]:


#Unimos ambos datasets
pd_aire = pd_aire.merge(pd_estaciones_aire, on =["CODIGO_CORTO"])


# In[23]:


#pd_aire


# # [2] -  UNIR AIRE + CLIMA

# In[24]:


#Datos hoy
pd_datos_hoy = pd_aire


# ## [2.0] - MERGE (ESTACION AIRE + DATOS CLIMA )

# In[25]:


# Separamos columnas de info (comunes) y las columnas de datos
columnas = list(pd_clima.columns) 
c_info = columnas[:5]
c_magnitudes = columnas[5:]


# In[26]:


#Asociamos cada columna de datos de clima (magnitud) a la estación de aire correspondiente.  
for magnitud in c_magnitudes:
    cols = c_info.copy()
    cols.append(magnitud)
    
    pd_clima_magnitud_hoy = pd_clima[cols]
    
    #HOY
    pd_clima_magnitud_hoy = pd_clima_magnitud_hoy.rename(columns={"CODIGO_CORTO":"E_AEMET_HOY_%s"%magnitud})
    pd_datos_hoy = pd_datos_hoy.merge(pd_clima_magnitud_hoy,on = ["ANO", "MES", "DIA","FECHA","E_AEMET_HOY_%s"%magnitud])


# # [3] - Incluir CALENDARIO

# In[27]:


pd_datos_hoy_y_calendario = pd_datos_hoy.merge(pd_calendario, on = "FECHA")


# In[28]:


#pd_datos_hoy_y_calendario.dtypes


# # [4] - FORMATO

# In[29]:


#Borramos las columnas utilizadas para relacionar estaciones de aire y clima
a_borrar = c_aemet_hoy


# In[37]:


pd_datos_hoy = pd_datos_hoy_y_calendario.drop(columns = a_borrar)


# In[74]:


cols = pd_datos_hoy.columns.tolist()
cols = [cols[4]] + cols[0:4] + cols[5:]
pd_datos_hoy = pd_datos_hoy[cols]


# In[82]:


pd_datos_hoy["CODIGO_CORTO"] = pd_datos_hoy["CODIGO_CORTO"].astype(int)


# In[75]:


#pd_final


# # [5] - EXPORTAR

# In[87]:


#Versiones
hoy = datetime.date.today().strftime("%Y-%m-%d")
pd_datos_hoy.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/BackUp/Datos-Dia-" + hoy + ".csv")


# In[88]:


pd_datos_hoy.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos-hoy.csv")


# # [6] - UNIR A DATO FINAL

pd_datos = pd.read_csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos.csv')

pd_datos = pd_datos.drop(columns = ["Unnamed: 0"])

pd_datos_final = pd.concat([pd_datos,pd_datos_hoy])

#Versiones
pd_datos_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/BackUp/Datos-" + hoy + ".csv")

pd_datos_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos.csv")


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

