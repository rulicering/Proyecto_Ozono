#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - ETL_Join_Mes_Cerrado - v0

#     0. Inicialización 
#     1. Datos
#         1.0 Carga de ficheros (Aire, Clima, Estaciones y Calendario)
#         1.1 Paso a Pandas
#         1.2 Tipos
#             1.2.0 Estaciones
#             1.2.1 Aire
#             1.2.2 Clima
#             1.2.3 Calendario
#         1.3 Darle grupo a estaciones de aire        
#     2. Unir AIRE + CLIMA
#         2.0 Separamos datos contaminacion (2014-18 | 2019-NOW)
#         2.1 MERGE (ESTACION AIRE, DIA) + (DATOS CLIMA )
#         2.2 Unimos partes generadas por el MERGE       
#     3. Incluir CALENDARIO   
#     4. Formato  
#     5. Actualizar Dato final
#     6. Exportar

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


spark = SparkSession.builder.appName('join_mes_cerrado').getOrCreate()


# # [1] Datos

# In[106]:


ultimo_dia_mes_cerrado = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
primer_dia_mes_cerrado = ultimo_dia_mes_cerrado.replace(day=1)
mes_cerrado = ultimo_dia_mes_cerrado.year *100 + ultimo_dia_mes_cerrado.month 


# ## [1.0] - Carga de ficheros (Aire, Clima, Estaciones y Calendario)

# In[4]:


df_aire = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mes_cerrado.csv',inferSchema= True,header=True)
df_clima = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima_mes_cerrado.csv',inferSchema= True,header=True)
df_estaciones = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-' + str(mes_cerrado)+'.csv',inferSchema= True,header=True)
df_calendario = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Calendario/Calendario_2001-2020.csv',inferSchema= True,header=True)


# ## [1.1] - Paso a Pandas

# In[30]:


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

# In[31]:


#None to NULO
pd_estaciones = pd_estaciones.replace(('None',None),np.nan)


# In[32]:


#Columnas estaciones medicion - 2019
regex = reg.compile("E_\d\d")
c_estacionxmagnitud_19 = [elem for elem in list(filter(regex.search,df_estaciones.columns))]
c_estacionxmagnitud_19


# In[33]:


#Columnas estaciones medicion a string
for columna in c_estacionxmagnitud_19:
    pd_estaciones[columna] = pd_estaciones[columna].astype(str)


# In[34]:


#pd_estaciones.dtypes


# ###  [1.2.1] - Aire

# In[35]:


#None to NULO
pd_aire = pd_aire.replace(('None',None),np.nan)


# In[36]:


#CODIGOS CORTOS A Strig
pd_aire["CODIGO_CORTO"] = pd_aire["CODIGO_CORTO"].astype(str)


# In[37]:


pd_aire = pd_aire.sort_values(by="FECHA")


# In[38]:


#pd_aire.dtypes


# ### [1.2.2] - Clima

# In[39]:


#None to NULO
pd_clima = pd_clima.replace(('None',None),np.nan)


# In[40]:


#Listar las columnas que miden magnitudes
columnas_valoresmagnitudes = list(pd_clima.columns)[5:]


# In[41]:


#Columnas magnitudes a float
for columna in columnas_valoresmagnitudes:
    #Comas por puntos
    pd_clima[columna]  =  [reg.sub(',','.',str(x)) for x in pd_clima[columna]]
    # String to float
    pd_clima[columna] = pd_clima[columna].astype(float)


# In[42]:


pd_clima = pd_clima.sort_values(by="FECHA")


# In[43]:


#pd_clima.dtypes


# ### [1.2.3] - Calendario

# In[44]:


pd_calendario = pd_calendario.replace(('None',None),np.nan)


# ## [1.3] - Darle grupo a estaciones de AIRE
# 
#     2014-2018 -> E_AEMET
#     
#     2019-NOW -> 81,82,83,87,89

# In[45]:


#Sacamos estaciones de aire y codigos de estaciones de clima asociadas
pd_estaciones_aire = pd_estaciones[pd_estaciones["MIDE_AIRE"]>0]


# In[46]:


#Nos quedamos con las columnas que queremos
c_agrupamiento = ["CODIGO_CORTO"] + c_estacionxmagnitud_19
pd_estaciones_aire = pd_estaciones_aire[c_agrupamiento]


# In[47]:


#Unimos ambos datasets
pd_aire = pd_aire.merge(pd_estaciones_aire, on =["CODIGO_CORTO"])


# In[48]:


#pd_aire


# # [2] -  UNIR AIRE + CLIMA

# In[52]:


pd_datos = pd_aire


# ## [2.1] - MERGE (ESTACION AIRE, DIA) + (DATOS CLIMA )

# In[50]:


# Separamos columnas de info (comunes) y las columnas de datos
columnas = list(pd_clima.columns) 
c_info = columnas[:5]
c_magnitudes = columnas[5:]


# In[53]:


#Asociamos cada columna de datos de clima (magnitud) a la estación de aire correspondiente.  
for magnitud in c_magnitudes:
    cols = c_info.copy()
    cols.append(magnitud)
    
    pd_clima_magnitud = pd_clima[cols]

    #MES_CERRADO
    pd_clima_magnitud_mes_cerrado = pd_clima_magnitud.rename(columns={"CODIGO_CORTO":"E_%s"%magnitud})
    pd_datos = pd_datos.merge(pd_clima_magnitud_mes_cerrado,on = ["ANO", "MES", "DIA","FECHA","E_%s"%magnitud])


# In[55]:


#pd_datos


# # [3] - Incluir CALENDARIO

# In[56]:


pd_datos_y_calendario = pd_datos.merge(pd_calendario, on = "FECHA")


# # [4] - FORMATO

# In[57]:


#Borramos las columnas utilizadas para relacionar estaciones de aire y clima
a_borrar = c_estacionxmagnitud_19


# In[58]:


pd_final = pd_datos_y_calendario.drop(columns = a_borrar)


# In[59]:


cols = pd_final.columns.tolist()


# In[60]:


cols = cols[0:5]+ cols[-3:]+ cols [5:-3]


# In[61]:


pd_final = pd_final[cols]


# In[123]:


pd_mes_cerrado = pd_final


# # [5] - ACTUALIZAR DATOS FINALES

# In[118]:


datos_finales = pd.read_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos.csv")


# In[119]:


#Quitamos la primera columna, innecesaria
datos_finales = datos_finales[datos_finales.columns.tolist()[1:]]


# In[120]:


ini = primer_dia_mes_cerrado.strftime("%Y%m%d")
fin = ultimo_dia_mes_cerrado.strftime("%Y%m%d")


# In[121]:


datos_finales_sin_mes_cerrado = datos_finales[(datos_finales["FECHA"] < int(ini)) | (datos_finales["FECHA"] > int(fin))]


# In[126]:


datos_finales= pd.concat([datos_finales_sin_mes_cerrado,pd_mes_cerrado])


# In[131]:


datos_finales = datos_finales.sort_values(by=(["FECHA","CODIGO_CORTO"]))


# # [6] - EXPORTAR

# In[37]:


#Versiones
hoy = datetime.date.today().strftime("%Y-%m-%d")
datos_finales.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/BackUp/Join/Datos-[ACT]-mes_cerrado-" + hoy + ".csv")


# In[38]:


datos_finales.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos-.csv")


# # [EXTRA] - CHECKEO

# In[40]:


#pd_chequeo = pd_datos[['CODIGO_CORTO', 'FECHA','E_AEMET','E_81', 'E_82', 'E_83', 'E_87', 'E_89', '81', '82', '83', '87', '89']]


# In[41]:


#2014-2018
#pd_chequeo[(pd_datos["FECHA"]==20140101)].sort_values(by="E_AEMET")
#pd_clima[pd_clima["FECHA"]==20140101]

#2019-NOW
#pd_chequeo[(pd_datos["FECHA"]==20190101)]
#pd_clima[pd_clima["FECHA"]==20190101]

