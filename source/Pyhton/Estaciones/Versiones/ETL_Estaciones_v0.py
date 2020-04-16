#!/usr/bin/env python
# coding: utf-8

# # [o3] - Proyecto Ozono - ETL_Estaciones  - v0

# #Origenes
# 
# Aire : https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=9e42c176313eb410VgnVCM1000000b205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD&vgnextfmt=default
# 
# Tiempo: https://datos.madrid.es/sites/v/index.jsp?vgnextoid=2ac5be53b4d2b610VgnVCM2000001f4a900aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD

# ## [0] - Librerias

# In[135]:


import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
import pandas as pd


# ## [1] - Inicializar sesión de Spark

# In[136]:


spark = SparkSession.builder.appName('estaciones').getOrCreate()


# ## [2] - Carga fichero Aire

# In[137]:


# https://datos.madrid.es/egob/catalogo/212629-0-estaciones-control-aire.xls


# In[138]:


pd_aire = pd.read_excel("https://datos.madrid.es/egob/catalogo/212629-0-estaciones-control-aire.xls")


# In[141]:


pd_aire = pd_aire[['CODIGO_CORTO', 'ESTACION', 'DIRECCION', 'LONGITUD_ETRS89',
       'LATITUD_ETRS89', 'ALTITUD','LONGITUD', 'LATITUD']]
pd_aire["MIDE_AIRE"] = 1


# In[143]:


print("Número de estaciones medición calidad del aire: %d" % pd_aire["ESTACION"].count())

for elem in pd_aire[["CODIGO_CORTO", "ESTACION"]].values.tolist():
    print(elem)


# ## [3] -  Carga fichero Tiempo

# In[144]:


#https://datos.madrid.es/egob/catalogo/300360-0-meteorologicos-estaciones.xls


# In[145]:


pd_tiempo = pd.read_excel("https://datos.madrid.es/egob/catalogo/300360-0-meteorologicos-estaciones.xls")


# In[150]:


pd_tiempo = pd_tiempo[['CÓDIGO_CORTO', 'ESTACIÓN', 'DIRECCIÓN', 'LONGITUD_ETRS89',
       'LATITUD_ETRS89', 'ALTITUD','LONGITUD', 'LATITUD']]
pd_tiempo["MIDE_CLIMA"] = 1


# In[151]:


#Modificamos los nombres de las columnas -> sin tildes para que cuadre luego al hacer el join
pd_tiempo = pd_tiempo.rename(columns = {"CÓDIGO_CORTO" : "CODIGO_CORTO",'ESTACIÓN': 'ESTACION', 'DIRECCIÓN':'DIRECCION'})


# In[153]:


print("Número de estaciones medición clima: %d" % pd_tiempo["ESTACION"].count())

for elem in pd_tiempo[["CODIGO_CORTO", "ESTACION"]].values.tolist():
    print(elem)


# ## [4] - MERGE

# In[159]:


#pd_final = pd_aire.join(pd_tiempo,lsuffix = "_aire",rsuffix = "_tiempo", on =["CODIGO_CORTO"], how = "outer", sort = True) 
# No tienen el mismo tipo las columnas de Código corto en ambos datasets

pd_final = pd_aire.merge(pd_tiempo, on =['CODIGO_CORTO', 'ESTACION', 'DIRECCION', 'LONGITUD_ETRS89',
       'LATITUD_ETRS89', 'ALTITUD', 'LONGITUD', 'LATITUD'], how = "outer")


# In[160]:


#pd_final["CODIGO_CORTO"].count()


# ## [5] - ELIMINAMOS DUPLICADOS

# In[166]:


pd_final.duplicated(["CODIGO_CORTO"])
pd_final = pd_final.drop_duplicates(["CODIGO_CORTO"],keep='first')


# In[167]:


#pd_final["CODIGO_CORTO"].count()


# ## [6] - NULL -> 0

# In[171]:


pd_final = pd_final.fillna(0)


# ## [7] - EXPORTAMOS

# In[172]:


pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Versiones/[v0]-Estaciones.csv")

