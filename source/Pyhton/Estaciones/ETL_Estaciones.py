#!/usr/bin/env python
# coding: utf-8

# # [o3] - Proyecto Ozono -ETL_Estaciones- v2

# ### [.0] - DISTANCIA MÍNIMA PARA ASOCIAR AIRE-CLIMA

# ### [.1] -> 2014-18 | [.2] ->2019-NOW

# #Origenes
# 
# Aire : https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=9e42c176313eb410VgnVCM1000000b205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD&vgnextfmt=default
# 
# Tiempo: https://datos.madrid.es/sites/v/index.jsp?vgnextoid=2ac5be53b4d2b610VgnVCM2000001f4a900aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD

# ## [0] - Librerias

# In[1]:


from __future__ import print_function
import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

#AEMET
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint
import datetime
import requests,json
import re as reg


# ## [1] - Inicializar sesión de Spark

# In[2]:


spark = SparkSession.builder.appName('estaciones_2019').getOrCreate()


# ## [2] - ESTACIONES CONTROL AIRE (Ayunt. Madrid)

# In[3]:


# https://datos.madrid.es/egob/catalogo/212629-0-estaciones-control-aire.xls


# In[4]:


pd_aire = pd.read_excel("https://datos.madrid.es/egob/catalogo/212629-0-estaciones-control-aire.xls")


# In[5]:


#pd_aire.head(5)


# In[6]:


pd_aire = pd_aire[['CODIGO_CORTO', 'ESTACION', 'DIRECCION', 'ALTITUD','LONGITUD', 'LATITUD']]
#pd_aire["MIDE_AIRE"] = 1 // Te lo mete como float
#pd_aire.LATITUD= pd_aire.LATITUD.round(6)
#pd_aire.LONGITUD= pd_aire.LONGITUD.round(6)
pd_aire.insert(6,"MIDE_AIRE",1)


# In[7]:


print("Número de estaciones medición calidad del aire: %d" % pd_aire["ESTACION"].count())

for elem in pd_aire[["CODIGO_CORTO", "ESTACION"]].values.tolist():
    print(elem)


# ## [3.2] -  ESTACIONES CONTROL METEOROLÓGICO (Ayunt. Madrid)

# In[8]:


#https://datos.madrid.es/egob/catalogo/300360-0-meteorologicos-estaciones.xls


# In[9]:


pd_tiempo = pd.read_excel("https://datos.madrid.es/egob/catalogo/300360-0-meteorologicos-estaciones.xls")


# In[10]:


pd_tiempo = pd_tiempo[['CÓDIGO_CORTO', 'ESTACIÓN', 'DIRECCIÓN', 'ALTITUD','LONGITUD', 'LATITUD']]
#pd_tiempo["MIDE_CLIMA"] = 1 #Te lo mete como float
#pd_tiempo.LATITUD= pd_tiempo.LATITUD.round(6)
#pd_tiempo.LONGITUD= pd_tiempo.LONGITUD.round(6)
pd_tiempo.insert(6,"MIDE_CLIMA",1)


# In[11]:


#Modificamos los nombres de las columnas -> sin tildes para que cuadre luego al hacer el join
pd_tiempo = pd_tiempo.rename(columns = {"CÓDIGO_CORTO" : "CODIGO_CORTO",'ESTACIÓN': 'ESTACION', 'DIRECCIÓN':'DIRECCION'})


# In[12]:


print("Número de estaciones medición clima: %d" % pd_tiempo["ESTACION"].count())

for elem in pd_tiempo[["CODIGO_CORTO", "ESTACION"]].values.tolist():
    print(elem)


# ## [4.1] - AYUNTAMIENTO = AIRE

# In[13]:


pd_estaciones_ayunt_1 = pd_aire


# ## [4.2] - MERGE - AYUNTAMIENTO

# In[14]:


#pd_final = pd_aire.join(pd_tiempo,lsuffix = "_aire",rsuffix = "_tiempo", on =["CODIGO_CORTO"], how = "outer", sort = True) 
#No tienen el mismo tipo las columnas de Código corto en ambos datasets

pd_estaciones_ayunt_2 = pd_aire.merge(pd_tiempo, on =['CODIGO_CORTO', 'ESTACION', 'DIRECCION', 'ALTITUD', 'LONGITUD', 'LATITUD'], how = "outer")


# In[15]:


#pd_estaciones_ayunt_2["CODIGO_CORTO"].count()


# ## [5.2] - ELIMINAMOS DUPLICADOS

# In[16]:


pd_estaciones_ayunt_2.duplicated(["CODIGO_CORTO"])
pd_estaciones_ayunt_2 = pd_estaciones_ayunt_2.drop_duplicates(["CODIGO_CORTO"],keep='first')


# In[17]:


#pd_estaciones_ayunt_2["CODIGO_CORTO"].count()


# ## [6.2] - NULL -> 0

# In[18]:


pd_estaciones_ayunt_2 = pd_estaciones_ayunt_2.fillna(0)
pd_estaciones_ayunt_2["MIDE_CLIMA"] = pd_estaciones_ayunt_2["MIDE_CLIMA"].round(1).astype(int)
pd_estaciones_ayunt_2["MIDE_AIRE"] = pd_estaciones_ayunt_2["MIDE_AIRE"].round(1).astype(int)


# ## [7] - ESTACIONES CONTROL METEOROLÓGICO (AEMET)

# In[19]:


configuration = swagger_client.Configuration()
configuration.api_key['api_key'] = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwcm95ZWN0by5vem9uby5jb250YWN0QGdtYWlsLmNvbSIsImp0aSI6ImNlZDZiZWQ2LTUyN2EtNGQ2Yi1iOGMyLWU1YmRlNzk3YzYzZSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNTg2NzE3MTE2LCJ1c2VySWQiOiJjZWQ2YmVkNi01MjdhLTRkNmItYjhjMi1lNWJkZTc5N2M2M2UiLCJyb2xlIjoiIn0.U3b4ELAg-9eJcwgpzr4QgkF-Yj6jb9gw0DOa8sqAwHo'


# In[20]:


api_valores = swagger_client.ValoresClimatologicosApi(swagger_client.ApiClient(configuration))


# In[21]:


try:
    api_response = api_valores.inventario_de_estaciones__valores_climatolgicos_()
    #print(api_response)
except ApiException as e:
    print("Exception: %s\n" % e)


# In[22]:


r = requests.get(api_response.datos)
data = r.content


# In[23]:


def data_to_sparkdf(data):
    #Encoding "ISO-8859"
    data_v = data.decode(encoding ='ISO-8859-15')
    data_v0 = data_v
    # Clean the data
    # Step 0 
    for i in range(20):
        if(data_v0[i]=='{'):
            data_v0 = data_v0[i:]
    for i in range(20):
        if(data_v0[-i]=='}'):
            data_v0 = data_v0[:-i+1]
    # Step 1     
    data_v1 = data_v0
    data_v1 = data_v1.replace("\n", "")
    
    # Step 2
    data_v2 = data_v1.replace("},","}},")
    
    # Step 3
    patron =['\s\s','\s"','"\s','\s{']
    replace = [' ','"','"','{']
    
    data_v3 = data_v2
    for i in range(len(patron)):
        data_v3 = reg.sub(patron[i],replace[i],data_v3)

    # Step 4
    data_v4 = data_v3.replace("\",\"","\";\"")
    
    # Step 5
    data_cleaned = data_v4.split("},")

    # String to List of dictionaries
    diccionarios = []
    for fila in data_cleaned:
        #print(fila)
        keys = []
        values = []
        for pareja in fila[1:-1].split(';'):
            #print("Pareja= ",pareja)
            elems =pareja.split(':')
            #print("Elementos= ",elems)
            keys.append(elems[0][1:-1])
            values.append(elems[1][1:-1])
        diccionarios.append(dict(zip(keys,values)))

    # Schema for the new DF
    data_schema = [StructField('latitud',StringType(), True), #Tercer argumento = nullable
                   StructField('provincia', StringType(), True),
                   StructField('altitud', StringType(), True),
                   StructField('indicativo', StringType(), True),
                   StructField('nombre', StringType(), True),
                   StructField('indsinop', StringType(), True),
                   StructField('longitud', StringType(), True)                    
                  ]
    # Create and return the new DF
    return spark.createDataFrame(diccionarios,schema = StructType(data_schema))  


# In[121]:


df = data_to_sparkdf(data)


# In[122]:


pd_estaciones_aemet = df.filter(df["PROVINCIA"]=="MADRID").orderBy("indsinop").toPandas()


# In[123]:


def degrees_to_decimal(elem):
    elem = reg.sub('N|E','1',elem)
    elem = reg.sub('S|W','-1',elem)
    return (float(elem[6:])* (float(elem[0:2]) + float(elem[2:4])/60 + float(elem[4:6])/3600))


# In[124]:


pd_estaciones_aemet["DIRECCION"] = pd_estaciones_aemet["nombre"]+ "-" + pd_estaciones_aemet["provincia"]
#pd_mad["MIDE_CLIMA_AEMET"] = 1
pd_estaciones_aemet.insert(7,"MIDE_CLIMA_AEMET",1)
pd_estaciones_aemet["LONGITUD"] = [degrees_to_decimal(elem) for elem in pd_estaciones_aemet["longitud"]]
pd_estaciones_aemet["LATITUD"] = [degrees_to_decimal(elem) for elem in pd_estaciones_aemet["latitud"]]


# In[125]:


pd_estaciones_aemet = pd_estaciones_aemet[["indsinop","nombre","DIRECCION","altitud","LONGITUD","LATITUD","MIDE_CLIMA_AEMET"]]
pd_estaciones_aemet = pd_estaciones_aemet.rename(columns = {"indsinop":"CODIGO_CORTO",
                                  "nombre": "ESTACION",
                                  "altitud": "ALTITUD"})


# In[126]:


pd_estaciones_aemet["CODIGO_CORTO"] = pd_estaciones_aemet["CODIGO_CORTO"].astype(str).astype(int)
pd_estaciones_aemet["ALTITUD"] = pd_estaciones_aemet["ALTITUD"].astype(str).astype(int)


# In[127]:


#pd_estaciones_aemet


# ## [!] -  Descartar las estaciones que estén fuera del area de madrid ciudad

# In[129]:


import math
def haversine(p0,p1):

    lat1, lon1 = round(float(p0[0]),6),round(float(p0[1]),6)
    lat2, lon2 = round(float(p1[0]),6),round(float(p1[1]),6)
    
    rad=math.pi/180
    dlat=lat2-lat1
    dlon=lon2-lon1
    R=6372.795477598
    a=(math.sin(rad*dlat/2))**2 + math.cos(rad*lat1)*math.cos(rad*lat2)*(math.sin(rad*dlon/2))**2
    distancia=2*R*math.asin(math.sqrt(a))
    #Devuelve distancia en grados
    return distancia


# plaza_españa = df_estaciones_ayunt.filter(df_estaciones_ayunt["CODIGO_CORTO"]==4).select("LATITUD","LONGITUD")
# plaza_españa = plaza_españa.collect()[0]
# centro_de_madrid = [40.4165,-3.702561]
# 
# plaza_españa_r=[round(plaza_españa[0],6),round(plaza_españa[1],6)]
# 
# print("Plaza españa= ", plaza_españa_r[0] , "- ",  plaza_españa_r[1])
# print("Madrid= ", centro_de_madrid)
# 
# haversine(centro_de_madrid,plaza_españa_r)

# In[130]:


# Radio madrid ciudad 15km (PARDO)
pd_estaciones_aemet["LAT_LONG"]= round(pd_estaciones_aemet["LATITUD"],6).astype(str) +','+ round(pd_estaciones_aemet["LONGITUD"],6).astype(str)
centro_de_madrid = [40.4165,-3.702561]
pd_estaciones_aemet["MADRID_CIUDAD"] = [1 if haversine(x.split(','),centro_de_madrid) <= 15 else 0 for x in (pd_estaciones_aemet["LAT_LONG"])]
#pd_estaciones_aemet["MADRID_CIUDAD"] = [1 if haversine(x,centro_de_madrid) <= 15 else 0 for x in (pd_estaciones_aemet["LAT_LONG"])]
#pd_estaciones_aemet["DISTC_MADRID_CIUDAD"] = [haversine(x,centro_de_madrid) for x in (pd_estaciones_aemet["LAT_LONG"])]


# In[131]:


df_estaciones_aemet= spark.createDataFrame(pd_estaciones_aemet)
df_estaciones_aemet= df_estaciones_aemet.filter(df_estaciones_aemet["MADRID_CIUDAD"]== 1).select('CODIGO_CORTO', 'ESTACION', 'DIRECCION', 'ALTITUD', 'LONGITUD','LATITUD', 'MIDE_CLIMA_AEMET')


# In[132]:


pd_estaciones_aemet = df_estaciones_aemet.toPandas()


# In[133]:


pd_estaciones_aemet.head(10)


# ##  [8.1] - MERGE AYUNT-AEMET

# In[134]:


#pd_estaciones_ayunt.dtypes


# In[135]:


#pd_estaciones_aemet.dtypes


# In[136]:


pd_estaciones_1 = pd_estaciones_ayunt_1.merge(pd_estaciones_aemet,how='outer')
pd_estaciones_1 = pd_estaciones_1.fillna(0)


# ##  [8.2] - MERGE AYUNT-AEMET

# In[137]:


pd_estaciones_2 = pd_estaciones_ayunt_2.merge(pd_estaciones_aemet,how='outer')
pd_estaciones_2 = pd_estaciones_2.fillna(0)


# ## [!!.1] - COLUMNA MIDE_CLIMA _FINAL = MIDE_CLIMA_AEMET

# In[138]:


pd_estaciones_1["MIDE_CLIMA_FINAL"] = pd_estaciones_1["MIDE_CLIMA_AEMET"]


# In[139]:


#pd_estaciones.head(5)


# ## [!!.2] - COLUMNA MIDE_CLIMA _FINAL = MIDE_CLIMA (AYUNT) + MIDE_CLIMA_AEMET

# In[140]:


pd_estaciones_2["MIDE_CLIMA_FINAL"] = pd_estaciones_2["MIDE_CLIMA"] + pd_estaciones_2["MIDE_CLIMA_AEMET"]


# ## [FUNC] - CLUSTERING

# In[182]:


def lista_diccionarios_estaciones(pd):
    return [dict(CODIGO = codigo,LATITUD = latitud,LONGITUD = longitud) 
                        for codigo,latitud,longitud 
                        in zip(pd["CODIGO_CORTO"].values,pd["LATITUD"].values,pd["LONGITUD"].values)] 


# In[200]:


def objetivo_mas_cercano(objetivos,punto):
    objetivo_mas_cercano = None
    menor_distancia = 9999999
    for objetivo in objetivos:
        distancia = haversine([objetivo["LATITUD"],objetivo["LONGITUD"]],
                              [punto["LATITUD"],punto["LONGITUD"]])
        #print(objetivo["CODIGO"], " - ",punto["CODIGO"], "DISTANCIA = ", distancia )
        if(distancia < menor_distancia):
            objetivo_mas_cercano = objetivo["CODIGO"]
            menor_distancia = distancia
    #print("="*100)
    return objetivo_mas_cercano


# In[201]:


def agrupamiento(df):
    #Sacamos las estaciones de CLIMA
    pd = df.filter(df["MIDE_CLIMA_FINAL"] > 0).toPandas()
    #Sacar un diccionario de : Estacion,latitud,longitud
    estaciones_clima = lista_diccionarios_estaciones(pd)
    #Para todas las estaciones, le asignamos la de clima mas cercana
    pd = df.toPandas()
    pd["COD_CLIMA"] = [objetivo_mas_cercano(estaciones_clima,estacion) 
                   for estacion in lista_diccionarios_estaciones(pd)]
                   
    return pd


# ## [9.1] - CLUSTERING

# In[202]:


df_estaciones_1 = spark.createDataFrame(pd_estaciones_1)


# In[203]:


pd_1 = agrupamiento(df_estaciones_1)


# In[209]:


pd_estaciones_14_18 = pd_1[["CODIGO_CORTO","ESTACION","LATITUD", "LONGITUD","COD_CLIMA","MIDE_AIRE","MIDE_CLIMA_AEMET","MIDE_CLIMA_FINAL"]].sort_values(by="CODIGO_CORTO")
pd_estaciones_14_18 = pd_estaciones_14_18.rename(columns={"COD_CLIMA":"COD_CLIMA_14"})
#pd_estaciones_14_18


# ## [9.2] - CLUSTERING

# In[205]:


df_estaciones_2 = spark.createDataFrame(pd_estaciones_2)


# In[206]:


pd_2 = agrupamiento(df_estaciones_2)


# In[208]:


pd_estaciones_19_NOW = pd_2[["CODIGO_CORTO","ESTACION","LATITUD", "LONGITUD","COD_CLIMA","MIDE_AIRE","MIDE_CLIMA_AEMET","MIDE_CLIMA_FINAL"]].sort_values(by="CODIGO_CORTO")
pd_estaciones_19_NOW = pd_estaciones_19_NOW.rename(columns={"COD_CLIMA":"COD_CLIMA_19"})
#pd_estaciones_19_NOW


# ## [10] - UNIMOS DATASETS

# In[220]:


pd_estaciones = pd_estaciones_14_18.merge(pd_estaciones_19_NOW,
                                          on=["CODIGO_CORTO","ESTACION",
                                              "LATITUD", "LONGITUD"],
                                          how='outer')
pd_estaciones.sort_values(by="CODIGO_CORTO")
pd_estaciones = pd_estaciones[['CODIGO_CORTO', 'ESTACION', 'LATITUD', 
                               'LONGITUD', 'COD_CLIMA_14','COD_CLIMA_19',
                               'MIDE_AIRE_y', 'MIDE_CLIMA_AEMET_y','MIDE_CLIMA_FINAL_y']]
pd_estaciones =pd_estaciones.rename(columns={"MIDE_AIRE_y":"MIDE_AIRE",
                              "MIDE_CLIMA_AEMET_y":"MIDE_CLIMA_AEMET",
                              "MIDE_CLIMA_FINAL_y":"MIDE_CLIMA_FINAL"                            
                             })
pd_estaciones = pd_estaciones.fillna(-1)


# ## [11] - EXPORTAMOS

# In[221]:


pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones.csv")

