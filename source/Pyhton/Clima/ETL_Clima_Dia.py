#!/usr/bin/env python
# coding: utf-8

# # [o3] - Proyecto Ozono - ETL_Clima_Dia_v0

# # [INFO]
#     
#        SOLO AEMET PROPORCIONA DATOS ACTUALES DE CLIMA 
#        
#        !! CON 3 HORAS DE RETRASO POR LO QUE LA CARGA DE DATOS DIARIOS SE REALIZA EL DIA SIGUIENTE A LAS 3 AM

#     0. Inicialización
#     1. Datos
#         1.0 Carga fichero Estacione HOY
#         1.1 Lista magnitudes 
#         1.2 ----------------------- AEMET -----------------------
#             1.2.0 Codegen + API
#             1.2.1 [FUNCION] -  Formateo datos
#             1.2.2 [FUNCIONES] - Request datos
#             1.2.3  _______ HOY _______
#                 1.2.3.0 Estaciones 
#                 1.2.3.1 Obtenemos los datos
#             1.2.4 Columnas -> ANO,MES,DIA,FECHA
#             1.2.5 Columnas -> avg(Temp)
#             1.2.6 Rename & Colcar
#             1.2.7  Filtrar datos de hoy
#             1.2.8 Select
#             1.2.9 Tipos
#             1.2.10 ESTACION -> CODIGO_CORTO
#             1.2.11 Valores diarios
#                 1.2.11.0 AVERAGE DIA (Presion,temperatura,Velocidad-Direccion del viento)+SUMA(Precipitaciones)
#             1.2.12 "None" a Nulo
#     2. Export
#             
#             

# # [0] - Inicialización

# In[ ]:


from __future__ import print_function
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint
import datetime
import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
pd.options.mode.chained_assignment = None
import requests
import numpy as np
import re as reg
import json
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType


# In[ ]:


spark = SparkSession.builder.appName('clima_hoy').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# # [1] -  Datos

# In[ ]:


f = open("/home/rulicering/Datos_Proyecto_Ozono/Credenciales/Credenciales.json")
credenciales = json.load(f)
AEMET_API_KEY = credenciales["aemet"]["api_key"]


# ## [1.0] - Carga fichero Estaciones
#     Se ejecuta a las 2 AM del día siguiente, 1 dia de diferencia.

# In[ ]:


ayer = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


# In[ ]:


df_estaciones = spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + ayer +".csv",inferSchema= True, header= True)
#Fuerzo que se ejecute para que luego al filtrar no tenga que volver a leer el csv
df_estaciones = spark.createDataFrame(df_estaciones.toPandas())


# ## [1.1] - Lista magnitudes

# In[ ]:


regex = reg.compile("E_AEMET_HOY")
c_aemet_hoy = [elem for elem in list(filter(regex.search,df_estaciones.columns))]
c_magnitudes_aemet_hoy = [elem[-2:] for elem in list(filter(regex.search,df_estaciones.columns))]
#c_magnitudes_aemet_hoy


# ## [1.2]  ----------------------- AEMET -----------------------

#     81 - VELOCIDAD VIENTO
#     82 - DIR. DE VIENTO
#     83 - TEMPERATURA 
#     87 - PRESION BARIOMETRICA
#     89 - PRECIPITACIÓN    

# ### [1.2.0] - Codegen + API

# In[ ]:


configuration = swagger_client.Configuration()
configuration.api_key['api_key'] = AEMET_API_KEY


# In[ ]:


#api_instance = swagger_client.AvisosCapApi(swagger_client.ApiClient(configuration))
api_observacion = swagger_client.ObservacionConvencionalApi(swagger_client.ApiClient(configuration))


# ### [1.2.1] - [FUNCION] -  Formateo datos

# In[ ]:


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
    data_v2 = data_v1.replace("},","}};")
    
    # Step 3
    patron =['\s\s','\s"','"\s','\s{']
    replace = [' ','"','"','{']
    
    data_v3 = data_v2
    for i in range(len(patron)):
        data_v3 = reg.sub(patron[i],replace[i],data_v3)

    # Step 4
    data_v4 = data_v3.replace(",",";")
 
    #Step 5 
    data_v5 = data_v4.replace("\"", "")
    
    # Step 6
    data_cleaned = data_v5.split("};")


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
            keys.append(elems[0])
            values.append(elems[1])
        diccionarios.append(dict(zip(keys,values)))
        
    # Schema for the new DF
    data_schema = [StructField('idema',StringType(), False), #Tercer argumento = nullable
                   StructField('lon', StringType(), True),
                   StructField('fint', StringType(), True),
                   StructField('prec', StringType(), True),
                   StructField('alt', StringType(), True),
                   StructField('vmax', StringType(), True),
                   StructField('vv', StringType(), True),
                   StructField('dv',StringType(), True), 
                   StructField('lat', StringType(), True),
                   StructField('dmax', StringType(), True),
                   StructField('ubi', StringType(), True),
                   StructField('pres', StringType(), True),
                   StructField('hr',StringType(), True), 
                   StructField('ts', StringType(), True),
                   StructField('pres_nmar', StringType(), True),
                   StructField('tamin', StringType(), True),
                   StructField('ta', StringType(), True),
                   StructField('tamax', StringType(), True),
                   StructField('tpr', StringType(), True),
                   StructField('vis', StringType(), True),
                   StructField('stddv', StringType(), True),
                   StructField('inso', StringType(), True),
                   StructField('rviento', StringType(), True),
                  ]
    # Create and return the new DF
    return spark.createDataFrame(diccionarios,schema = StructType(data_schema))


# ### [1.2.2]  [FUNCIONES] - Request datos

# In[ ]:


def req_hoy_to_df(codigo):
    print("CODIGO: ", codigo)
    try:
        api_response = api_observacion.datos_de_observacin__tiempo_actual_1(codigo)
        pprint(api_response)
    except ApiException as e:
        pprint(api_response)
        print("Exception: %s\n" % e)
    r = requests.get(api_response.datos)
    data = r.content
    df_aemet = data_to_sparkdf(data)
    print("OK")
    
    return df_aemet.select('idema','fint','prec','pres','tamax','tamin','dv','vv')
    
    # Las estaciones del ayunt no tienen datos de insolacion (sol)
    #return df_aemet.select('fecha','indicativo','dir','prec','presMax','presMin','sol','tmax','tmin','velmedia')


# In[ ]:


def datos_aemet_hoy(codigos_estaciones):
    lista_df =[]
    for codigo in codigos_estaciones:
        lista_df.append(req_hoy_to_df(codigo))
    #Unimos
    df = lista_df[0]
    for i in range(1,len(lista_df)):
        df = df.union(lista_df[i])
    return df


# ### [1.2.3]  _______ HOY _______

# #### [1.2.3.0] - Estaciones 

# In[ ]:


df_estaciones_aemet_hoy = df_estaciones.filter(df_estaciones["U_AEMET_HOY"])


# In[ ]:


cod_estaciones_aemet_hoy = [elem[0] for elem in df_estaciones_aemet_hoy.select("CODIGO_CORTO").collect()]


# In[ ]:


cod_estaciones_aemet_hoy


# #### [1.2.3.1] -  Obtenemos los datos

# In[ ]:


df_aemet_hoy = datos_aemet_hoy(cod_estaciones_aemet_hoy)


# In[ ]:


df_aemet = df_aemet_hoy


# ### [1.2.4] -Columnas -> ANO,MES,DIA,FECHA

# In[ ]:


df_aemet = df_aemet.withColumn("ANO",df_aemet["fint"][0:4])
df_aemet = df_aemet.withColumn("MES",df_aemet["fint"][6:2])
df_aemet = df_aemet.withColumn("DIA",df_aemet["fint"][9:2])
df_aemet = df_aemet.withColumn("HORA",df_aemet["fint"][12:2])
df_aemet = df_aemet.withColumn("FECHA",F.concat(df_aemet["fint"][0:4],df_aemet["fint"][6:2],df_aemet["fint"][9:2]))


# ### [1.2.5] - Columna -> avg(Temp)

# In[ ]:


pd_aemet = df_aemet.toPandas()


# In[ ]:


#Cambias comas por puntos
pd_aemet["tamax"]  =  [reg.sub(',','.',str(x)) for x in pd_aemet["tamax"]]
pd_aemet["tamin"]  =  [reg.sub(',','.',str(x)) for x in pd_aemet["tamin"]]


# In[ ]:


def media(vals):
    validos = 0
    nulos = 0
    media = 0
    for i in range(len(vals)):
        if(vals[i] != 'None')or(vals[i] is None):
            validos += 1
            media += float(vals[i])
        else:
            nulos +=1
    if(nulos == len(vals)):
        return None
    else :
        return media/validos


# In[ ]:


pd_aemet["temp"] = [media([pmax,pmin])for pmax,pmin in zip(pd_aemet["tamax"].values, pd_aemet["tamin"].values)]


# ### [1.2.6]- Rename & Colocar

# In[ ]:


pd_aemet =pd_aemet.rename(columns={"idema":"ESTACION",
                                   "vv":"81",                         
                                   "dv":"82",
                                   "temp":"83",
                                   "pres":"87",
                                   "prec":"89",
                             })


# In[ ]:


pd_aemet


# ### [1.2.7] - FIltrar datos de ayer ( Se ejecuta a las 2 AM del dia siguiente)

# In[ ]:


ayer = '%02d' % (datetime.date.today()+datetime.timedelta(days=-1)).day


# In[ ]:


pd_aemet = pd_aemet[pd_aemet["DIA"]== ayer]


# In[ ]:


#pd_aemet


# ### [1.2.8] - Select

# In[ ]:


columnas = ["ESTACION","ANO","MES","DIA","HORA","FECHA"]
for elem in c_magnitudes_aemet_hoy:
    columnas.append(elem) 


# In[ ]:


pd_aemet = pd_aemet[columnas]


# ### [1.2.9] - Tipos

# In[ ]:


for elem in c_magnitudes_aemet_hoy:
    pd_aemet[elem]= pd_aemet[elem].astype(float)


# ### [1.2.10] - ESTACION -> CODIGO_CORTO

# In[ ]:


pd_aemet = pd_aemet.rename(columns={"ESTACION":"CODIGO_CORTO"})


# ### [1.2.11] - Valores Diarios

# ####  [1.2.11.0] - AVERAGE DIA - (Presion,Temperatura,Velocidad del viento y Direccion del viento) + SUMA (Precipitaciones)

# In[ ]:


pd_aemet_media = pd_aemet.groupby(by=["CODIGO_CORTO","ANO","MES","DIA","FECHA"]).agg({'81':'mean',
                                                                         '82':'mean',
                                                                         '83':'mean',
                                                                         '87':'mean',
                                                                         '89':'sum',})


# ### [1.2.12] - "None" a Nulo

# In[ ]:


pd_aemet_media = pd_aemet_media.replace(('None',None),np.nan)


# In[ ]:


pd_final = pd_aemet_media


# # [2] -Export

# In[ ]:


#Los datos de ayer se cargan a las 3 am de hoy. 1 dia de diferencia
nuevo = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
anterior = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")


# In[ ]:


#Versiones
pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/BackUp/Clima-"+ nuevo + ".csv")


# In[ ]:


pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima-"+ nuevo + ".csv")
print("[INFO] - Clima-", hoy ,".csv --- Created successfully")


# In[ ]:


#Borrar la de ayer
try:
    os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima-"+ anterior + ".csv")
     print("[INFO] - Clima-", ayer,".csv --- Removed successfully")
except:
    print("[ERROR] - Clima-", ayer,".csv --- Could not been removed")

