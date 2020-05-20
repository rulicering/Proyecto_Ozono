#!/usr/bin/env python
# coding: utf-8

# # [o3] - Proyecto Ozono -ETL_Estaciones- v8

# # [INFO]
# 
#     [--1] -> CLIMA AEMET (2014-2018) |  [--2] -> CLIMA AEMET +CLIMA AYUNT (2019-NOW)
# 
#     Agrupar AEMET: AIRE + CLIMA AEMET -> Distancia mínima
#     
#     Agrupar TODAS: AIRE + CLIMA ALL -> Min Distancia x Magnitud
#     

#     0. Inicializacion
#     1. AYUNT
#         1.0 ESTACIONES AIRE AYUNT
#         1.1 ESTACIONES CLIMA AYUNT
#         1.2 AYUNTAMIENTO = AIRE + CLIMA
#         1.3 Types
#     2. AEMET
#         2.0 ESTACIONES CLIMA AEMET
#             2.0.1 Descartar ESTACIONES fuera del area de Madrid ciudad (15 KM)
#         2.1 ESTACIONES CLIMA AEMET HOY
#             2.1.0 [FUNCION] -  Formateo datos
#             2.1.1 [FUNCIONES] - Request datos
#             2.1.2 QUÉ ESTACIONES MIDEN DATOS ACTUALES
#             2.1.3 DATOS      
#             2.1.4 Tipos
#     3. Merge AYUNT + AEMET
#     4. Columna MIDE_CLIMA_FINAL = MIDE_CLIMA_AYUNT + MIDE_CLIMA_AEMET
#     5. AGRUPAMIENTO AIRE <-CLIMA
#         [FUNCIONES] - CLUSTERING
#         5 --1 ESTACION AEMET + CERCANA
#         5 --2 X MAGNITUD -> ESTACIÓN CLIMA + CERCANA
#             5 --2.0 DIARO(AYUNT+AEMET)
#             5 --2.1 DIA ACTUAL (AEMET)
#     6. Nulos -> "-1!
#     7. Columna UTILIZADA  -> (True|False)
#         7.0 U_AEMET
#         7.1 U_AEMET_HOY
#         7.2 U_TODAS
#     8. Exportamos
#         8.1 Diaria
#         8.2 Mensual
#         8.3 Total      

# # [0] - Inicialización

# In[44]:


from __future__ import print_function
import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
import pandas as pd
pd.options.mode.chained_assignment = None
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pyspark.sql import functions as F
import math

#AEMET
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint
import datetime
import requests,json
import re as reg


# In[45]:


spark = SparkSession.builder.appName('estaciones').getOrCreate()


# # [1] - AYUNT

# ## [1.0] - ESTACIONES AIRE AYUNT

# In[46]:


pd_aire_excel = pd.read_excel("https://datos.madrid.es/egob/catalogo/212629-0-estaciones-control-aire.xls")


# In[47]:


pd_aire = pd_aire_excel[['CODIGO_CORTO', 'ESTACION', 'DIRECCION', 'ALTITUD','LONGITUD', 'LATITUD']]
pd_aire.insert(6,"MIDE_AIRE",1)


# In[48]:


pd_aire["CODIGO_CORTO"] = pd_aire[["CODIGO_CORTO"]].astype(str)


# In[49]:


print("Número de estaciones medición calidad del aire: %d" % pd_aire["ESTACION"].count())

#for elem in pd_aire[["CODIGO_CORTO", "ESTACION"]].values.tolist():
#    print(elem)


# ## [1.1] -  ESTACIONES CLIMA AYUNT

# In[50]:


pd_clima_excel = pd.read_excel("https://datos.madrid.es/egob/catalogo/300360-0-meteorologicos-estaciones.xls")


# In[51]:


pd_clima = pd_clima_excel[['CÓDIGO_CORTO', 'ESTACIÓN', 'DIRECCION', 'ALTITUD','LONGITUD', 'LATITUD']]
pd_clima.insert(6,"MIDE_CLIMA",1)


# In[52]:


#Modificamos los nombres de las columnas -> sin tildes para que cuadre luego al hacer el join
pd_clima = pd_clima.rename(columns = {"CÓDIGO_CORTO" : "CODIGO_CORTO",'ESTACIÓN': 'ESTACION', 'DIRECCIÓN':'DIRECCION'})


# In[53]:


pd_clima["CODIGO_CORTO"] = pd_clima[["CODIGO_CORTO"]].astype(str)


# In[54]:


print("Número de estaciones medición clima ayuntamiento: %d" % pd_clima["ESTACION"].count())

#for elem in pd_clima[["CODIGO_CORTO", "ESTACION"]].values.tolist():
#    print(elem)


# ## [1.2] - AYUNTAMIENTO = AIRE + CLIMA

# In[55]:


#No se puede hacer el merge por mas campos porque no son exactamente iguales por lo que hay que filtrar
pd_estaciones_ayunt = pd_aire.merge(pd_clima, on =['CODIGO_CORTO'], how = "outer")

#Cambiamos nulos -> 0s en columnas "MIDE_AIRE" y "MIDE_CLIMA"
pd_estaciones_ayunt = pd_estaciones_ayunt.fillna({"MIDE_AIRE":0,"MIDE_CLIMA":0})
#Nulos a "Nulo" para el resto
pd_estaciones_ayunt = pd_estaciones_ayunt.fillna("Nulo")


# In[56]:


#Creamos columnas nuevas mergeando las _X y _y 
columnas = pd_estaciones_ayunt.columns.to_list()
columnas_x = list(filter(None,[columna if "_x" in columna else None 
                               for columna in columnas]))
columnas_y = list(filter(None,[columna if "_y" in columna else None 
                               for columna in columnas]))


# In[57]:


#Creamos las nuevas columnas mergeando 
for x,y in zip(columnas_x,columnas_y):
    pd_estaciones_ayunt[x[:-2]] = [b if(a == "Nulo") else a 
                                     for a,b in zip(pd_estaciones_ayunt[x],
                                                    pd_estaciones_ayunt[y])]


# In[58]:


#Quitamos las columnas duplicadas por el merge _x y _y
columnas = pd_estaciones_ayunt.columns.to_list()
columnas_a_coger = list(filter(None,
                               [None if "_x" in columna or "_y" in columna else columna 
                                for columna in columnas]))
pd_estaciones_ayunt = pd_estaciones_ayunt[columnas_a_coger]


# In[59]:


pd_estaciones_ayunt["CODIGO_CORTO"] = pd_estaciones_ayunt["CODIGO_CORTO"].astype(str)


# In[60]:


pd_estaciones_ayunt = pd_estaciones_ayunt[['CODIGO_CORTO', 'ESTACION', 'DIRECCION',
       'ALTITUD', 'LONGITUD', 'LATITUD', 'MIDE_AIRE', 'MIDE_CLIMA']]


# In[61]:


print("TOTAL ESTACIONES AYUNT: ", pd_estaciones_ayunt[["CODIGO_CORTO"]].count())


# ## [1.3] - Types

# In[62]:


pd_estaciones_ayunt["MIDE_CLIMA"] = pd_estaciones_ayunt["MIDE_CLIMA"].round(1).astype(int)
pd_estaciones_ayunt["MIDE_AIRE"] = pd_estaciones_ayunt["MIDE_AIRE"].round(1).astype(int)


# # [2] - AEMET

# ## [2.0] - ESTACIONES CLIMA AEMET

# In[63]:


configuration = swagger_client.Configuration()
configuration.api_key['api_key'] = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwcm95ZWN0by5vem9uby5jb250YWN0QGdtYWlsLmNvbSIsImp0aSI6ImNlZDZiZWQ2LTUyN2EtNGQ2Yi1iOGMyLWU1YmRlNzk3YzYzZSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNTg2NzE3MTE2LCJ1c2VySWQiOiJjZWQ2YmVkNi01MjdhLTRkNmItYjhjMi1lNWJkZTc5N2M2M2UiLCJyb2xlIjoiIn0.U3b4ELAg-9eJcwgpzr4QgkF-Yj6jb9gw0DOa8sqAwHo'


# In[64]:


api_valores = swagger_client.ValoresClimatologicosApi(swagger_client.ApiClient(configuration))


# In[65]:


try:
    api_response = api_valores.inventario_de_estaciones__valores_climatolgicos_()
    #print(api_response)
except ApiException as e:
    print("Exception: %s\n" % e)


# In[66]:


r = requests.get(api_response.datos)
data = r.content


# In[67]:


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


# In[68]:


df = data_to_sparkdf(data)


# In[69]:


pd_estaciones_aemet = df.filter(df["PROVINCIA"]=="MADRID").orderBy("indicativo").toPandas()


# In[70]:


def degrees_to_decimal(elem):
    elem = reg.sub('N|E','1',elem)
    elem = reg.sub('S|W','-1',elem)
    return (float(elem[6:])* (float(elem[0:2]) + float(elem[2:4])/60 + float(elem[4:6])/3600))


# In[71]:


pd_estaciones_aemet["DIRECCION"] = pd_estaciones_aemet["nombre"]+ "-" + pd_estaciones_aemet["provincia"]
#pd_mad["MIDE_CLIMA_AEMET"] = 1
pd_estaciones_aemet.insert(7,"MIDE_CLIMA_AEMET",1)
pd_estaciones_aemet["LONGITUD"] = [degrees_to_decimal(elem) for elem in pd_estaciones_aemet["longitud"]]
pd_estaciones_aemet["LATITUD"] = [degrees_to_decimal(elem) for elem in pd_estaciones_aemet["latitud"]]


# In[72]:


pd_estaciones_aemet = pd_estaciones_aemet[["indicativo","nombre","DIRECCION","altitud","LONGITUD","LATITUD","MIDE_CLIMA_AEMET"]]
pd_estaciones_aemet = pd_estaciones_aemet.rename(columns = {"indicativo":"CODIGO_CORTO",
                                  "nombre": "ESTACION",
                                  "altitud": "ALTITUD"})


# In[73]:


pd_estaciones_aemet["CODIGO_CORTO"] = pd_estaciones_aemet["CODIGO_CORTO"].astype(str)
pd_estaciones_aemet["ALTITUD"] = pd_estaciones_aemet["ALTITUD"].astype(str).astype(int)


# In[74]:


#pd_estaciones_aemet


# ### [2.0.0] -  Descartar ESTACIONES fuera del area de Madrid ciudad (15 KM)

# In[75]:


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


# In[76]:


# Radio madrid ciudad 15km (PARDO)
pd_estaciones_aemet["LAT_LONG"]= round(pd_estaciones_aemet["LATITUD"],6).astype(str) +','+ round(pd_estaciones_aemet["LONGITUD"],6).astype(str)
centro_de_madrid = [40.4165,-3.702561]
pd_estaciones_aemet["MADRID_CIUDAD"] = [1 if haversine(x.split(','),centro_de_madrid) <= 15 else 0 for x in (pd_estaciones_aemet["LAT_LONG"])]


# In[77]:


pd_estaciones_aemet = pd_estaciones_aemet[pd_estaciones_aemet["MADRID_CIUDAD"]==1]
pd_estaciones_aemet = pd_estaciones_aemet[['CODIGO_CORTO', 'ESTACION', 'DIRECCION', 'ALTITUD', 'LONGITUD','LATITUD', 'MIDE_CLIMA_AEMET']]


# In[78]:


print("TOTAL ESTACIONES AEMET: ", pd_estaciones_aemet[["CODIGO_CORTO"]].count())
#pd_estaciones_aemet.head(10)


# ## [2.1] - ESTACIONES CLIMA AEMET HOY

# In[79]:


api_observacion = swagger_client.ObservacionConvencionalApi(swagger_client.ApiClient(configuration))


# ### [2.1.0] - [FUNCION] -  Formateo datos

# In[80]:


def data_to_sparkdf_HOY(data):
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
    data_schema = [StructField('idema',StringType(), True), #Tercer argumento = nullable
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


# ### [2.1.1]  [FUNCIONES] - Request datos

# In[81]:


def req_hoy_to_df(codigo):
    print("CODIGO: ", codigo)
    api_response = api_observacion.datos_de_observacin__tiempo_actual_1(codigo)
    pprint(api_response)
    r = requests.get(api_response.datos)
    data = r.content
    df_aemet = data_to_sparkdf_HOY(data)
    print("OK")
    
    return df_aemet.select('idema','fint','prec','pres','tamax','tamin','dv','vv')
    #return df_aemet.select('idema','prec','pres','tamax','tamin','dv','vv')


# In[82]:


def datos_aemet_hoy(codigos_estaciones):
    lista_df =[]
    for codigo in codigos_estaciones:
        lista_df.append(req_hoy_to_df(codigo))
    #Unimos
    df_r = lista_df[0]
    for i in range(1,len(lista_df)):
        df_r = df_r.union(lista_df[i])
    return df_r


# ### [2.1.2] -QUÉ ESTACIONES MIDEN DATOS ACTUALES

# In[83]:


#Todos los datos actuales de todas las estaciones de la aemet
api_response = api_observacion.datos_de_observacin__tiempo_actual_()
pprint(api_response)

r = requests.get(api_response.datos)
data = r.content
df_aemet = data_to_sparkdf_HOY(data)

cod_estaciones_aemet_madrid = list(pd_estaciones_aemet["CODIGO_CORTO"].values)
cod_todas_estaciones_aemet_hoy = [elem[0] for elem in df_aemet.select("idema").distinct().collect()]

cod_estaciones_aemet_hoy = set(list([elem if elem in cod_estaciones_aemet_madrid else "-1"
                                 for elem in cod_todas_estaciones_aemet_hoy]))
cod_estaciones_aemet_hoy.remove("-1")

cod_estaciones_aemet_hoy


# ###  [2.1.3] -  DATOS

# In[84]:


df_aemet_hoy = datos_aemet_hoy(cod_estaciones_aemet_hoy)


# ### [2.1.4] - Tipos

# In[85]:


magnitudes_aemet = df_aemet_hoy.columns[2:]
#magnitudes_aemet


# In[86]:


for columna in magnitudes_aemet:
    df_aemet_hoy = df_aemet_hoy.withColumn(columna,df_aemet_hoy[columna].cast(FloatType()))


# # [3] - Merge AYUNT + AEMET

# In[87]:


pd_estaciones = pd_estaciones_ayunt.merge(pd_estaciones_aemet,how='outer')
pd_estaciones = pd_estaciones.fillna(0)


# # [4] - Columna MIDE_CLIMA_FINAL = 
# ### MIDE_CLIMA (AYUNT) + MIDE_CLIMA_AEMET

# In[88]:


pd_estaciones["MIDE_CLIMA_FINAL"] = pd_estaciones["MIDE_CLIMA"] + pd_estaciones["MIDE_CLIMA_AEMET"]


# # [5] - AGRUPAMIENTO AIRE <-CLIMA

# ## [FUNCIONES] - CLUSTERING

# In[89]:


def lista_diccionarios_estaciones(pd):
    return [dict(CODIGO = codigo,LATITUD = latitud,LONGITUD = longitud) 
                        for codigo,latitud,longitud 
                        in zip(pd["CODIGO_CORTO"].values,pd["LATITUD"].values,pd["LONGITUD"].values)] 


# In[90]:


"""
PARAMETROS:
objetivos : lista de diccionarios->(CODIGO,LAT,LONG),
                        de las estaciones objetivo.
                        
estacion: diccionario de la estación actual -> (CODIGO,LAT,LONG)
"""
def objetivo_mas_cercano(objetivos,punto):
    objetivo_mas_cercano = None
    menor_distancia = 9999999
    for objetivo in objetivos:
        if(objetivo["CODIGO"] == punto["CODIGO"]):
            objetivo_mas_cercano = punto["CODIGO"]
            break
        else:
            distancia = haversine([objetivo["LATITUD"],objetivo["LONGITUD"]],
                                  [punto["LATITUD"],punto["LONGITUD"]])
            #print(objetivo["CODIGO"], " - ",punto["CODIGO"], "DISTANCIA = ", distancia )
            if(distancia < menor_distancia):
                objetivo_mas_cercano = objetivo["CODIGO"]
                menor_distancia = distancia
    #print("ESTACION: ", punto["CODIGO"],"más cercana= " ,objetivo_mas_cercano, " a ",menor_distancia)
    return objetivo_mas_cercano


# In[91]:


def agrupamiento_AEMET(pd):
    #Sacamos las estaciones de CLIMA
    pd_aemet = pd[pd["MIDE_CLIMA_AEMET"]>0]

    #Sacar un diccionario de : Estacion,latitud,longitud
    estaciones_clima = lista_diccionarios_estaciones(pd_aemet)
    
    #Para todas las estaciones, le asignamos la de clima mas cercana
    pd["E_AEMET"] = [objetivo_mas_cercano(estaciones_clima,estacion) 
                   for estacion in lista_diccionarios_estaciones(pd)]
    return pd


# In[92]:


"""
FUNCIONAMIENTO:
Si la estacion es de AIRE:

    -> Devuelve la estación de CLIMA más cercana que lea esa magnitud

PARAMETROS:
estaciones_magnitud: lista de diccionarios->(CODIGO,LAT,LONG),
                        de las estaciones que miden esa magnitud.

estaciones_aire: lista de las estaciones que miden aire -> (CODIGO_CORTO)

estacion: diccionario de la estación actual -> (CODIGO,LAT,LONG)
"""

def clima_mas_cercana(estaciones_magnitud,estaciones_aire,estacion):
    if(estacion["CODIGO"] in estaciones_aire):
        return str(objetivo_mas_cercano(estaciones_magnitud,estacion))
    else:
        return None;


# In[93]:


def agrupamiento_xmagnitud(pd,aemet= None,ayunt = None):
    
    #DIARIO
    if(aemet is not None):
        magnitudes = aemet.columns[1:]
    elif(ayunt is not None):
        magnitudes = ayunt.columns[1:]
    else: 
        raise Exception("[FUNC] agrupamiento_xmagnitud: Ambos datasets no pueden ser nulos")
    
    #Diccionario: Magnitud:[Lista de estaciones que la leen]
    estacionesxmagnitud = {}
    for magnitud in magnitudes:
        estacionesxmagnitud.update({magnitud:[]})
        #Para cuando solo se utilicen las estaciones de la aemet
        if(ayunt is not None): 
            for est_ayunt in (ayunt[ayunt[magnitud]=="X"]["CODIGO_CORTO"].values): 
                estacionesxmagnitud[magnitud].append(str(est_ayunt))
        if(aemet is not None):
            for est_aemet in (aemet[aemet[magnitud]=="X"]["CODIGO_CORTO"].values): 
                estacionesxmagnitud[magnitud].append(str(est_aemet))    
    
    #Lista de estaciones que miden aire
    l_estaciones_aire = [elem for elem in pd[pd["MIDE_AIRE"]>0]["CODIGO_CORTO"].values]

     #Todas las estaciones
    pd = pd.sort_values(by ="CODIGO_CORTO")
    estaciones = lista_diccionarios_estaciones(pd)
    
    for magnitud in magnitudes :

        #Estaciones que leen esa magnitud
        pd_magnitud = pd[pd["CODIGO_CORTO"].isin(estacionesxmagnitud[magnitud])]
        estaciones_magnitud = lista_diccionarios_estaciones(pd_magnitud)
          
        pd['E_%s' % magnitud] = [clima_mas_cercana(estaciones_magnitud,l_estaciones_aire,estacion) 
                        for estacion in estaciones]
        
        """
        estaciones_clima_utilizadas = set(pd['E_%s' % magnitud].values)
        pd['M_%s' % magnitud] = ["X" if estacion["CODIGO"] in estaciones_clima_utilizadas 
                                     else "-"
                                 for estacion in estaciones]
        """
    return pd


# ## [5 -- 1] - ESTACION AEMET + CERCANA

# In[94]:


pd_estaciones = agrupamiento_AEMET(pd_estaciones)


# In[95]:


pd_estaciones = pd_estaciones[["CODIGO_CORTO","ESTACION","LATITUD", "LONGITUD","E_AEMET","MIDE_AIRE","MIDE_CLIMA","MIDE_CLIMA_AEMET","MIDE_CLIMA_FINAL"]]
#pd_estaciones.head(50)


# ## [5 --2] - X MAGNITUD -> ESTACIÓN CLIMA + CERCANA

# ### [5 --2.0] - DIARIO ( AYUNT + AEMET)

#     Magnitudes que las estaciones de clima miden:
# 
#     AEMET: (TODAS MIDEN ESTOS DATOS)
#         DIRECCION DEL VIENTO
#         VELOCIDAD MEDIA DEL VIENTO
#         PRECIPITACIONES
#         PRESION(MAX Y MIN)
#         TEMPERATURA(MAX Y MIN)
#         SOL (INSOLACION EN HORAS)
# 
#     AYUNTAMIENTO: (NO TODAS MIDEN ESTOS DATOS)
#         81 - VELOCIDAD VIENTO   
#         82 - DIR. DE VIENTO       
#         83 - TEMPERATURA
#         86 - HUMEDAD RELATIVA
#         87 - PRESION BARIOMETRICA
#         88 - RADIACION SOLAR
#         89 - PRECIPITACIÓN
# 
#     NOS QUEDAMOS CON EL INNER JOIN:
#         81 - VELOCIDAD VIENTO   
#         82 - DIR. DE VIENTO       
#         83 - TEMPERATURA
#         87 - PRESION BARIOMETRICA
#         89 - PRECIPITACIÓN
# 

# In[96]:


#DF ESTACIONES AYUNTAMIENTO + MAGNITUDES LEIDAS
magnitudes_estaciones_ayunt = pd_clima_excel[['CÓDIGO_CORTO','VV (81)', 'DV (82)', 'T (83)','PB (87)', 'P (89)']]
magnitudes_estaciones_ayunt = magnitudes_estaciones_ayunt.rename(columns={'CÓDIGO_CORTO':'CODIGO_CORTO',
                                                                         'VV (81)':"81",
                                                                         'DV (82)':"82",
                                                                         'T (83)':"83",
                                                                         'PB (87)':"87",
                                                                         'P (89)':"89"
                                                                        })
ayunt = magnitudes_estaciones_ayunt.fillna("0")


# In[97]:


#DF ESTACIONES AEMET + MAGNITUDES LEIDAS
magnitudes_estaciones_aemet = pd_estaciones[pd_estaciones["MIDE_CLIMA_AEMET"]>0][["CODIGO_CORTO"]]

magnitudes_estaciones_aemet["81"] = "X"
magnitudes_estaciones_aemet["82"] = "X"
magnitudes_estaciones_aemet["83"] = "X"
magnitudes_estaciones_aemet["87"] = "X"
magnitudes_estaciones_aemet["89"] = "X"

aemet = magnitudes_estaciones_aemet


# In[98]:


pd_estaciones = agrupamiento_xmagnitud(pd_estaciones,aemet,ayunt)


# ### [5 --2.1] - DIA ACTUAL (AEMET)

# In[113]:


#magnitudes_estaciones_aemet_hoy
aux = df_aemet_hoy.withColumn("DIA",df_aemet_hoy["fint"][9:2])
aux = aux.filter(aux["DIA"]=='%02d' % datetime.date.today().day)

aux = aux.groupBy("idema").mean().toPandas()

aux = aux.rename(columns={'idema':'CODIGO_CORTO','avg(vv)':"AEMET_HOY_81",'avg(dv)':"AEMET_HOY_82",
                          'avg(tamax)':"AEMET_HOY_83_MAX",'avg(tamin)':"AEMET_HOY_83_MIN",
                          'avg(pres)':"AEMET_HOY_87",'avg(prec)':"AEMET_HOY_89"})
columnas = aux.columns[1:]
for columna in columnas:
    aux[columna] = ["X" if pd.notna(elem) else "-" for elem in aux[columna]]

#TEMPERATURA = TEMPERTURA MAX & MIN
aux["AEMET_HOY_83"] =["X" if (tmax == "X") & (tmin == "X") else "-"
                  for tmax,tmin in zip(aux["AEMET_HOY_83_MAX"].values,aux["AEMET_HOY_83_MIN"].values)]

aux = aux.drop(columns= ["AEMET_HOY_83_MIN","AEMET_HOY_83_MAX"])

aemet_hoy = aux
#aemet_hoy


# In[114]:


aemet_hoy


# In[ ]:


pd_estaciones = agrupamiento_xmagnitud(pd_estaciones,aemet_hoy)


# In[ ]:


#pd_estaciones[pd_estaciones["MIDE_AIRE"]>0][["CODIGO_CORTO","ESTACION","LATITUD", "LONGITUD","MIDE_AIRE","MIDE_CLIMA_FINAL",'E_AEMET_HOY_81', 'E_AEMET_HOY_82', 'E_AEMET_HOY_83', 'E_AEMET_HOY_87', 'E_AEMET_HOY_89']]


# # [6] - Nulos -> "-1"

# In[ ]:


pd_estaciones = pd_estaciones.fillna("-1")
#pd_estaciones = pd_estaciones.replace(("None",None),np.nan)


# In[ ]:


print("TOTAL ESTACIONES: ", pd_estaciones[["CODIGO_CORTO"]].count())


# # [7] - Columna UTILIZADA  -> (True|False)

# ##### Esto nos sirve para a la hora de leer datos de clima, solo leer aquellos que vayan a ser utilizados.

# In[ ]:


#Nos quedamos solo con las que miden aire(contaminacion)
pd_estaciones_aire = pd_estaciones[pd_estaciones["MIDE_AIRE"]>0]


# ## [7.0] - U_AEMET

# In[ ]:


#Vemos que estaciones de clima utilizan
u_aemet = set(pd_estaciones_aire["E_AEMET"].values)
#u_aemet


# In[ ]:


# Marcamos como utilizadas
pd_estaciones["U_AEMET"] = [elem in u_aemet for elem in pd_estaciones["CODIGO_CORTO"]]


# ## [7.1] - U_AEMET_HOY

# In[ ]:


magnitudes_hoy = aemet_hoy.columns[1:]
#Lista de listas
#Estaciones de clima utilizadas por cada una de aire para leer cada una de las magnitudes.
l_u_hoy = [pd_estaciones_aire['E_%s' % magnitud].values 
                   for magnitud in magnitudes_hoy]

#Lista de listas -> Lista
u_aemet_hoy = set([estacion for sublist in l_u_hoy for estacion in sublist])


# In[ ]:


pd_estaciones["U_AEMET_HOY"] = [elem in u_aemet_hoy for elem in pd_estaciones["CODIGO_CORTO"]]


# In[ ]:


#u_aemet_hoy


# ## [7.2] - U_TODAS

# In[ ]:


magnitudes = aemet.columns[1:]
#Lista de listas
#Estaciones de clima utilizadas por cada una de aire para leer cada una de las magnitudes.
l_u_todas = [pd_estaciones_aire['E_%s' % magnitud].values 
                   for magnitud in magnitudes]

#Lista de listas -> Lista
u_todas = set([estacion for sublist in l_u_todas for estacion in sublist])


# In[ ]:


pd_estaciones["U_TODAS"] = [elem in u_todas for elem in pd_estaciones["CODIGO_CORTO"]]


# In[ ]:


#pd_estaciones[['CODIGO_CORTO',"ESTACION",'E_AEMET','UTILIZADA_19','MIDE_AIRE','MIDE_CLIMA','MIDE_CLIMA_AEMET']]


# # [8] - Exportamos

# In[ ]:


hoy = datetime.date.today().strftime("%Y-%m-%d")
ayer = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


# ## [8.0] - Diaria

# In[ ]:


#BackUp
pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/BackUp/Estaciones-" + hoy +".csv")


# In[ ]:


pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + hoy +".csv")

#Borrar la de ayer
try:
    os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + ayer + ".csv")
     print("[INFO] - Estaciones-", ayer,".csv --- Removed successfully")
except:
    print("[ERROR] - Estaciones-", ayer,".csv --- Could not been removed")



# Estaciones mes cerrado
if(datetime.date.today().day == 1):
    #Añadir la de este mes
    ult_dia_mes_cerrado = datetime.date.today()- datetime.timedelta(days=1)
    mes_cerrado = str(ult_dia_mes_cerrado.year*100 + ult_dia_mes_cerrado.month)
    pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + mes_cerrado +".csv")
    #BackUp de este mes
    pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/BackUp/Estaciones-" + mes_cerrado +".csv")


    
    #Borrar la del mes anterior
    dia_mes_cerrado_anterior = datetime.date.today() - datetime.timedelta(days=32)
    mes_cerrado_anterior = dia_mes_cerrado_anterior.year*100+ dia_mes_cerrado_anterior.month
    try:
        os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + mes_cerrado_anterior + ".csv")
    except:
        print("[ERROR] - Estaciones-", mes_cerrado_anterior,".csv --- Could not been removed")


# ## [8.2] - Total

#      Solo ejecutar si se quiere recalcular datos de 2014 - NOW mes cerrado
#      !!! CUIDADO PUEDE HABER NUEVAS ESTACIONES Y JODERLO TODO

# In[ ]:


#pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones.csv")

