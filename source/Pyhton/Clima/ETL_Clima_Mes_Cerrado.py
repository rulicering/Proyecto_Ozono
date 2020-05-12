#!/usr/bin/env python
# coding: utf-8

# # [o3] - Proyecto Ozono - ETL_Clima_Mes_Cerrado  - v0

# # [INFO]
#     
#        AEMET Y AYUNT -> DATOS A MES CERRADO
#        
#        ¡¡¡COMPRBAR CUANDO ESTÁN DISPONIBLES LOS DATOS DEL MES ANTERIOR!!!
#        
#     
#         

#     0. Inicialización
#     1. Datos
#         1.0 Carga fichero Estaciones - Mes Cerrado
#         1.1 Lista magnitudes 
#         1.2 ----------------------- AEMET -----------------------
#             1.2.0 Codegen + API
#             1.2.1 [FUNCION] -  Formateo datos
#             1.2.2 [FUNCIONES] - Request datos
#             1.2.3 _______ MES_CERRADO _______
#                 1.2.3.0 Estaciones 
#                 1.2.3.1 Fechas
#                 1.2.3.2 Obtenemos los datos
#             1.2.4 Columnas -> ANO,MES,DIA,FECHA
#             1.2.5 Columnas -> avg(Temp), avg(Pres)
#             1.2.6 Rename
#             1.2.7 Select
#             1.2.8 "None" a NULO
#             1.2.9 Types
#         1.3 ----------------------- AYUNTAMIENTO -----------------------
#             1.3.0 Obtenemos los datos
#                 1.3.0.0 Filtramos los datos del ultimo mes cerrado
#             1.3.1 Estaciones
#             1.3.2 Filtar -> ESTACIONES UTILIZADAS
#             1.3.3 Select
#             1.3.4 Formato
#             1.3.5 Select -> [ESTACION,ANO,MES,DIA,FECHA,+MAGNITUDES]
#     2. Union AYUNT + AEMET
#     3. Formato
#         3.1 89-PRECIPITACION == "IP" == Inapreciable -> 0
#         3.2 Rename
#         3.3 Tipos
#         3.4 Nulos -> Media diaria
#     4. Export
#             
#             

# # [0] - Inicialización

# In[1]:


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
import requests
import numpy as np
import re as reg
import json
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType


# In[2]:


spark = SparkSession.builder.appName('recalculo_clima').getOrCreate()


# # [1] -  Datos

# In[3]:


f = open("/home/rulicering/Datos_Proyecto_Ozono/Credenciales/Credenciales.json")
credenciales = json.load(f)
AEMET_API_KEY = credenciales["aemet"]["api_key"]


# ## [1.0] - Carga fichero Estaciones-Mes Cerrado

# In[4]:


ultimo_dia_mes_cerrado = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
mes_cerrado = ultimo_dia_mes_cerrado.year *100 + ultimo_dia_mes_cerrado.month 
ruta = "/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + str(mes_cerrado) + ".csv"


# In[5]:


df_estaciones = spark.read.csv(ruta,inferSchema= True, header= True)
#Fuerzo que se ejecute para que luego al filtrar no tenga que volver a leer el csv
df_estaciones = spark.createDataFrame(df_estaciones.toPandas())


# ## [1.1] - Lista magnitudes

# In[6]:


regex = reg.compile("E_\d\d")
magnitudes = [elem[2:] for elem in list(filter(regex.search,df_estaciones.columns))]
#magnitudes


# ## [1.2]  ----------------------- AEMET -----------------------

#     81 - VELOCIDAD VIENTO
#     82 - DIR. DE VIENTO
#     83 - TEMPERATURA 
#     87 - PRESION BARIOMETRICA
#     89 - PRECIPITACIÓN    

# ### [1.2.0] - Codegen + API

# In[7]:


configuration = swagger_client.Configuration()
configuration.api_key['api_key'] = AEMET_API_KEY


# In[8]:


#api_instance = swagger_client.AvisosCapApi(swagger_client.ApiClient(configuration))
api_observacion = swagger_client.ObservacionConvencionalApi(swagger_client.ApiClient(configuration))
master_api_instance = swagger_client.MaestroApi(swagger_client.ApiClient(configuration))
api_valores = swagger_client.ValoresClimatologicosApi(swagger_client.ApiClient(configuration))


# ### [1.2.1] - [FUNCION] -  Formateo datos

# In[9]:


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
    data_schema = [StructField('altitud',StringType(), True), #Tercer argumento = nullable
                   StructField('dir', StringType(), True),
                   StructField('fecha', StringType(), True),
                   StructField('horaPresMax', StringType(), True),
                   StructField('horaPresMin', StringType(), True),
                   StructField('horaracha', StringType(), True),
                   StructField('horatmax', StringType(), True),
                   StructField('horatmin',StringType(), True), #Tercer argumento = nullable
                   StructField('indicativo', StringType(), True),
                   StructField('nombre', StringType(), True),
                   StructField('prec', StringType(), True),
                   StructField('presMax', StringType(), True),
                   StructField('presMin',StringType(), True), #Tercer argumento = nullable
                   StructField('provincia', StringType(), True),
                   StructField('racha', StringType(), True),
                   StructField('sol', StringType(), True),
                   StructField('tmax', StringType(), True),
                   StructField('tmin', StringType(), True),
                   StructField('velmedia', StringType(), True)
                  ]
    # Create and return the new DF
    return spark.createDataFrame(diccionarios,schema = StructType(data_schema))  


# ### [1.2.2]  [FUNCIONES] - Request datos

# In[10]:


def req_to_df(codigo,fecha_ini,fecha_fin):
    print("CODIGO: ", codigo, "FECHAS", fecha_ini,fecha_fin)
    try:
        api_response = api_valores.climatologas_diarias_(fecha_ini, fecha_fin,codigo)
        #pprint(api_response)
    except ApiException as e:
        print("Exception: %s\n" % e)
    r = requests.get(api_response.datos)
    data = r.content
    df_aemet = data_to_sparkdf(data)
    print("OK")
    
    return df_aemet.select('fecha','indicativo','dir','prec','presMax','presMin','tmax','tmin','velmedia')
    # Las estaciones del ayunt no tienen datos de insolacion (sol)
    #return df_aemet.select('fecha','indicativo','dir','prec','presMax','presMin','sol','tmax','tmin','velmedia')


# In[11]:


def datos_aemet(codigos_estaciones,fecha_ini,fecha_fin):
    lista_df =[]
    for codigo in codigos_estaciones:
        lista_df.append(req_to_df(codigo,fecha_ini,fecha_fin))
    #Unimos
    df = lista_df[0]
    for i in range(1,len(lista_df)):
        df = df.union(lista_df[i])
    return df


# ### [1.2.3]  _______ MES_CERRADO _______

# #### [1.2.3.0] - Estaciones

# In[12]:


df_estaciones_aemet_mes_cerrado = df_estaciones.filter(df_estaciones["MIDE_CLIMA_AEMET"]>0).filter(df_estaciones["U_TODAS"])


# In[13]:


cod_estaciones_aemet_mes_cerrado = [elem[0] for elem in df_estaciones_aemet_mes_cerrado.select("CODIGO_CORTO").collect()]


# In[14]:


cod_estaciones_aemet_mes_cerrado


# #### [1.2.3.1] - Fechas 

# In[15]:


#Fechas inicio y fecha fin datos
ultimo_dia_mes_cerrado = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
primer_dia_mes_cerrado = ultimo_dia_mes_cerrado.replace(day=1)

fecha_ini = primer_dia_mes_cerrado
fecha_fin = ultimo_dia_mes_cerrado
formato= "%Y-%m-%dT%H:%M:%SUTC"

fecha_ini_str = fecha_ini.strftime(formato)
fecha_fin_str = fecha_fin.strftime(formato)


# #### [1.2.3.2] - Obtenemos los datos

# In[16]:


df_aemet = datos_aemet(cod_estaciones_aemet_mes_cerrado,fecha_ini_str,fecha_fin_str)


# ### [1.2.4] -Columnas -> ANO,MES,DIA,FECHA

# In[17]:


df_aemet = df_aemet.withColumn("ANO",df_aemet["fecha"][0:4])
df_aemet = df_aemet.withColumn("MES",df_aemet["fecha"][6:2])
df_aemet = df_aemet.withColumn("DIA",df_aemet["fecha"][9:2])
df_aemet = df_aemet.withColumn("FECHA",F.concat(df_aemet["fecha"][0:4],df_aemet["fecha"][6:2],df_aemet["fecha"][9:2]))


# ### [1.2.5] - Columnas -> avg(Temp), avg(Pres)

# In[18]:


pd_aemet = df_aemet.toPandas()


# In[19]:


#Cambias comas por puntos
pd_aemet["presMax"]  =  [reg.sub(',','.',str(x)) for x in pd_aemet["presMax"]]
pd_aemet["presMin"]  =  [reg.sub(',','.',str(x)) for x in pd_aemet["presMin"]]
pd_aemet["tmax"]  =  [reg.sub(',','.',str(x)) for x in pd_aemet["tmax"]]
pd_aemet["tmin"]  =  [reg.sub(',','.',str(x)) for x in pd_aemet["tmin"]]


# In[20]:


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


# In[21]:


pd_aemet["pres"] = [media([pmax,pmin])for pmax,pmin in zip(pd_aemet["presMax"].values, pd_aemet["presMin"].values)]
pd_aemet["temp"] = [media([pmax,pmin])for pmax,pmin in zip(pd_aemet["tmax"].values, pd_aemet["tmin"].values)]


# In[22]:


#pd_aemet.dtypes


# ### [1.2.6]- Rename

# In[23]:


#pd_aemet = pd_aemet[["indicativo","fecha","dir","prec","velmedia","pres","temp"]]


# In[24]:


pd_aemet =pd_aemet.rename(columns={"indicativo":"ESTACION",
                                   "velmedia":"81",                         
                                   "dir":"82",
                                   "temp":"83",
                                   "pres":"87",
                                   "prec":"89",
                             })


# ### [1.2.7] - Select

# In[25]:


columnas = ["ESTACION","ANO","MES","DIA","FECHA"]
for elem in magnitudes:
    columnas.append(elem) 


# In[26]:


pd_aemet = pd_aemet[columnas]


# ### [1.2.8] - "None" a Nulo

# In[27]:


pd_aemet = pd_aemet.replace(('None',None),np.nan)


# ### [1.2.9] - Types

# In[28]:


pd_aemet["ANO"] =pd_aemet["ANO"].astype(int)
pd_aemet["MES"] =pd_aemet["MES"].astype(int)
pd_aemet["DIA"] =pd_aemet["DIA"].astype(int)
pd_aemet["FECHA"] =pd_aemet["FECHA"].astype(int)


# In[29]:


#pd_aemet


# # [1.3]  ----------------------- AYUNTAMIENTO -----------------------

#  Todas las estaciones miden todas las variables:
#  
#         SI -81 - VELOCIDAD VIENTO   
#         SI -82 - DIR. DE VIENTO       
#         SI -83 - TEMPERATURA
#         NO -86 - HUMEDAD RELATIVA
#         SI -87 - PRESION BARIOMETRICA
#         NO -88 - RADIACION SOLAR
#         SI -89 - PRECIPITACIÓN
#        
#  

# ### [1.3.0] - Obtenemos los datos

# In[30]:


#!MANUAL, EL ENLACE CAMBIA CADA MES


# In[31]:


anos = [2020]
urls = ["https://datos.madrid.es/egob/catalogo/300351-3-meteorologicos-diarios.csv"]


# In[32]:


def pd_read_to_df(url):
    pdf = pd.read_csv(url,sep=';')
    df = spark.createDataFrame(pdf)
    return df


# In[33]:


lista = [pd_read_to_df(urls[i])for i in range(len(anos))]


# In[34]:


df_ayunt_ano_completo = lista[0]
for i in range(1,len(lista)):
    df_ayunt_19=df_ayunt_19.union(lista[i])


# #### [1.3.0.0] - Cogemos los datos del mes que queremos recalcular

# In[35]:


df_ayunt= df_ayunt_ano_completo.filter(df_ayunt_ano_completo["MES"] == mes_cerrado%100)


# ### [1.3.1] - Estaciones

# In[36]:


#DF info de todas las estaciones Clima utilizadas del AYUNTAMIENT0
df_estaciones_ayunt_mes_cerrado = df_estaciones.filter(df_estaciones["MIDE_CLIMA"]>0).filter(df_estaciones["U_TODAS"])
#Lista con codigos_cortos de las estaciones anteriores.
cod_estaciones_ayunt_mes_cerrado = [elem[0] for elem in df_estaciones_ayunt_mes_cerrado.select("CODIGO_CORTO").collect()]


# ### [1.3.2] -  Filtrar  -> ESTACIONES UTILIZADAS

# In[37]:


df_ayunt = df_ayunt.filter(df_ayunt["ESTACION"].isin(cod_estaciones_ayunt_mes_cerrado))


# ### [1.3.3] - Select

# In[38]:


df_ayunt = df_ayunt.select('ESTACION','MAGNITUD','ANO', 'MES', 'D01', 'V01', 'D02', 'V02', 'D03', 'V03', 'D04', 'V04', 'D05', 'V05', 'D06', 'V06', 'D07', 'V07', 'D08', 'V08', 'D09', 'V09', 'D10', 'V10', 'D11', 'V11', 'D12', 'V12', 'D13', 'V13', 'D14', 'V14', 'D15', 'V15', 'D16', 'V16', 'D17', 'V17', 'D18','V18', 'D19', 'V19', 'D20', 'V20', 'D21', 'V21', 'D22', 'V22', 'D23', 'V23', 'D24', 'V24', 'D25', 'V25', 'D26', 'V26', 'D27', 'V27', 'D28', 'V28', 'D29', 'V29', 'D30', 'V30', 'D31', 'V31')


# ### [1.3.4] - Formato

# In[39]:


def dar_formato(df):
    data_schema = [StructField('ESTACION',IntegerType(), False), 
              StructField('MAGNITUD',IntegerType(), False),
              StructField('ANO',IntegerType(), False),
              StructField('MES',IntegerType(), False),
              StructField('VALOR',FloatType(), True),
              StructField('VALIDO',IntegerType(), False),
              StructField('DIA',IntegerType(), False)]
    struct = StructType(fields = data_schema)

    df_v1 = spark.createDataFrame(spark.sparkContext.emptyRDD(),struct)
    

    for i in range(1,32): #Días  
        valor = 'D%02d' % i
        valido = 'V%02d' % i
        df_v1 = df_v1.union(df.select("ESTACION","MAGNITUD","ANO","MES",valor,valido).withColumn('DIA', F.lit(i)))

    df = df_v1

    df = df.withColumn("FECHA",df["ANO"]*10000 + df["MES"]*100 + df["DIA"])

    cols = df.columns
    cols = cols[:4] + cols[-2:] + cols[-4:-2]
    df= df[cols]

    df = df.withColumn("VALOR VALIDADO",F.when(F.col("VALIDO")== 'N',None).otherwise(F.col("VALOR")) )
    df = df.select("ESTACION","MAGNITUD","ANO","MES","DIA","FECHA",df["VALOR VALIDADO"].alias("VALOR"))

    df = df.groupBy('ESTACION','ANO', 'MES', 'DIA',"FECHA").pivot("MAGNITUD").sum("VALOR").orderBy("FECHA")

    #Esto se puede hacer en el paso 6
    df_31 = df.filter(df["MES"].isin([1,3,5,7,8,10,12]))
    df_30 = df.filter((df["MES"].isin([4,6,9,11])) & (df["DIA"] <31))
    df_feb = df.filter((df["MES"] == 2) & (df["DIA"] <30)) #Para no excluir los bisiestos
    df = df_31.union(df_30).union(df_feb)
    
    return df


# In[40]:


df_ayunt = dar_formato(df_ayunt)


# ### [1.3.5] - Select -> [ESTACION,ANO,MES,DIA,FECHA,+MAGNITUDES]

# In[41]:


pd_ayunt = df_ayunt.toPandas()


# In[42]:


columnas = pd_ayunt.columns.to_list()[:5]
for elem in magnitudes:
    columnas.append(elem) 


# In[43]:


pd_ayunt = pd_ayunt[columnas]


# # [2] -  Union AYUNT + AEMET

# In[44]:


#pd_aemet.dtypes


# In[45]:


#pd_ayunt.dtypes


# In[46]:


pd_final = pd.concat([pd_ayunt,pd_aemet])


# In[47]:


#pd_final.dtypes


# # [3] - Formato

# ## [3.1] - 89-PRECIPITACION == "IP" == Inapreciable -> 0

# In[48]:


pd_final["89"] = [elem if elem != "Ip" else 0 for elem in pd_final["89"].values]


# ## [3.2] - Rename

# In[49]:


pd_final = pd_final.rename(columns={"ESTACION":"CODIGO_CORTO"})


# ## [3.3] - Tipos

# In[56]:


for magnitud in magnitudes:
    pd_final[magnitud]  =  [reg.sub(',','.',str(x)) for x in pd_final[magnitud]]
    # String to float
    pd_final[magnitud] = pd_final[magnitud].astype(float)


# ## [3.4] - Nulos -> Media diaria

# In[58]:


dias = set(pd_final["FECHA"].values.tolist())


# In[59]:


pd_means = pd_final.groupby(by = ["ANO","MES","DIA"]).agg('mean')
for magnitud in magnitudes:
    for dia in dias:
        media_magnitud_dia = pd_means[pd_means["FECHA"]== int(dia)][magnitud].values[0]       
        pd_final.loc[pd_final["FECHA"]==int(dia),magnitud] = [elem if pd.notnull(elem) else media_magnitud_dia
                                                              for elem in pd_final[pd_final["FECHA"]== int(dia)][magnitud].values]
        


# # [4] -Export

# In[60]:


#pd_final.head(5)


# In[61]:


#BackUp
pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/BackUp/Clima_mes-"+ str(mes_cerrado) + ".csv")


# In[62]:


pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima_mes_cerrado.csv")


#  try:
#     api_response = api_observacion.datos_de_observacin__tiempo_actual_1(3195)
#     pprint(api_response)
# except ApiException as e:
#     print("Exception: %s\n" % e)
