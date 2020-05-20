#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - ETL_Contaminación_Dia_v1

#     0. Inicializacion
#     1. Datos
#         1.0 Carga ficheros AIRE ( Tiempo Real) - URL
#         1.1 FORMATO:
#             [ESTACION,MAGNITUD,ANO,MES,DIA,DATO_HORA1,VALIDO,DATO_HORA2,VALIDO.]
#         1.2 Creacion nuevo esquema
#         1.3 Creacion nuevo DF vacio
#         1.4 Nuevo DF -> [ESTACION,MAGNITUD,ANO,MES,DIA,HORA,VALOR,VALIDO]
#         1.5 Columna FECHA -> 20140101
#         1.6 Colocar columnas -> 
#             [ESTACION, MAGNITUD, ANO,MES,DIA,HORA,FECHA,VALOR,VALIDO]
#         1.7 VALOR ? VALIDO -> VALOR VALIDADO
#         1.8 PIVOT Magnitudes
#     2. Formato
#     3. Ordenar
#     4. Media diaria
#     5. Exportar
#         5.0 Dia x Horas
#         5.1 Media dia
#         
#     

# # [0] - Inicialización

# In[ ]:


from __future__ import print_function
import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,IntegerType,FloatType,StructType
from pyspark.ml.feature import StringIndexer
from pyspark.sql import functions as F
import pandas as pd
import datetime
import requests
import io


# In[ ]:


spark = SparkSession.builder.appName('contaminacion_hoy').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# # [1] - Datos

# ## [1.0] - Carga ficheros AIRE ( Tiempo Real) - URL

# In[ ]:


url =  "http://www.mambiente.madrid.es/opendata/horario.csv"


# In[ ]:


hdr = {"Host": "www.mambiente.madrid.es",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Cookie": "MadridCookiesPolicy=; _hjid=0faf8542-e813-4294-b619-5eb313249e16",
    "Upgrade-Insecure-Requests": "1"}


# In[ ]:


r = requests.get(url = url, headers = hdr)
#r2.headers
file_object = io.StringIO(r.content.decode(r.apparent_encoding))
pd_aire_hoy = pd.read_csv(file_object,sep=';')


# In[ ]:


#pd_aire_hoy.head(5)


# In[ ]:


df = spark.createDataFrame(pd_aire_hoy)


# ## [1.1] - FORMATO: 
# ### [ESTACION,MAGNITUD,ANO,MES,DIA,DATO_HORA1,VALIDO,DATO_HORA2,VALIDO...]

# In[ ]:


#Eliminamos columnas no esenciales
df = df.select('ESTACION','MAGNITUD','ANO','MES','DIA','H01','V01','H02','V02','H03','V03','H04','V04','H05','V05','H06','V06','H07','V07','H08','V08','H09','V09','H10','V10','H11','V11','H12','V12','H13','V13','H14','V14','H15','V15','H16','V16','H17','V17','H18','V18','H19','V19','H20','V20','H21','V21','H22','V22','H23','V23','H24','V24')


# ## [1.2] - Creacion nuevo esquema

# In[ ]:


data_schema = [StructField('ESTACION',IntegerType(), False), 
              StructField('MAGNITUD',IntegerType(), False),
              StructField('ANO',IntegerType(), False),
              StructField('MES',IntegerType(), False),
              StructField('DIA',IntegerType(), False),
              StructField('VALOR',FloatType(), True),
              StructField('VALIDO',IntegerType(), False),
              StructField('HORA',IntegerType(), False)]
struct = StructType(fields = data_schema)


# ## [1.3] - Creacion nuevo DF vacio

# In[ ]:


df_v1 = spark.createDataFrame(spark.sparkContext.emptyRDD(),struct)


# ## [1.4] - Nuevo DF -> [ESTACION,MAGNITUD,ANO,MES,DIA,HORA,VALOR,VALIDO]

# In[ ]:


for i in range(1,25): #Horas dia
    valor = 'H%02d' % i
    valido = 'V%02d' % i
    df_v1 = df_v1.union(df.select("ESTACION","MAGNITUD","ANO","MES","DIA",valor,valido).withColumn('HORA', F.lit(i)))

df = df_v1


# ## [1.5] - Columna FECHA -> 20140101

# In[ ]:


df = df.withColumn("FECHA",df["ANO"]*10000 + df["MES"]*100 + df["DIA"])


# ## [1.6] -Colocar columnas 
# ### [ESTACION, MAGNITUD, ANO,MES,DIA, HORA,FECHA, VALOR,VALIDO]

# In[ ]:


#Colocar las columnas
cols = df.columns
cols = cols[:5] + cols[-2:] + cols[-4:-2]
df= df[cols]


# ## [1.7] - VALOR ? VALIDO -> VALOR VALIDADO

# In[ ]:


df = df.withColumn("VALOR VALIDADO",F.when(F.col("VALIDO")== 'N',None).otherwise(F.col("VALOR")) )
df = df.select("ESTACION","MAGNITUD","ANO","MES","DIA","HORA","FECHA",df["VALOR VALIDADO"].alias("VALOR"))


# ## [1.8] - PIVOT Magnitudes

# In[ ]:


df = df.groupBy('ESTACION','ANO', 'MES', 'DIA','HORA',"FECHA").pivot("MAGNITUD").sum("VALOR").orderBy("FECHA")


# # [2] - Formato

# In[ ]:


df = df.withColumn("ESTACION",df["ESTACION"].cast(StringType()))


# In[ ]:


pd = df.toPandas()
pd = pd.rename(columns={"ESTACION":"CODIGO_CORTO"})
pd_diaxhoras = pd


# # [3] - Ordenar 

# In[ ]:


cols_magnitudes = pd_diaxhoras.columns.tolist()[6:]
cols_magnitudes.sort()
cols = pd_diaxhoras.columns.tolist()[0:6] + cols_magnitudes
pd_diaxhoras = pd_diaxhoras[cols]


# In[ ]:


pd_diaxhoras = pd_diaxhoras.sort_values(by=["ANO","MES","DIA","FECHA","HORA","CODIGO_CORTO"])


# In[ ]:


#pd_diaxhoras


# # [4] - Media diaria

# In[ ]:


pd_media_dia = pd_diaxhoras.groupby(by=["ANO","MES","DIA","FECHA","CODIGO_CORTO"]).agg('mean')


# In[ ]:


#Quitamos la columna "HORA"
cols = pd_media_dia.columns
cols = cols[1:] 
pd_media_dia = pd_media_dia[cols]


# In[ ]:


#pd_media_dia


# # [5] - Exportar

# In[ ]:


hoy = datetime.date.today().strftime("%Y-%m-%d")
ayer = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


# ## [5.0] - Dia x Horas

# In[ ]:


#pd_diaxhoras.head(10)


# In[ ]:


#BackUp
pd_diaxhoras.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/BackUp/Contaminacion_diaxhoras-" + hoy +".csv")


# In[ ]:


pd_diaxhoras.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_diaxhoras-"+ hoy +".csv")
print("[INFO] - Contaminacion_diaxhoras-", hoy ,".csv --- Created successfully")


# In[ ]:


#Borrar la de ayer
try:
    os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_diaxhoras-"+ ayer +".csv")
     print("[INFO] - Contaminacion_diaxhoras-", ayer,".csv --- Removed successfully")
except:
    print("[ERROR] - Contaminacion_diaxhoras-", ayer,".csv --- Could not been removed")


# ## [5.1] -Media dia

# In[ ]:


#pd_media_dia.head(10)


# In[ ]:


#BackUp
pd_media_dia.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/BackUp/Contaminacion_mediadia-" + hoy +".csv",float_format = '%.8f')


# In[ ]:


pd_media_dia.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mediadia-" + hoy +".csv",float_format = '%.8f')
print("[INFO] - Contaminacion_mediadia-", hoy ,".csv --- Created successfully")


# In[ ]:


#Borrar la de ayer
try:
    os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mediadia-" + ayer +".csv")
     print("[INFO] - Contaminacion_mediadia-", ayer,".csv --- Removed successfully")
except:
    print("[ERROR] - Contaminacion_mediadia-", ayer,".csv --- Could not been removed")

