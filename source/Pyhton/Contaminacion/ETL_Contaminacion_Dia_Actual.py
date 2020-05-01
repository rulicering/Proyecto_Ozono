#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - ETL_Contaminación_Dia_Actual_v0

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

# In[24]:


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


# In[25]:


spark = SparkSession.builder.appName('contaminacion_hoy').getOrCreate()


# # [1] - Datos

# ## [1.0] - Carga ficheros AIRE ( Tiempo Real) - URL

# In[26]:


url =  "http://www.mambiente.madrid.es/opendata/horario.csv"


# In[27]:


hdr = {"Host": "www.mambiente.madrid.es",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Cookie": "MadridCookiesPolicy=; _hjid=0faf8542-e813-4294-b619-5eb313249e16",
    "Upgrade-Insecure-Requests": "1"}


# In[28]:


r = requests.get(url = url, headers = hdr)
#r2.headers
file_object = io.StringIO(r.content.decode(r.apparent_encoding))
pd_aire_hoy = pd.read_csv(file_object,sep=';')


# In[29]:


#pd_aire_hoy.head(5)


# In[30]:


df = spark.createDataFrame(pd_aire_hoy)


# ## [1.1] - FORMATO: 
# ### [ESTACION,MAGNITUD,ANO,MES,DIA,DATO_HORA1,VALIDO,DATO_HORA2,VALIDO...]

# In[31]:


#Eliminamos columnas no esenciales
df = df.select('ESTACION','MAGNITUD','ANO','MES','DIA','H01','V01','H02','V02','H03','V03','H04','V04','H05','V05','H06','V06','H07','V07','H08','V08','H09','V09','H10','V10','H11','V11','H12','V12','H13','V13','H14','V14','H15','V15','H16','V16','H17','V17','H18','V18','H19','V19','H20','V20','H21','V21','H22','V22','H23','V23','H24','V24')


# ## [1.2] - Creacion nuevo esquema

# In[32]:


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

# In[33]:


df_v1 = spark.createDataFrame(spark.sparkContext.emptyRDD(),struct)


# ## [1.4] - Nuevo DF -> [ESTACION,MAGNITUD,ANO,MES,DIA,HORA,VALOR,VALIDO]

# In[34]:


for i in range(1,25): #Horas dia
    valor = 'H%02d' % i
    valido = 'V%02d' % i
    df_v1 = df_v1.union(df.select("ESTACION","MAGNITUD","ANO","MES","DIA",valor,valido).withColumn('HORA', F.lit(i)))

df = df_v1


# ## [1.5] - Columna FECHA -> 20140101

# In[35]:


df = df.withColumn("FECHA",df["ANO"]*10000 + df["MES"]*100 + df["DIA"])


# ## [1.6] -Colocar columnas 
# ### [ESTACION, MAGNITUD, ANO,MES,DIA, HORA,FECHA, VALOR,VALIDO]

# In[36]:


#Colocar las columnas
cols = df.columns
cols = cols[:5] + cols[-2:] + cols[-4:-2]
df= df[cols]


# ## [1.7] - VALOR ? VALIDO -> VALOR VALIDADO

# In[37]:


df = df.withColumn("VALOR VALIDADO",F.when(F.col("VALIDO")== 'N',None).otherwise(F.col("VALOR")) )
df = df.select("ESTACION","MAGNITUD","ANO","MES","DIA","HORA","FECHA",df["VALOR VALIDADO"].alias("VALOR"))


# ## [1.8] - PIVOT Magnitudes

# In[38]:


df = df.groupBy('ESTACION','ANO', 'MES', 'DIA','HORA',"FECHA").pivot("MAGNITUD").sum("VALOR").orderBy("FECHA")


# # [2] - Formato

# In[39]:


df = df.withColumn("ESTACION",df["ESTACION"].cast(StringType()))


# In[40]:


pd = df.toPandas()
pd = pd.rename(columns={"ESTACION":"CODIGO_CORTO"})
pd_diaxhoras = pd


# # [3] - Ordenar 

# In[41]:


pd_diaxhoras = pd_diaxhoras.sort_values(by=["ANO","MES","DIA","FECHA","HORA","CODIGO_CORTO"])


# # [4] - Media diaria

# In[42]:


pd_media_dia = pd_diaxhoras.groupby(by=["ANO","MES","DIA","FECHA","CODIGO_CORTO"]).agg('mean')


# In[43]:


cols = pd_media_dia.columns
cols = cols[1:] #Quitamos la columna "HORA"


# In[44]:


pd_media_dia = pd_media_dia[cols]


# In[45]:


#pd_media_dia


# # [5] - Exportar

# In[46]:


hoy = datetime.date.today().strftime("%Y-%m-%d")


# ## [5.0] - Dia x Horas

# In[47]:


#pd_diaxhoras.head(10)


# In[48]:


#BackUp
pd_diaxhoras.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/BackUp/Contaminacion_diaxhoras-" + hoy +".csv")


# In[49]:


pd_diaxhoras.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_diaxhoras.csv")


# ## [5.1] -Media dia

# In[50]:


#pd_media_dia.head(10)


# In[51]:


#BackUp
pd_media_dia.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/BackUp/Contaminacion_mediadia-" + hoy +".csv")


# In[52]:


pd_media_dia.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mediadia.csv")

