#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - ETL_Contaminacion_Recalculo_Mes_Cerrado_v0

#     0. Inicializacion
#     1. Datos
#         1.0 Carga ficheros AIRE (AÑO ACTUAL) - URL
#             1.0.0 Filtrar los datos del ultimo mes cerrado
#         1.1 FORMATO:  [ESTACION,MAGNITUD,ANO,MES,DATO_DIA1,VALIDO,DATO_DIA2,VALIDO...]
#         1.2 Creacion nuevo esquema
#         1.3 Creacion nuevo DF vacio
#         1.4 Nuevo DF -> [ESTACION,MAGNITUD,ANO,MES,DIA,FECHA,VALOR,VALIDO]
#         1.5 Columna FECHA -> 20140101
#         1.6 Colocar columnas -> [ESTACION,MAGNITUD, ANO,MES,DIA, FECHA, VALOR,VALIDO]
#         1.7 VALOR ? VALIDO -> VALOR VALIDADO
#         1.8 PIVOT Magnitudes
#         1.9 Limpiar: Meses 31 días + Meses 30 días + Febreros
#     2. Formato
#     3. Ordenar
#     4. Actualizar datos final
#     5. Exportar
#         
#     

# # [0] - Inicialización

# In[1]:


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


# In[2]:


spark = SparkSession.builder.appName('recalculo_contaminacion').getOrCreate()


# # [1] - Datos

# ## [1.0] - Carga ficheros AIRE ( AÑO ACTUAL) - URL

# In[3]:


#CAMBIAR URL MANUALMENTE
anos= [2020]
urls =["https://datos.madrid.es/egob/catalogo/201410-10306609-calidad-aire-diario.csv"]


# In[4]:


def pd_read_to_df(url):
    pdf = pd.read_csv(url,sep=';')
    df = spark.createDataFrame(pdf)
    return df


# In[5]:


lista = [pd_read_to_df(urls[i])for i in range(len(anos))]


# In[6]:


df = lista[0]
for i in range(1,len(lista)):
    df=df.union(lista[i])


# ### [1.0.0] - Filtramos datos del utlimo mes cerrado

# In[7]:


ultimo_dia_mes_cerrado = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
mes_cerrado = ultimo_dia_mes_cerrado.year *100 + ultimo_dia_mes_cerrado.month 
df= df.filter(df["MES"] == mes_cerrado%100)


# ## [1.1] - FORMATO:  [ESTACION,MAGNITUD,ANO,MES,DATO_DIA1,VALIDO,DATO_DIA2,VALIDO...]

# In[8]:


#Eliminamos columnas no esenciales
df = df.select('ESTACION','MAGNITUD','ANO','MES','D01','V01','D02','V02','D03','V03','D04','V04','D05','V05','D06','V06','D07','V07','D08','V08','D09','V09','D10','V10','D11','V11','D12','V12','D13','V13','D14','V14','D15','V15','D16','V16','D17','V17','D18','V18','D19','V19','D20','V20','D21','V21','D22','V22','D23','V23','D24','V24','D25','V25','D26','V26','D27','V27','D28','V28','D29','V29','D30','V30','D31','V31')


# ## [1.2] - Creacion nuevo esquema

# In[9]:


data_schema = [StructField('ESTACION',IntegerType(), False), 
              StructField('MAGNITUD',IntegerType(), False),
              StructField('ANO',IntegerType(), False),
              StructField('MES',IntegerType(), False),
              StructField('VALOR',FloatType(), True),
              StructField('VALIDO',IntegerType(), False),
              StructField('DIA',IntegerType(), False)]
struct = StructType(fields = data_schema)


# ## [1.3] - Creacion nuevo DF vacio

# In[10]:


df_v1 = spark.createDataFrame(spark.sparkContext.emptyRDD(),struct)


# ## [1.4] - Nuevo DF -> [ESTACION,MAGNITUD,ANO,MES,DIA,FECHA,VALOR,VALIDO]

# In[11]:


for i in range(1,32): #Días  
    valor = 'D%02d' % i
    valido = 'V%02d' % i
    df_v1 = df_v1.union(df.select("ESTACION","MAGNITUD","ANO","MES",valor,valido).withColumn('DIA', F.lit(i)))
df = df_v1


# ## [1.5] - Columna FECHA -> 20140101

# In[12]:


df = df.withColumn("FECHA",df["ANO"]*10000 + df["MES"]*100 + df["DIA"])


# ## [1.6] -Colocar columnas ->
# ### [ESTACION, MAGNITUD, ANO,MES,DIA, FECHA, VALOR,VALIDO]

# In[13]:


#Colocar las columnas
cols = df.columns
cols = cols[:4] + cols[-2:] + cols[-4:-2]
df= df[cols]


# ## [1.7] - VALOR ? VALIDO -> VALOR VALIDADO

# In[14]:


df = df.withColumn("VALOR VALIDADO",F.when(F.col("VALIDO")== 'N',None).otherwise(F.col("VALOR")) )
df = df.select("ESTACION","MAGNITUD","ANO","MES","DIA","FECHA",df["VALOR VALIDADO"].alias("VALOR"))


# ## [1.8] - PIVOT Magnitudes

# In[15]:


df = df.groupBy('ESTACION','ANO', 'MES', 'DIA',"FECHA").pivot("MAGNITUD").sum("VALOR").orderBy("FECHA")


# ## [1.9] - Limpiar: Meses 31 días + Meses 30 días + Febreros

# In[16]:


#Esto se puede hacer en el paso 6
df_31 = df.filter(df["MES"].isin([1,3,5,7,8,10,12]))
df_30 = df.filter((df["MES"].isin([4,6,9,11])) & (df["DIA"] <31))
df_feb = df.filter((df["MES"] == 2) & (df["DIA"] <30)) #Para no excluir los bisiestos
df = df_31.union(df_30).union(df_feb)


# # [2] - Formato

# In[21]:


df = df.withColumn("ESTACION",df["ESTACION"].cast(StringType()))


# In[24]:


df = df.withColumnRenamed("ESTACION","CODIGO_CORTO")


# # [3] - Ordenar 

# In[26]:


df = df.orderBy("ANO","MES","DIA","FECHA","CODIGO_CORTO")


# In[28]:


#Columnas
cols = df.columns
info = cols[0:5]
cols_magnitudes= cols[5:]
cols_magnitudes.sort()
cols = info + cols_magnitudes
df = df.select(cols)


# In[31]:


pd_final = df.toPandas()


# # [5] - Exportar

# In[33]:


#pd_final.head(10)


# In[36]:


#Versiones
pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/BackUp/Contaminacion_mes-" + str(mes_cerrado) +".csv")


# In[37]:


pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mes_cerrado.csv")

