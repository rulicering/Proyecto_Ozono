#!/usr/bin/env python
# coding: utf-8

# In[62]:


import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,IntegerType,FloatType,StructType
from pyspark.ml.feature import StringIndexer
from pyspark.sql import functions as F
import pandas as pd

spark = SparkSession.builder.appName('punta_de_lanza').getOrCreate()

df_2013 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2013.csv',inferSchema= True,header=True, sep=';')
df_2014 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2014.csv',inferSchema= True,header=True, sep=';')
df_2015 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2015.csv',inferSchema= True,header=True, sep=';')
df_2016 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2016.csv',inferSchema= True,header=True, sep=';')
df_2017 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2017.csv',inferSchema= True,header=True, sep=';')
df_2018 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2018.csv',inferSchema= True,header=True, sep=';')
df_2019 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2019.csv',inferSchema= True,header=True, sep=';')
df_2020 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2020_03.csv',inferSchema= True,header=True, sep=';')
lista = [df_2014,df_2015,df_2016,df_2017,df_2018,df_2019,df_2020]
df = df_2013
for elem in lista:
    df = df.union(elem)
    

df = df.select('ESTACION','MAGNITUD','ANO','MES','D01','V01','D02','V02','D03','V03','D04','V04','D05','V05','D06','V06','D07','V07','D08','V08','D09','V09','D10','V10','D11','V11','D12','V12','D13','V13','D14','V14','D15','V15','D16','V16','D17','V17','D18','V18','D19','V19','D20','V20','D21','V21','D22','V22','D23','V23','D24','V24','D25','V25','D26','V26','D27','V27','D28','V28','D29','V29','D30','V30','D31','V31')


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
#Introducimos una columna date numérica para agilizar las operaciones(orderby)
df = df_v1
df = df.withColumn("FECHA",df["ANO"]*10000 + df["MES"]*100 + df["DIA"])

cols = df.columns
cols = cols[:4] + cols[-2:] + cols[-4:-2]
df= df[cols]

indexer = StringIndexer(inputCol ="VALIDO",outputCol = "V",stringOrderType="alphabetAsc")
df = indexer.fit(df.orderBy("VALIDO")).transform(df.orderBy("VALIDO"))

df = df.withColumn("VALOR VALIDADO",F.when(F.col("V")== 0, -1).otherwise(F.col("VALOR")) )
df = df.select("ESTACION","MAGNITUD","ANO","MES","DIA","FECHA",df["VALOR VALIDADO"].alias("VALOR"))

df = df.groupBy('ESTACION','ANO', 'MES', 'DIA',"FECHA").pivot("MAGNITUD").sum("VALOR").orderBy("FECHA")

df_31 = df.filter(df["MES"].isin([1,3,5,7,8,10,12]))
df_30 = df.filter((df["MES"].isin([4,6,9,11])) & (df["DIA"] <31))
df_feb = df.filter((df["MES"] == 2) & (df["MES"] <30)) #Para no excluir los bisiestos
df = df_31.union(df_30).union(df_feb)

df = df.orderBy("ANO","MES","DIA","FECHA","ESTACION")

df.toPandas().to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion_2013-2020.csv")

