#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,IntegerType,FloatType,StructType
from pyspark.ml.feature import StringIndexer
from pyspark.sql import functions as F
import pandas as pd


spark = SparkSession.builder.appName('punta_de_lanza').getOrCreate()

df_2019 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2019.csv',inferSchema= True,header=True, sep=';')
df_2020 = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Contaminacion/datos_contaminacion_2020_03.csv',inferSchema= True,header=True, sep=';')
df = df_2019.union(df_2020)

df_v0 = df.select('ESTACION','MAGNITUD','ANO','MES','D01','V01','D02','V02','D03','V03','D04','V04','D05','V05','D06','V06','D07','V07','D08','V08','D09','V09','D10','V10','D11','V11','D12','V12','D13','V13','D14','V14','D15','V15','D16','V16','D17','V17','D18','V18','D19','V19','D20','V20','D21','V21','D22','V22','D23','V23','D24','V24','D25','V25','D26','V26','D27','V27','D28','V28','D29','V29','D30','V30','D31','V31')


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
    df_v1 = df_v1.union(df_v0.select("ESTACION","MAGNITUD","ANO","MES",valor,valido).withColumn('DIA', F.lit(i)))
    
cols = df_v1.columns
cols = cols[:4] + cols[-1:] + cols[-3:-1]
df_v2= df_v1[cols]

indexer = StringIndexer(inputCol ="VALIDO",outputCol = "V",stringOrderType="alphabetAsc")
df_v3 = indexer.fit(df_v2).transform(df_v2)

df_v4 = df_v3.select('ESTACION', 'MAGNITUD', 'ANO', 'MES', 'DIA', 'VALOR','V')


df_v5 = df_v4.withColumn("VALOR VALIDADO",F.when(F.col("V")== 0, -1).otherwise(F.col("VALOR")) )
df_v5 = df_v5.select("ESTACION","MAGNITUD","ANO","MES","DIA",df_v5["VALOR VALIDADO"].alias("VALOR"))

df_v6 = df_v5.groupBy('ESTACION','ANO', 'MES', 'DIA').pivot("MAGNITUD").sum("VALOR")

df_final = df_v6

df_final.toPandas().to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion_2019_2020.csv")

