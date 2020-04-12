#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - ETL_Calendario - v0

# ## [0] - Librerías

# In[1]:


import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
import pandas as pd


# ## [1] - Inicializar sesion de Spark

# In[ ]:


spark = SparkSession.builder.appName('calendario').getOrCreate()


# ## [2] - Origenes de datos

# In[2]:


df = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Raw/Calendario/calendario.csv',inferSchema= True,header=True, sep=',')


# ## [3] -  FECHA: 01/01/2013 -> '20130101'

# In[3]:


from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


# In[4]:


df = df.withColumn("FECHA",F.concat(df["Dia"][7:4],df["Dia"][4:2],df["Dia"][0:2]).cast(IntegerType()))


# ## [4] - DIA SEMANA -> Numérico

# In[5]:


def dia_semana_numerico(col):
    return F.when(F.col("Dia_semana")== 'lunes',1).when(F.col("Dia_semana")== 'martes',2).when(F.col("Dia_semana")== 'miercoles',3).when(F.col("Dia_semana")== 'jueves',4).when(F.col("Dia_semana")== 'viernes',5).when(F.col("Dia_semana")== 'sabado',6).otherwise(7)


# In[6]:


df = df.withColumn("DIASEMANA", dia_semana_numerico(df["Dia_Semana"]))


# ## [5] -  LABORABLE: 1, FESTIVO:0

# In[7]:


df = df.withColumn("LABORAL", F.when(F.col('laborable / festivo / domingo festivo')== "laborable",1).otherwise(0))


# ## [6] - Unir columnas LABORAL Y LECTIVO

# ####  Columna TIPODIA: 2 = Laborable+Lectivo , 1= Laborable , 0 = Festivo

# In[8]:


df = df.withColumn("TIPODIA", F.when((F.col("LABORAL")==1)&(F.col("lectivo")==1),2).when((F.col("LABORAL")==1)&(F.col("lectivo")==0),1).otherwise(0))


# ## [7] - Seleccionamos columnas utiles y exportamos

# In[9]:


df = df.select('FECHA',"DIASEMANA","TIPODIA",df["Confinamiento"].alias("CONFINAMIENTO"))


# In[10]:


df.toPandas().to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Calendario_2013-2020.csv")

