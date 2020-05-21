#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - Predictor_v0

# # [0] - Inicialización

# In[1]:


import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
import re as reg
import numpy as np
import datetime

#MlLib
#from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

#Aux
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


# In[2]:


spark = SparkSession.builder.appName('predictor').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# # [1] Datos

# ## [1.0] - Carga de ficheros (Datos, Predicción clima,  Calendario)
#     Se ejecuta el predictor a las 2 am del dia siguiente

# In[ ]:


ayer = (datetime.date.today() + datetime.timedelta(days = -1)).strftime("%Y%m%d")
hoy = datetime.date.today().strftime("%Y%m%d")


# In[3]:


df_datos = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos.csv',inferSchema= True,header=True)
df_clima_prediccion = spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/BackUp/Clima_Prediccion-"+ hoy + ".csv",inferSchema= True,header=True)
df_calendario = spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Calendario/Calendario_2001-2020.csv',inferSchema= True,header=True)


# In[4]:


df_datos = df_datos.drop("_c0")
df_clima_prediccion = df_clima_prediccion.drop("_c0")
df_calendario = df_calendario.drop("_c0")


# In[5]:


magnitudes= df_datos.columns[8:]
magnitudes_clima = df_datos.columns[-5:]
magnitudes_aire = df_datos.columns[8:-5]


# In[6]:


dic_clima = { "VIENTO":"81",
                 "DIRECCION": "82",
                 "TEMPERATURA": "83",
                 "PRESION": "87",
                 "LLUVIA":"89"
}


# ## [1.1] - Datos para prediccion - Prediccion clima + Calendario + Estaciones

# In[8]:


df_estaciones_aire = df_datos.filter(df_datos["FECHA"]== ayer).select("CODIGO_CORTO")


# In[9]:


cod_estaciones_aire = [elem[0] for elem in df_estaciones_aire.collect()]


# In[10]:


cod_estaciones_aire.sort()


# In[11]:


df_hoy = df_calendario.filter(df_calendario["FECHA"]== hoy)


# In[12]:


#Calendario + Magnitudes aire a null
for magnitud in magnitudes_aire:
    df_hoy = df_hoy.withColumn(magnitud,F.lit(None))


# In[13]:


#Calendario + Prediccion clima
df_clima_hoy = df_hoy.join(df_clima_prediccion,on= "FECHA")


# In[14]:


#Estaciones cross datos clima y calendario
df_datos_hoy = df_estaciones_aire.crossJoin(df_clima_hoy)


# In[16]:


cols = df_datos_hoy.columns
cols = cols[0:1] + cols[19:22]+ cols[1:5]+ cols[5:19] + cols[22:]


# In[17]:


df_datos_hoy = df_datos_hoy.select(cols)


# ### [1.1.0] - Probabilidad lluvia -> Prediccion lluvia m/l2 + Presion dia anterior
#     Si la probabilidad es > 50%:
#         se hace la media por estacion del historial de precipitaciones
#         cogiendo datos de +-10 días al dia de hoy de cada año anterior

# In[18]:


def probabilidad_a_lluvia_presion_ayer_aire_a_null(df_datos,df_datos_hoy):
    mes_dia_min = (datetime.date.today() +  datetime.timedelta(days = -10)).strftime("%m%d")
    mes_dia_max = (datetime.date.today() +  datetime.timedelta(days = 10)).strftime("%m%d")
    df_historial = df_datos.filter((df_datos["FECHA"]%1000 >= mes_dia_min) & (df_datos["FECHA"]%1000 <= mes_dia_max))
    df_datos_hoy = df_datos_hoy.drop('%' + dic_clima["LLUVIA"])
    l_df = []
    for estacion in cod_estaciones_aire:
        df_datos_hoy_estacion = df_datos_hoy.filter(df_datos_hoy["CODIGO_CORTO"]==estacion)
        aux = df_historial.filter(df_historial["CODIGO_CORTO"]== estacion).select("FECHA",dic_clima["PRESION"],dic_clima["LLUVIA"]).na.drop()
        #Precipitacions
        prob_lluvia_hoy = df_clima_prediccion.select("%" +dic_clima["LLUVIA"]).collect()[0][0]
        prec = 0
        if(float(prob_lluvia_hoy) > 50):
            try:
                prec = aux.filter(aux[dic_clima["LLUVIA"]] >0).select(dic_clima["LLUVIA"]).groupBy().mean().collect()[0][0]
            except:
                print("[WARN]: No hay lluvias historicas en ese rango de fechas")
        df_datos_hoy_estacion = df_datos_hoy_estacion.withColumn(dic_clima["LLUVIA"],F.lit(prec))
            
        #Presion
        ayer = (datetime.date.today() + datetime.timedelta(days= -1)).strftime("%Y%m%d")
        presion_ayer = aux.filter(aux["FECHA"]==ayer).select(dic_clima["PRESION"]).collect()[0][0]
        df_datos_hoy_estacion = df_datos_hoy_estacion.withColumn(dic_clima["PRESION"],F.lit(presion_ayer))

        l_df.append(df_datos_hoy_estacion)
        
    df_datos_hoy = l_df[0]
    for i in range(1,len(l_df)):
        df_datos_hoy = df_datos_hoy.union(l_df[i])
    return df_datos_hoy


# In[19]:


df_datos_hoy = probabilidad_a_lluvia_presion_ayer_aire_a_null(df_datos,df_datos_hoy)


# ## [1.2] - Union Datos + Datos hoy

# In[21]:


df_datos= df_datos.union(df_datos_hoy)


# ## [1.3] - Dar cada fila de datos +  contaminacion ayer

# In[22]:


ventana = Window.partitionBy("CODIGO_CORTO").orderBy("FECHA")


# In[23]:


for magnitud in magnitudes_aire:
    df_datos = df_datos.withColumn("A_%s"%magnitud, F.lag(magnitud,1,None).over(ventana))


# ## [1.4] - Tipos
#     
#     CODIGO_CORTO -> INTEGER
#     ANO -> INTEGER
#     MES -> INTEGER
#     DIA -> INTEGER
#     FECHA -> INTEGER
#     DIASEMANA -> INTEGER
#     TIPODIA -> INTEGER
#     CONFINAMIENTO -> INTEGER
#     MEDICIONES -> DOUBLE

# # [2] - PREDICCIONES
#     
#     Se hace 1 a 1 para cada magnitud de contaminación
#     

# In[26]:


cols_comunes = df_datos.columns[0:8] + magnitudes_clima


# ## [2.1] - GBT

# In[47]:


l_predicciones = []
for magnitud in magnitudes_aire:
    print("="*20, magnitud, "="*20)
    cols_comunes = df_datos.columns[0:8] + magnitudes_clima
    cols_features = cols_comunes + ["A_%s"%magnitud]

    #Limpiamos las filas con el dato para esa magnitud a Null
    cols_y_magnitud = cols_features + [magnitud]
    df_datos_magnitud_calculados = df_datos.filter(df_datos["FECHA"] <hoy).select(cols_y_magnitud).na.drop()
    df_datos_magnitud_hoy = df_datos.filter(df_datos["FECHA"] == hoy).select(cols_y_magnitud).na.drop(subset = "A_%s"%magnitud)
    #df_datos_magnitud = df_datos_magnitud_calculados.union(df_datos_magnitud_hoy)
    
    #Assembles to create features column
    assembler = VectorAssembler(inputCols = cols_features, outputCol = "F_%s" % magnitud)
    data_assembled = assembler.transform(df_datos_magnitud_calculados)
    data_to_predict_assembled = assembler.transform(df_datos_magnitud_hoy)
    
    #Seleccionamos las filas que vamos a utilizar
    trainingData = data_assembled.select("CODIGO_CORTO","F_%s" %magnitud, magnitud)
    data_to_predict = data_to_predict_assembled.select("CODIGO_CORTO","F_%s" %magnitud, magnitud)

    # Train a GBT model.
    gbt = GBTRegressor(featuresCol="F_%s" % magnitud, labelCol=magnitud,maxIter=20,predictionCol="P_%s" %magnitud)
        
    # Train model.  This also runs the indexer.
    model = gbt.fit(trainingData)
    
    # Make predictions.
    predictions = model.transform(data_to_predict)
    
    # Select example rows to display
    predictions.select("CODIGO_CORTO","P_%s" %magnitud,"F_%s" % magnitud).show()
    l_predicciones.append(predictions)
    


# ### [2.1.0] - Unimos las predicciones por magnitud

# In[33]:


df_prediccion = l_predicciones[0]


# In[34]:


for i in range(1,len(l_predicciones)):
    df_prediccion = df_prediccion.join(l_predicciones[i],on= "CODIGO_CORTO",how='outer')


# In[36]:


pd_prediccion = df_prediccion.toPandas()


# # [4] - FORMATO

# In[39]:


cols = pd_prediccion.columns.tolist()


# In[40]:


regex = reg.compile("P_")
cols_predicciones = [elem for elem in list(filter(regex.search,cols))]


# In[44]:


cols = cols[0:1]+cols_predicciones


# In[46]:


pd_prediccion = pd_prediccion[cols]


# # [5] - EXPORTAR

# In[50]:


#Versiones
pd_prediccion.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Predicciones/BackUp/Prediccion-" + hoy + ".csv")


# In[49]:


pd_prediccion.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Predicciones/Prediccion-" + hoy + ".csv")
print("[INFO] - Prediccion-", hoy,".csv --- Generated successfully")


# In[ ]:


#Borrar la de ayer
try:
    os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Predicciones/Prediccion-" + ayer + ".csv")
    print("[INFO] - Prediccion-", ayer,".csv --- Removed successfully")
except:
    print("[ERROR] - Prediccion-", ayer,".csv --- Could not been removed")

