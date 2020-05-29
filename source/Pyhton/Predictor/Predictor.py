#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - Predictor_[STRC]

# # [0] - Inicialización

# In[ ]:


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
import os


# In[ ]:


class Predictor():
    def __init__(self):
        self.spark = SparkSession.builder.appName('predictor_datos').getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        
    def process(self):
        self.extraccion()
        self.preparar_dato()
        self._probabilidad_a_lluvia_presion_ayer_aire_a_null()
        self._union_total_hoy()
        self._añadir_contaminacion_ayer()
        self.predictor_gbt()
        self.carga()
        
    def extraccion(self):
        ayer = (datetime.date.today() + datetime.timedelta(days = -1)).strftime("%Y-%m-%d")
        hoy = datetime.date.today().strftime("%Y-%m-%d")
        df_datos = self.spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos.csv',inferSchema= True,header=True)
        df_clima_prediccion = self.spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima_Prediccion-"+ hoy + ".csv",inferSchema= True,header=True)
        df_calendario = self.spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Calendario/Calendario_2001-2020.csv',inferSchema= True,header=True)
        
        self.df_datos = df_datos.drop("_c0")
        self.df_clima_prediccion = df_clima_prediccion.drop("_c0")
        self.df_calendario = df_calendario.drop("_c0")
        
        self.magnitudes= self.df_datos.columns[8:]
        self.magnitudes_clima = self.df_datos.columns[-5:]
        self.magnitudes_aire = self.df_datos.columns[8:-5]
        
        self.dic_clima = { "VIENTO":"81",
                 "DIRECCION": "82",
                 "TEMPERATURA": "83",
                 "PRESION": "87",
                 "LLUVIA":"89"
        }
        
    def preparar_dato(self):
        ayer = (datetime.date.today() + datetime.timedelta(days = -1)).strftime("%Y%m%d")
        hoy = datetime.date.today().strftime("%Y%m%d")
        
        df_estaciones_aire = self.df_datos.filter(self.df_datos["FECHA"]== ayer).select("CODIGO_CORTO")
        cod_estaciones_aire = [elem[0] for elem in df_estaciones_aire.collect()]
        cod_estaciones_aire.sort()
        self.cod_estaciones_aire = cod_estaciones_aire
        
        df_hoy = self.df_calendario.filter(self.df_calendario["FECHA"]== hoy)

        #Calendario + Magnitudes aire a null
        for magnitud in self.magnitudes_aire:
            df_hoy = df_hoy.withColumn(magnitud,F.lit(None))
            
        #Calendario + Prediccion clima
        df_clima_hoy = df_hoy.join(self.df_clima_prediccion,on= "FECHA")
        
        #Estaciones cross datos clima y calendario
        df_datos_hoy = df_estaciones_aire.crossJoin(df_clima_hoy)
        
        cols = df_datos_hoy.columns
        cols = cols[0:1] + cols[19:22]+ cols[1:5]+ cols[5:19] + cols[22:]
        self.df_datos_hoy = df_datos_hoy.select(cols)
        
        
    def _probabilidad_a_lluvia_presion_ayer_aire_a_null(self):
        
        df_datos = self.df_datos
        df_datos_hoy = self.df_datos_hoy
        cod_estaciones_aire = self.cod_estaciones_aire
        dic_clima = self.dic_clima
        df_clima_prediccion = self.df_clima_prediccion
        
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
            
        self.df_datos_hoy = df_datos_hoy
    
    
    def _union_total_hoy(self):
        self.df_datos= self.df_datos.union(self.df_datos_hoy)
        
    def _añadir_contaminacion_ayer(self):
        df_datos = self.df_datos
        ventana = Window.partitionBy("CODIGO_CORTO").orderBy("FECHA")
        for magnitud in self.magnitudes_aire:
            df_datos = df_datos.withColumn("A_%s"%magnitud, F.lag(magnitud,1,None).over(ventana))
        self.df_datos = df_datos
        
    def predictor_gbt(self):
        hoy = datetime.date.today().strftime("%Y%m%d")
        df_datos = self.df_datos.cache()
        magnitudes_clima = self.magnitudes_clima
        magnitudes_aire = self.magnitudes_aire
        
        cols_comunes = df_datos.columns[0:8] + magnitudes_clima
        
        l_predicciones = []
        for magnitud in magnitudes_aire:
            print("="*20, magnitud, "="*20)
            cols_comunes = df_datos.columns[0:8] + magnitudes_clima
            cols_features = cols_comunes + ["A_%s"%magnitud]

            #Limpiamos las filas con el dato para esa magnitud a Null
            cols_y_magnitud = cols_features + [magnitud]
            df_datos_magnitud_calculados = df_datos.filter(df_datos["FECHA"] <hoy).select(cols_y_magnitud).na.drop().cache()
            df_datos_magnitud_hoy = df_datos.filter(df_datos["FECHA"] == hoy).select(cols_y_magnitud).na.drop(subset = "A_%s"%magnitud).cache()
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
        
        df_prediccion = l_predicciones[0]
        for i in range(1,len(l_predicciones)):
            df_prediccion = df_prediccion.join(l_predicciones[i],on= "CODIGO_CORTO",how='outer')
        pd_prediccion = df_prediccion.toPandas()
        
        #Formato
        cols = pd_prediccion.columns.tolist()
        regex = reg.compile("P_")
        cols_predicciones = [elem for elem in list(filter(regex.search,cols))]
        cols = cols[0:1]+cols_predicciones
        self.pd_prediccion = pd_prediccion[cols]
    
        
    def carga(self):
        pd_prediccion = self.pd_prediccion
        
        ayer = (datetime.date.today() + datetime.timedelta(days = -1)).strftime("%Y-%m-%d")
        hoy = datetime.date.today().strftime("%Y-%m-%d")
        
        #BackUp
        pd_prediccion.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Predicciones/BackUp/Prediccion-" + hoy + ".csv")
        pd_prediccion.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Predicciones/Prediccion-" + hoy + ".csv")
        print("[INFO] - Prediccion-" +  hoy +".csv --- Generated successfully")
        
        #Borrar la de ayer
        try:
            os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Predicciones/Prediccion-" + ayer + ".csv")
            print("[INFO] - Prediccion-" + ayer + ".csv --- Removed successfully")
        except:
            print("[ERROR] - Prediccion-" + ayer + ".csv --- Could not been removed")


# In[ ]:


#EJECUTAR
predictor = Predictor()
predictor.process()


# In[ ]:




