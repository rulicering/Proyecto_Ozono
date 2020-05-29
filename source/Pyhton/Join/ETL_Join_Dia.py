#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - ETL_Join_Dia_[STRC]

# # [0] - Inicialización

# In[ ]:


import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pandas
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
import re as reg
import numpy as np
import datetime
import os


# In[ ]:


class JoinDia():
    def __init__(self):
        self.spark = SparkSession.builder.appName('join_hoy').getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
    
    def process(self):
        self.extraccion()
        self.dar_grupo_clima_a_estaciones_aire()
        self.merge_clima_aire()
        self.incluir_calendario()
        self.formato()
        self.carga()
        
    def extraccion(self):
        dia = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        df_aire = self.spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mediadia-" + dia +".csv",inferSchema= True,header=True)
        df_clima = self.spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima-' + dia +  '.csv',inferSchema= True,header=True)
        df_estaciones = self.spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + dia +".csv",inferSchema= True,header=True)
        df_calendario = self.spark.read.csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Calendario/Calendario_2001-2020.csv',inferSchema= True,header=True)
        
        pd_aire = df_aire.drop("_c0").toPandas()
        pd_clima = df_clima.drop("_c0").toPandas()
        pd_estaciones = df_estaciones.drop("_c0").toPandas()
        pd_calendario = df_calendario.drop("_c0").toPandas()
        
        #Estaciones
        #None to NULO
        pd_estaciones = pd_estaciones.replace(('None',None),np.nan)
        #Columnas estaciones medicion - 2014
        regex = reg.compile("E_AEMET_HOY")
        self.c_aemet_hoy = [elem for elem in list(filter(regex.search,df_estaciones.columns))]
        
        #Columnas estaciones medicion a string
        for columna in self.c_aemet_hoy:
            pd_estaciones[columna] = pd_estaciones[columna].astype(str)
            
        self.pd_estaciones = pd_estaciones
        
        #Aire
        #None to NULO
        pd_aire = pd_aire.replace(('None',None),np.nan)
        #CODIGOS CORTOS A Strig
        pd_aire["CODIGO_CORTO"] = pd_aire["CODIGO_CORTO"].astype(str)
        pd_aire = pd_aire.sort_values(by="FECHA")
        print("Estaciones que miden datoss actuales de contaminación: ", pd_aire["CODIGO_CORTO"].count())
        self.pd_aire = pd_aire

        #Clima
        #None to NULO
        pd_clima = pd_clima.replace(('None',None),np.nan)
        #Listar las columnas que miden magnitudes
        columnas_valoresmagnitudes = list(pd_clima.columns)[5:]
        #Columnas magnitudes a float
        for columna in columnas_valoresmagnitudes:
            #Comas por puntos
            pd_clima[columna]  =  [reg.sub(',','.',str(x)) for x in pd_clima[columna]]
            # String to float
            pd_clima[columna] = pd_clima[columna].astype(float)
        pd_clima = pd_clima.sort_values(by="FECHA")
        self.pd_clima = pd_clima
        
        #Calendario
        #None to NULO
        pd_calendario = pd_calendario.replace(('None',None),np.nan)
        self.pd_calendario = pd_calendario
        
    def dar_grupo_clima_a_estaciones_aire(self):
        #Sacamos estaciones de aire y codigos de estaciones de clima asociadas
        pd_estaciones_aire = self.pd_estaciones[self.pd_estaciones["MIDE_AIRE"]>0]
        #Nos quedamos con las columnas que queremos
        c_agrupamiento = ["CODIGO_CORTO"] + self.c_aemet_hoy
        pd_estaciones_aire = pd_estaciones_aire[c_agrupamiento]
        #Unimos ambos datasets
        pd_aire = self.pd_aire.merge(pd_estaciones_aire, on =["CODIGO_CORTO"])
        self.pd_datos_hoy = pd_aire
        
    def merge_clima_aire(self):
        pd_datos_hoy = self.pd_datos_hoy
        pd_clima = self.pd_clima
        
        columnas = list(pd_clima.columns) 
        c_info = columnas[:5]
        c_magnitudes = columnas[5:]

        #Asociamos cada columna de datos de clima (magnitud) a la estación de aire correspondiente.  
        for magnitud in c_magnitudes:
            cols = c_info.copy()
            cols.append(magnitud)
            pd_clima_magnitud_hoy = pd_clima[cols]

            #HOY
            pd_clima_magnitud_hoy = pd_clima_magnitud_hoy.rename(columns={"CODIGO_CORTO":"E_AEMET_HOY_%s"%magnitud})
            pd_datos_hoy =pd_datos_hoy.merge(pd_clima_magnitud_hoy,on = ["ANO", "MES", "DIA","FECHA","E_AEMET_HOY_%s"%magnitud])
            
        self.pd_datos_hoy = pd_datos_hoy
    def incluir_calendario(self):
        self.pd_datos_hoy_y_calendario = self.pd_datos_hoy.merge(self.pd_calendario, on = "FECHA")
        
    def formato(self):
        #Borramos las columnas utilizadas para relacionar estaciones de aire y clima
        a_borrar = self.c_aemet_hoy
        pd_datos_hoy = self.pd_datos_hoy_y_calendario.drop(columns = a_borrar)
        
        #Colocamos columnas
        cols = pd_datos_hoy.columns.tolist()
        cols = [cols[4]] + cols[0:4] +cols[-3:]+ cols[5:-3]
        pd_datos_hoy = pd_datos_hoy[cols]
        
        pd_datos_hoy["CODIGO_CORTO"] = pd_datos_hoy["CODIGO_CORTO"].astype(int)
        self.pd_datos_hoy = pd_datos_hoy
        
    def carga(self):
        pd_datos_hoy = self.pd_datos_hoy
        #EL join se ejecuta a las 2 am del dia siguiente. 1 día de diferencia
        nuevo = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        anterior = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        
        #BackUp
        pd_datos_hoy.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/BackUp/Datos-Dia-" + nuevo + ".csv")
        pd_datos_hoy.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos-Dia-" + nuevo + ".csv")
        print("[INFO] - Datos-Dia-"+ nuevo+".csv --- Created successfully")
        #Borrar la de ayer
        try:
            os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos-Dia-" + anterior + ".csv")
            print("[INFO] - Datos-Dia-" +  anterior + ".csv --- Removed successfully")
        except:
            print("[ERROR] - Datos-Dia-"+ anterior +".csv --- Could not been removed")
            
        #Inlcuir datos hoy en el conjunto total de datos (Dato Final)
        pd_datos = pandas.read_csv('/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos.csv')
        pd_datos = pandas_datos.drop(columns = ["Unnamed: 0"])
        pd_datos_final = pandas.concat([pd_datos,pd_datos_hoy])
        
        #BackUp
        pd_datos_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/BackUp/Join_diario/Datos-" + nuevo + ".csv")
        
        pd_datos_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Dato_Final/Datos.csv")
        print("[INFO] - JOIN DATOS HOY + TOTAL - Successfully")


# In[ ]:


join_dia = JoinDia()
join_dia.process()


# In[ ]:




