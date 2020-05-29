#!/usr/bin/env python
# coding: utf-8

# # [o3] - Proyecto Ozono - ETL_Clima_Dia_[STRC]

# In[ ]:


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
pd.options.mode.chained_assignment = None
import requests
import numpy as np
import re as reg
import json
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
import os


# In[ ]:


class ClimaDia():
    """
    
        FUNCIONES AUXILIARES<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    
    
    """
    def data_to_sparkdf(self,data):
        #Encoding "ISO-8859"
        data_v = data.decode(encoding ='ISO-8859-15')
        data_v0 = data_v

        # Clean the data
        for i in range(20):
            if(data_v0[i]=='{'):
                data_v0 = data_v0[i:]
        for i in range(20):
            if(data_v0[-i]=='}'):
                data_v0 = data_v0[:-i+1]

        data_v1 = data_v0
        data_v1 = data_v1.replace("\n", "")

        data_v2 = data_v1.replace("},","}};")

        patron =['\s\s','\s"','"\s','\s{']
        replace = [' ','"','"','{']
        data_v3 = data_v2
        for i in range(len(patron)):
            data_v3 = reg.sub(patron[i],replace[i],data_v3)

        data_v4 = data_v3.replace(",",";")

        data_v5 = data_v4.replace("\"", "")

        data_cleaned = data_v5.split("};")

        # String to List of dictionaries
        diccionarios = []
        for fila in data_cleaned:
            keys = []
            values = []
            for pareja in fila[1:-1].split(';'):
                elems =pareja.split(':')
                keys.append(elems[0])
                values.append(elems[1])
            diccionarios.append(dict(zip(keys,values)))

        # Schema for the new DF
        data_schema = [StructField('idema',StringType(), False),
                       StructField('lon', StringType(), True),
                       StructField('fint', StringType(), True),
                       StructField('prec', StringType(), True),
                       StructField('alt', StringType(), True),
                       StructField('vmax', StringType(), True),
                       StructField('vv', StringType(), True),
                       StructField('dv',StringType(), True), 
                       StructField('lat', StringType(), True),
                       StructField('dmax', StringType(), True),
                       StructField('ubi', StringType(), True),
                       StructField('pres', StringType(), True),
                       StructField('hr',StringType(), True), 
                       StructField('ts', StringType(), True),
                       StructField('pres_nmar', StringType(), True),
                       StructField('tamin', StringType(), True),
                       StructField('ta', StringType(), True),
                       StructField('tamax', StringType(), True),
                       StructField('tpr', StringType(), True),
                       StructField('vis', StringType(), True),
                       StructField('stddv', StringType(), True),
                       StructField('inso', StringType(), True),
                       StructField('rviento', StringType(), True),
                      ]
        # Create and return the new DF
        return self.spark.createDataFrame(diccionarios,schema = StructType(data_schema))
    
    def req_hoy_to_df(self,codigo):
        print("CODIGO: ", codigo)
        try:
            api_response = self.api_observacion.datos_de_observacin__tiempo_actual_1(codigo)
            pprint(api_response)
        except ApiException as e:
            pprint(api_response)
            print("Exception: %s\n" % e)
        r = requests.get(api_response.datos)
        data = r.content
        df_aemet = self.data_to_sparkdf(data)
        print("OK")

        return df_aemet.select('idema','fint','prec','pres','tamax','tamin','dv','vv')
    
    def datos_aemet_hoy(self,codigos_estaciones):
        lista_df =[]
        for codigo in codigos_estaciones:
            lista_df.append(self.req_hoy_to_df(codigo))
        #Unimos
        df = lista_df[0]
        for i in range(1,len(lista_df)):
            df = df.union(lista_df[i])
        return df
    
    def media(self,vals):
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
        else:
            return media/validos

    """
    
        FUNCIONES PRINCIPALES <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    
    
    """    
    def __init__(self):
        self.spark = SparkSession.builder.appName('clima_hoy').getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        #Api AEMET
        f = open("/home/rulicering/Datos_Proyecto_Ozono/Credenciales/Credenciales.json")
        credenciales = json.load(f)
        AEMET_API_KEY = credenciales["aemet"]["api_key"]
        configuration = swagger_client.Configuration()
        configuration.api_key['api_key'] = AEMET_API_KEY
        self.api_observacion = swagger_client.ObservacionConvencionalApi(swagger_client.ApiClient(configuration))
        
    def process(self):
        self.carga_estaciones()
        self.aemet()
        self.carga()
        
    def carga_estaciones(self):
        ayer = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        df_estaciones = spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + ayer +".csv",inferSchema= True, header= True)
        self.df_estaciones = df_estaciones.cache()
        
        #Lista de magnitudes
        regex = reg.compile("E_AEMET_HOY")
        self.c_magnitudes_aemet_hoy = [elem[-2:] for elem in list(filter(regex.search,df_estaciones.columns))]
        
    def aemet(self):
        df_estaciones_aemet_hoy = self.df_estaciones.filter(self.df_estaciones["U_AEMET_HOY"])
        cod_estaciones_aemet_hoy = [elem[0] for elem in df_estaciones_aemet_hoy.select("CODIGO_CORTO").collect()]
        
        df_aemet_hoy = self.datos_aemet_hoy(cod_estaciones_aemet_hoy)
        df_aemet = df_aemet_hoy
        
        df_aemet = df_aemet.withColumn("ANO",df_aemet["fint"][0:4])
        df_aemet = df_aemet.withColumn("MES",df_aemet["fint"][6:2])
        df_aemet = df_aemet.withColumn("DIA",df_aemet["fint"][9:2])
        df_aemet = df_aemet.withColumn("HORA",df_aemet["fint"][12:2])
        df_aemet = df_aemet.withColumn("FECHA",F.concat(df_aemet["fint"][0:4],df_aemet["fint"][6:2],df_aemet["fint"][9:2]))
        
        pd_aemet = df_aemet.toPandas()
        #Cambias comas por puntos
        pd_aemet["tamax"]  =  [reg.sub(',','.',str(x)) for x in pd_aemet["tamax"]]
        pd_aemet["tamin"]  =  [reg.sub(',','.',str(x)) for x in pd_aemet["tamin"]]
        
        pd_aemet["temp"] = [self.media([pmax,pmin])for pmax,pmin in zip(pd_aemet["tamax"].values, pd_aemet["tamin"].values)]
        
        pd_aemet =pd_aemet.rename(columns={"idema":"CODIGO_CORTO",
                                   "vv":"81",                         
                                   "dv":"82",
                                   "temp":"83",
                                   "pres":"87",
                                   "prec":"89",
                                     })
        
        # Filtrar datos del dia que se lee     
        ayer = '%02d' % (datetime.date.today()+datetime.timedelta(days=-1)).day
        pd_aemet = pd_aemet[pd_aemet["DIA"]== ayer]
        
        # Colocar columnas
        columnas = ["CODIGO_CORTO","ANO","MES","DIA","HORA","FECHA"]
        for elem in c_magnitudes_aemet_hoy:
            columnas.append(elem)
        pd_aemet = pd_aemet[columnas]   
            
        # Tipos
        for elem in self.c_magnitudes_aemet_hoy:
            pd_aemet[elem]= pd_aemet[elem].astype(float)
            
        #Valores diarios
        pd_aemet_media = pd_aemet.groupby(by=["CODIGO_CORTO","ANO","MES","DIA","FECHA"]).agg({'81':'mean',
                                                                         '82':'mean',
                                                                         '83':'mean',
                                                                         '87':'mean',
                                                                         '89':'sum'})
        #Nulos
        pd_aemet_media = pd_aemet_media.replace(('None',None),np.nan)
        self.pd_final =pd_aemet_media
        
    def carga(self):
        #Los datos de ayer se cargan a las 3 am de hoy. 1 dia de diferencia
        nuevo = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        anterior = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
        
        #BackUp
        pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/BackUp/Clima-"+ nuevo + ".csv")
        pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima-"+ nuevo + ".csv")
        print("[INFO] - Clima-"+ nuevo +".csv --- Created successfully")
        
        #Borrar la de ayer
        try:
            os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima-"+ anterior + ".csv")
            print("[INFO] - Clima-" +  anterior + ".csv --- Removed successfully")
        except:
            print("[ERROR] - Clima-" +  anterior + ".csv --- Could not been removed")


# In[ ]:


#EJECUTAR
clima_hoy = ClimaDia()
clima_hoy.process()

