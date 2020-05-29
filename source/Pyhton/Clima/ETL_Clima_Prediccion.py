#!/usr/bin/env python
# coding: utf-8

# # [o3] - Proyecto Ozono - ETL_Clima_Prediccion_[STRC]

# # [0] - Inicialización

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
import requests
import numpy as np
import re as reg
import json
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
import os


# In[ ]:


class ClimaPrediccion():
    """
    
        FUNCIONES AUXILIARES<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    
    
    """
    def convertir_a_diccionario(self,raw,inicio,tipo):
        #Variables locales
        i = inicio
        iniciob = -1
        #Dicionarios del tipo {}
        diccionario = {}
        #Diccionarios elementos de lista
        lista = []
        #Auxiliares
        final = len(raw)
        a_is_fixed = False 

        while i < final:
            c = raw[i] #Caracter a leer
            if(i > 0):c_ant = raw[i-1] # Caracter anterior

            if((c == ":") & ~(a_is_fixed)):
                a_is_fixed = True
                a = raw[inicio:i]
                iniciob = i+1
                b = ''

            if(c == ";" or c == "," ):
                #Si estamos en una lista apilamos el diccionario recien leido
                if(tipo == 2): lista.append(diccionario)
                # Si no, 3 opciones: Lo anterior sea una lista,un diccionario o un valor literal.
                else:
                    a_is_fixed = False
                    if(c_ant != "]")&(c_ant != "}"): #Para el valor literal, simplemente lo leemos
                        b = raw[iniciob:i]
                    diccionario[a] =  b #Para lista y diccionario cogemos b que ya guarda ese objeto
                    inicio = i+1 #Siempre movemos el puntero inicio para que pueda leer otra key

            if(c =="{"):
                b,i = self.convertir_a_diccionario(raw,i+1,1)

                if(tipo != 2): #Si no estamos en una lista, añadimos el nuevo diccionario
                    diccionario[a]=b
                else: #Si estamos en una lista, este es un elemento de ella.
                    diccionario  = b
                inicio = i
                a_is_fixed = False

            if(c =="}"):
                if(c_ant != "]")&(c_ant != "}"): #Si era una lista el elemento no se cogen los literales
                    b = raw[iniciob:i]
                diccionario[a] = b
                return diccionario, i

            if(c == '['):
                b,i = self.convertir_a_diccionario(raw,i+1,2)
                inicio = i
                a_is_fixed = False

            if(c == ']'):
                lista.append(diccionario)
                return lista, i

            i+=1
        return diccionario
        
    def dic_to_df(self,dic):

        mañana = (datetime.date.today()+datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        #Sacamos los datos de hoy
        i_dia = -1
        for i in range(len(dic["prediccion"]["dia"])):
            fecha = dic["prediccion"]["dia"][i]["fecha"][:10]
            if(fecha == mañana):
                i_dia = i
                break
        datos = dic["prediccion"]["dia"][i_dia]

        #Viento & Direccion
        #Periodos de 6 horas
        # Hacemos la media para la velocidad y cogemos la direccion del periodo de mayor velocidad
        count = 0
        agg = 0
        direccion = ''
        max_velocidad = -1
        for elem in datos["viento"]:
            hora_ini, hora_fin = elem["periodo"].split("-")
            velocidad = int(elem["velocidad"])
            if(int(hora_fin)-int(hora_ini)) <=6:
                count +=1
                agg+= velocidad
                if(velocidad > max_velocidad):
                    max_velocidad = velocidad
                    direccion = elem["direccion"]
        viento = agg/count

        #Temperatura
        #Periodos de 6 horas, hacemos la media
        count = 0
        agg = 0
        for elem in datos["temperatura"]["dato"]:
            count +=1
            agg+= int(elem["value"])
        temp = agg/count

        #Prob-Precipitacion
        # Periodos de 6 horas - Cogemos el valor máximo
        count = 0
        agg = 0
        max_probabilidad = 0.0
        for elem in datos["probPrecipitacion"]:
            hora_ini, hora_fin = elem["periodo"].split("-")
            probabilidad = int(elem["value"])
            if(int(hora_fin)-int(hora_ini)) <=6:
                if(probabilidad > max_probabilidad):
                    max_probabilidad= probabilidad

        diccionarios = []
        diccionarios.append({"FECHA" : mañana,"VIENTO" : viento,"DIRECCION" : direccion,
                            "TEMPERATURA" : temp , "PRESION": -1.0,
                            "PROBPRECIPITACION" : float(max_probabilidad)})

        # Schema for the new DF
        data_schema = [StructField('FECHA',StringType(), True), #Tercer argumento = nullable
                       StructField('VIENTO', FloatType(), True),
                       StructField('DIRECCION', StringType(), True),
                       StructField('TEMPERATURA', FloatType(), True),
                       StructField('PRESION', FloatType(), True),
                       StructField('PROBPRECIPITACION', FloatType(), True)
                      ]

        return self.spark.createDataFrame(diccionarios,schema = StructType(data_schema)) 
    
    def data_to_sparkdf(self,data):
        #Encoding "ISO-8859"
        data_v = data.decode(encoding ='ISO-8859-15')
        data_v0 = data_v

        # Clean the data
        # Step 0 - Acotamos final e inicio
        for i in range(50):
            if(data_v0[i]=='{'):
                data_v0 = data_v0[i+1:]
                break
        for i in range(50):
            if(data_v0[-i]=='}'):
                data_v0 = data_v0[:-i]   
                break

        # Step 1 - Saltos de linea    
        data_v1 = data_v0
        data_v1 = data_v1.replace("\n", "")

        # Step 2 - Evitar problemas -> };
        data_v2 = data_v1.replace("},","};")

        # Step 3 - Espacios en blanco
        patron =['\s','\s"','"\s','\s{',':/']
        replace = ['','"','"','{','/']

        data_v3 = data_v2
        for i in range(len(patron)):
            data_v3 = reg.sub(patron[i],replace[i],data_v3)

        # Step 4 - Separadores -> ;
        data_v4 = data_v3.replace("\",\"","\";\"")

        #Step 5 - Comillas
        data_clean = data_v4.replace("\"", "")

        diccionario = self.convertir_a_diccionario(data_clean,0,0)

        #Sacamos los datos que queremos
        return self.dic_to_df(diccionario)
    
    def req_to_df(self,codigo):
        print("PREDICCIONES ZONA: ", codigo)
        try:
            api_response = self.api_predicciones.prediccin_por_municipios_diaria__tiempo_actual_(codigo)
            pprint(api_response)
        except ApiException as e:
            print("Exception: %s\n" % e)
        r = requests.get(api_response.datos)
        data = r.content
        df_aemet = self.data_to_sparkdf(data)
        print ("OK")
        return df_aemet
    
    def datos_predicciones_aemet(self,codigos_zonas):
        lista_df =[]
        for codigo in codigos_zonas:
            lista_df.append(self.req_to_df(codigo))
        #Unimos
        df = lista_df[0]
        for i in range(1,len(lista_df)):
            df = df.union(lista_df[i])
        return df  
    
    @staticmethod
    def dir_to_grad(direccion):
        if(direccion == 'E'): return 0
        if(direccion == 'NE'): return 45
        if(direccion == 'N'): return 90
        if(direccion == 'NO'): return 135
        if(direccion == 'O'): return 180
        if(direccion == 'SO'): return 225
        if(direccion == 'S'): return 270
        if(direccion == 'SE'): return 315
        if(direccion == 'C'): return None  
        
        
    """
    
        FUNCIONES PRINCIPALES <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    
    
    """   
    def __init__(self):
        self.spark = SparkSession.builder.appName('clima_prediccion').getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        
        #API Aemet
        f = open("/home/rulicering/Datos_Proyecto_Ozono/Credenciales/Credenciales.json")
        credenciales = json.load(f)
        AEMET_API_KEY = credenciales["aemet"]["api_key"]
        configuration = swagger_client.Configuration()
        configuration.api_key['api_key'] = AEMET_API_KEY
        self.api_predicciones = swagger_client.PrediccionesEspecificasApi(swagger_client.ApiClient(configuration))
    
    def process(self):
        self.prediccion()
        self.carga()
        
    def prediccion(self):
        #Código zona MADRID CIUDAD
        codigos_zonas = ["28079"]
        df_predicciones = self.datos_predicciones_aemet(codigos_zonas)
        
        df_predicciones = df_predicciones.withColumn("ANO",df_predicciones["FECHA"][0:4])
        df_predicciones = df_predicciones.withColumn("MES",df_predicciones["FECHA"][6:2])
        df_predicciones = df_predicciones.withColumn("DIA",df_predicciones["FECHA"][9:2])
        df_predicciones = df_predicciones.withColumn("FECHA",F.concat(df_predicciones["FECHA"][0:4],df_predicciones["FECHA"][6:2],df_predicciones["FECHA"][9:2]))
        
        my_udf = F.udf(ClimaPrediccion.dir_to_grad,IntegerType())
        df_predicciones = df_predicciones.withColumn("DIRECCION",my_udf(df_predicciones["DIRECCION"]))
        
        #Rename
        pd_predicciones = df_predicciones.toPandas()
        pd_predicciones =pd_predicciones.rename(columns={ "VIENTO":"81",                         
                                                   "DIRECCION":"82",
                                                   "TEMPERATURA":"83",     
                                                   "PRESION":"87",
                                                   "PROBPRECIPITACION":"%89",
                                             })
        #Tipos
        pd_predicciones["ANO"] =pd_predicciones["ANO"].astype(int)
        pd_predicciones["MES"] =pd_predicciones["MES"].astype(int)
        pd_predicciones["DIA"] =pd_predicciones["DIA"].astype(int)
        pd_predicciones["FECHA"] =pd_predicciones["FECHA"].astype(int)
        
        #Columnas
        cols = pd_predicciones.columns.tolist()
        cols = cols[0:1]+ cols[-3:] + cols[1:-3]
        self.pd_predicciones = pd_predicciones[cols]
    
    def carga(self):
        pd_final = self.pd_predicciones
        nuevo = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        anterior = datetime.date.today().strftime("%Y-%m-%d")
        
        #BackUp
        pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/BackUp/Clima_Prediccion-"+ nuevo + ".csv")
        pd_final.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima_Prediccion-"+ nuevo + ".csv")
        print("[INFO] - Clima_Prediccion-"+ nuevo +".csv --- Created successfully")
        
        #Borrar la de ayer
        try:
            os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Clima/Clima_Prediccion-"+ anterior + ".csv")
            print("[INFO] - Clima_Prediccion-"+ anterior +".csv --- Removed successfully")
        except:
            print("[ERROR] - Clima_Prediccion-"+ anterior +".csv --- Could not been removed")


# In[ ]:


clima_prediccion = ClimaPrediccion()
clima_prediccion.process()

