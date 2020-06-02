#!/usr/bin/env python
# coding: utf-8

# # [o3] - Proyecto Ozono - ETL_Estaciones_[STRC]

# # [0] - Inicialización

# In[ ]:


from __future__ import print_function
import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
import pandas as pd
pd.options.mode.chained_assignment = None
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pyspark.sql import functions as F
import math

#AEMET
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint

#AUX
import datetime
import requests,json
import re as reg
import os


# In[ ]:


class Estaciones():
    """
    
        FUNCIONES AUXILIARES<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    
    
    """
    def haversine(self,p0,p1):
        lat1, lon1 = round(float(p0[0]),6),round(float(p0[1]),6)
        lat2, lon2 = round(float(p1[0]),6),round(float(p1[1]),6)

        rad=math.pi/180
        dlat=lat2-lat1
        dlon=lon2-lon1
        R=6372.795477598
        a=(math.sin(rad*dlat/2))**2 + math.cos(rad*lat1)*math.cos(rad*lat2)*(math.sin(rad*dlon/2))**2
        distancia=2*R*math.asin(math.sqrt(a))

        #Devuelve distancia en grados
        return distancia

    def degrees_to_decimal(self,elem):
        elem = reg.sub('N|E','1',elem)
        elem = reg.sub('S|W','-1',elem)
        return (float(elem[6:])* (float(elem[0:2]) + float(elem[2:4])/60 + float(elem[4:6])/3600))

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
        
        data_v2 = data_v1.replace("},","}},")

        patron =['\s\s','\s"','"\s','\s{']
        replace = [' ','"','"','{']
        data_v3 = data_v2
        for i in range(len(patron)):
            data_v3 = reg.sub(patron[i],replace[i],data_v3)
        data_v4 = data_v3.replace("\",\"","\";\"")

        data_cleaned = data_v4.split("},")

        # String to List of dictionaries
        diccionarios = []
        for fila in data_cleaned:
            keys = []
            values = []
            for pareja in fila[1:-1].split(';'):
                elems =pareja.split(':')
                keys.append(elems[0][1:-1])
                values.append(elems[1][1:-1])
            diccionarios.append(dict(zip(keys,values)))

        # Schema for the new DF
        data_schema = [StructField('latitud',StringType(), True),
                       StructField('provincia', StringType(), True),
                       StructField('altitud', StringType(), True),
                       StructField('indicativo', StringType(), True),
                       StructField('nombre', StringType(), True),
                       StructField('indsinop', StringType(), True),
                       StructField('longitud', StringType(), True)                    
                      ]
        # Create and return the new DF
        return self.spark.createDataFrame(diccionarios,schema = StructType(data_schema))  
    
    def data_to_sparkdf_HOY(self,data):
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
        data_schema = [StructField('idema',StringType(), True), 
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
        api_response = self.api_observacion.datos_de_observacin__tiempo_actual_1(codigo)
        pprint(api_response)
        r = requests.get(api_response.datos)
        data = r.content
        df_aemet = self.data_to_sparkdf_HOY(data)
        print("OK")

        return df_aemet.select('idema','fint','prec','pres','tamax','tamin','dv','vv')
    
    def datos_aemet_hoy(self,codigos_estaciones):
        lista_df =[]
        for codigo in codigos_estaciones:
            lista_df.append(self.req_hoy_to_df(codigo))
        #Unimos
        df_r = lista_df[0]
        for i in range(1,len(lista_df)):
            df_r = df_r.union(lista_df[i])
        return df_r

    def lista_diccionarios_estaciones(self,pd):
        return [dict(CODIGO = codigo,LATITUD = latitud,LONGITUD = longitud) 
                            for codigo,latitud,longitud 
                            in zip(pd["CODIGO_CORTO"].values,pd["LATITUD"].values,pd["LONGITUD"].values)] 
    
        """
    PARAMETROS:
    objetivos : lista de diccionarios->(CODIGO,LAT,LONG),
                            de las estaciones objetivo.

    estacion: diccionario de la estación actual -> (CODIGO,LAT,LONG)
    """
    def objetivo_mas_cercano(self,objetivos,punto):
        objetivo_mas_cercano = None
        menor_distancia = 9999999
        for objetivo in objetivos:
            if(objetivo["CODIGO"] == punto["CODIGO"]):
                objetivo_mas_cercano = punto["CODIGO"]
                break
            else:
                distancia = self.haversine([objetivo["LATITUD"],objetivo["LONGITUD"]],
                                      [punto["LATITUD"],punto["LONGITUD"]])
                if(distancia < menor_distancia):
                    objetivo_mas_cercano = objetivo["CODIGO"]
                    menor_distancia = distancia
        return objetivo_mas_cercano

    def agrupamiento_AEMET(self,pd):
        #Sacamos las estaciones de CLIMA
        pd_aemet = pd[pd["MIDE_CLIMA_AEMET"]>0]

        #Sacar un diccionario de : Estacion,latitud,longitud
        estaciones_clima = self.lista_diccionarios_estaciones(pd_aemet)

        #Para todas las estaciones, le asignamos la de clima mas cercana
        pd["E_AEMET"] = [self.objetivo_mas_cercano(estaciones_clima,estacion) 
                       for estacion in self.lista_diccionarios_estaciones(pd)]
        return pd
    

        """
    FUNCIONAMIENTO:
    Si la estacion es de AIRE:

        -> Devuelve la estación de CLIMA más cercana que lea esa magnitud

    PARAMETROS:
    estaciones_magnitud: lista de diccionarios->(CODIGO,LAT,LONG),
                            de las estaciones que miden esa magnitud.

    estaciones_aire: lista de las estaciones que miden aire -> (CODIGO_CORTO)

    estacion: diccionario de la estación actual -> (CODIGO,LAT,LONG)
    """

    def clima_mas_cercana(self,estaciones_magnitud,estaciones_aire,estacion):
        if(estacion["CODIGO"] in estaciones_aire):
            return str(self.objetivo_mas_cercano(estaciones_magnitud,estacion))
        else:
            return None
        
    def agrupamiento_xmagnitud(self,pd,aemet= None,ayunt = None):

        #DIARIO
        if(aemet is not None):
            magnitudes = aemet.columns[1:]
        elif(ayunt is not None):
            magnitudes = ayunt.columns[1:]
        else: 
            raise Exception("[ERROR] - agrupamiento_xmagnitud(): Ambos datasets no pueden ser nulos")

        #Diccionario: Magnitud:[Lista de estaciones que la leen]
        estacionesxmagnitud = {}
        for magnitud in magnitudes:
            estacionesxmagnitud.update({magnitud:[]})
            #Para cuando solo se utilicen las estaciones de la aemet
            if(ayunt is not None): 
                for est_ayunt in (ayunt[ayunt[magnitud]=="X"]["CODIGO_CORTO"].values): 
                    estacionesxmagnitud[magnitud].append(str(est_ayunt))
            if(aemet is not None):
                for est_aemet in (aemet[aemet[magnitud]=="X"]["CODIGO_CORTO"].values): 
                    estacionesxmagnitud[magnitud].append(str(est_aemet))    

        #Lista de estaciones que miden aire
        l_estaciones_aire = [elem for elem in pd[pd["MIDE_AIRE"]>0]["CODIGO_CORTO"].values]

         #Todas las estaciones
        pd = pd.sort_values(by ="CODIGO_CORTO")
        estaciones = self.lista_diccionarios_estaciones(pd)

        for magnitud in magnitudes :

            #Estaciones que leen esa magnitud
            pd_magnitud = pd[pd["CODIGO_CORTO"].isin(estacionesxmagnitud[magnitud])]
            estaciones_magnitud = self.lista_diccionarios_estaciones(pd_magnitud)

            pd['E_%s' % magnitud] = [self.clima_mas_cercana(estaciones_magnitud,l_estaciones_aire,estacion) 
                            for estacion in estaciones]
        return pd
    
    
    """
    
        FUNCIONES PRINCIPALES <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    
    
    """
    def __init__(self):
        self.spark = SparkSession.builder.appName('estaciones').getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        f = open("/home/rulicering/Datos_Proyecto_Ozono/Credenciales/Credenciales.json")
        credenciales = json.load(f)
        self.AEMET_API_KEY = credenciales["aemet"]["api_key"]
        self.magnitudes_clima = ['81', '82', '83', '87', '89']
        
    def process(self):
        self.ayuntamiento()
        self.aemet()
        self.merge()
        self.agrupamiento()
        self.columnas_uso()
        self.carga()
        
    def ayuntamiento(self):
        #Aire
        pd_aire_excel = pd.read_excel("https://datos.madrid.es/egob/catalogo/212629-0-estaciones-control-aire.xls")
        
        pd_aire = pd_aire_excel[['CODIGO_CORTO', 'ESTACION', 'DIRECCION', 'ALTITUD','LONGITUD', 'LATITUD']]
        pd_aire.insert(6,"MIDE_AIRE",1)
        pd_aire["CODIGO_CORTO"] = pd_aire[["CODIGO_CORTO"]].astype(str)
        
        print("[INFO] - Número de estaciones medición calidad del aire: %d" % pd_aire["ESTACION"].count())
        
        #Clima
        pd_clima_excel = pd.read_excel("https://datos.madrid.es/egob/catalogo/300360-0-meteorologicos-estaciones.xls")
        self.pd_clima_excel = pd_clima_excel
        
        pd_clima = pd_clima_excel[['CÓDIGO_CORTO', 'ESTACIÓN', 'DIRECCION', 'ALTITUD','LONGITUD', 'LATITUD']]
        pd_clima.insert(6,"MIDE_CLIMA",1)
        pd_clima = pd_clima.rename(columns = {"CÓDIGO_CORTO" : "CODIGO_CORTO",'ESTACIÓN': 'ESTACION', 'DIRECCIÓN':'DIRECCION'})
        pd_clima["CODIGO_CORTO"] = pd_clima[["CODIGO_CORTO"]].astype(str)
        
        print("[INFO] - Número de estaciones medición clima ayuntamiento: %d" % pd_clima["ESTACION"].count())
        
        #Estaciones Ayuntamiento = AIRE  CLIMA
        #No se puede hacer el merge por mas campos porque no son exactamente iguales por lo que hay que filtrar
        pd_estaciones_ayunt = pd_aire.merge(pd_clima, on =['CODIGO_CORTO'], how = "outer")
        pd_estaciones_ayunt = pd_estaciones_ayunt.fillna({"MIDE_AIRE":0,"MIDE_CLIMA":0})
        pd_estaciones_ayunt = pd_estaciones_ayunt.fillna("Nulo")
        
        #Creamos columnas nuevas mergeando las _X y _y 
        columnas = pd_estaciones_ayunt.columns.to_list()
        columnas_x = list(filter(None,[columna if "_x" in columna else None 
                                       for columna in columnas]))
        columnas_y = list(filter(None,[columna if "_y" in columna else None 
                                       for columna in columnas]))
        #Creamos las nuevas columnas mergeando 
        for x,y in zip(columnas_x,columnas_y):
            pd_estaciones_ayunt[x[:-2]] = [b if(a == "Nulo") else a 
                                             for a,b in zip(pd_estaciones_ayunt[x],
                                                            pd_estaciones_ayunt[y])]
        #Quitamos las columnas duplicadas por el merge _x y _y
        columnas = pd_estaciones_ayunt.columns.to_list()
        columnas_a_coger = list(filter(None,
                                       [None if "_x" in columna or "_y" in columna else columna 
                                        for columna in columnas]))
        pd_estaciones_ayunt = pd_estaciones_ayunt[columnas_a_coger]
        
        pd_estaciones_ayunt["CODIGO_CORTO"] = pd_estaciones_ayunt["CODIGO_CORTO"].astype(str)
        pd_estaciones_ayunt = pd_estaciones_ayunt[['CODIGO_CORTO', 'ESTACION', 'DIRECCION',
                                                   'ALTITUD', 'LONGITUD', 'LATITUD', 'MIDE_AIRE', 'MIDE_CLIMA']]
        
        print("[INFO] - TOTAL ESTACIONES AYUNT: ", pd_estaciones_ayunt[["CODIGO_CORTO"]].count())
        
        pd_estaciones_ayunt["MIDE_CLIMA"] = pd_estaciones_ayunt["MIDE_CLIMA"].round(1).astype(int)
        pd_estaciones_ayunt["MIDE_AIRE"] = pd_estaciones_ayunt["MIDE_AIRE"].round(1).astype(int)
        
        self.pd_estaciones_ayunt = pd_estaciones_ayunt
    
    def aemet(self):
        configuration = swagger_client.Configuration()
        configuration.api_key['api_key'] = self.AEMET_API_KEY
        self.api_valores = swagger_client.ValoresClimatologicosApi(swagger_client.ApiClient(configuration))
        self.api_observacion = swagger_client.ObservacionConvencionalApi(swagger_client.ApiClient(configuration))
        
        #Estaciones clima Aemet
        try:
            api_response = self.api_valores.inventario_de_estaciones__valores_climatolgicos_()
            print(api_response)
        except ApiException as e:
            print("Exception: %s\n" % e)
        
        r = requests.get(api_response.datos)
        data = r.content
        df = self.data_to_sparkdf(data)
        
        pd_estaciones_aemet = df.filter(df["PROVINCIA"]=="MADRID").orderBy("indicativo").toPandas()
        
        pd_estaciones_aemet["DIRECCION"] = pd_estaciones_aemet["nombre"]+ "-" + pd_estaciones_aemet["provincia"]
        pd_estaciones_aemet.insert(7,"MIDE_CLIMA_AEMET",1)
        pd_estaciones_aemet["LONGITUD"] = [self.degrees_to_decimal(elem) for elem in pd_estaciones_aemet["longitud"]]
        pd_estaciones_aemet["LATITUD"] = [self.degrees_to_decimal(elem) for elem in pd_estaciones_aemet["latitud"]]
        
        pd_estaciones_aemet = pd_estaciones_aemet[["indicativo","nombre","DIRECCION","altitud","LONGITUD","LATITUD","MIDE_CLIMA_AEMET"]]
        pd_estaciones_aemet = pd_estaciones_aemet.rename(columns = {"indicativo":"CODIGO_CORTO",
                                          "nombre": "ESTACION",
                                          "altitud": "ALTITUD"})
        pd_estaciones_aemet["CODIGO_CORTO"] = pd_estaciones_aemet["CODIGO_CORTO"].astype(str)
        pd_estaciones_aemet["ALTITUD"] = pd_estaciones_aemet["ALTITUD"].astype(str).astype(int)
        
        #Descartamos las estaciones fuera del area de Madrid Ciudad (15KM)
        pd_estaciones_aemet["LAT_LONG"]= round(pd_estaciones_aemet["LATITUD"],6).astype(str) +','+ round(pd_estaciones_aemet["LONGITUD"],6).astype(str)
        centro_de_madrid = [40.4165,-3.702561]
        pd_estaciones_aemet["MADRID_CIUDAD"] = [1 if self.haversine(x.split(','),centro_de_madrid) <= 15 else 0 for x in (pd_estaciones_aemet["LAT_LONG"])]

        pd_estaciones_aemet = pd_estaciones_aemet[pd_estaciones_aemet["MADRID_CIUDAD"]==1]
        pd_estaciones_aemet = pd_estaciones_aemet[['CODIGO_CORTO', 'ESTACION', 'DIRECCION', 'ALTITUD', 'LONGITUD','LATITUD', 'MIDE_CLIMA_AEMET']]
        
        print("[INFO] - TOTAL ESTACIONES AEMET: ", pd_estaciones_aemet[["CODIGO_CORTO"]].count())
        
        self.pd_estaciones_aemet = pd_estaciones_aemet
        
        #Estaciones clima AEMET [HOY]
        
        print("[INFO] - Obtenemos la lista de estaciones AEMET que miden datos HOY")
        #Todos los datos actuales de todas las estaciones de la aemet
        try:
            api_response = self.api_observacion.datos_de_observacin__tiempo_actual_()
            pprint(api_response)
        except ApiException as e:
            print("Exception: %s\n" % e)

        r = requests.get(api_response.datos)
        data = r.content
        df_aemet = self.data_to_sparkdf_HOY(data)

        cod_estaciones_aemet_madrid = list(pd_estaciones_aemet["CODIGO_CORTO"].values)
        cod_todas_estaciones_aemet_hoy = [elem[0] for elem in df_aemet.select("idema").distinct().collect()]
        cod_estaciones_aemet_hoy = set(list([elem if elem in cod_estaciones_aemet_madrid else "-1"
                                         for elem in cod_todas_estaciones_aemet_hoy]))
        cod_estaciones_aemet_hoy.remove("-1")

        print("[INFO] - Códigos estaciones AEMET hoy - " + str(cod_estaciones_aemet_hoy))
        print("[INFO] - Obtenemos datos de clima de HOY para dichas estaciones")
        df_aemet_hoy = self.datos_aemet_hoy(cod_estaciones_aemet_hoy)
        
        magnitudes_aemet = df_aemet_hoy.columns[2:]
        for columna in magnitudes_aemet:
            df_aemet_hoy = df_aemet_hoy.withColumn(columna,df_aemet_hoy[columna].cast(FloatType()))
        
        self.df_aemet_hoy = df_aemet_hoy
        
    def merge(self):
        pd_estaciones = self.pd_estaciones_ayunt.merge(self.pd_estaciones_aemet,how='outer')
        pd_estaciones = pd_estaciones.fillna(0)
        pd_estaciones["MIDE_CLIMA_FINAL"] = pd_estaciones["MIDE_CLIMA"] + pd_estaciones["MIDE_CLIMA_AEMET"]
        
        self.pd_estaciones = pd_estaciones
        
    def agrupamiento(self):
        #Estacion Aemet + cercana
        pd_estaciones = self.agrupamiento_AEMET(self.pd_estaciones)
        pd_estaciones = pd_estaciones[["CODIGO_CORTO","ESTACION","LATITUD", "LONGITUD","E_AEMET","MIDE_AIRE","MIDE_CLIMA","MIDE_CLIMA_AEMET","MIDE_CLIMA_FINAL"]]
        
        #Agrupacion x magnitud (Utilizamos todas las estaciones de clima disponibles)
        
        #DF ESTACIONES AYUNTAMIENTO + MAGNITUDES LEIDAS
        magnitudes_estaciones_ayunt = self.pd_clima_excel[['CÓDIGO_CORTO','VV (81)', 'DV (82)', 'T (83)','PB (87)', 'P (89)']]
        magnitudes_estaciones_ayunt = magnitudes_estaciones_ayunt.rename(columns={'CÓDIGO_CORTO':'CODIGO_CORTO',
                                                                                 'VV (81)':"81",
                                                                                 'DV (82)':"82",
                                                                                 'T (83)':"83",
                                                                                 'PB (87)':"87",
                                                                                 'P (89)':"89"
                                                                                })
        ayunt = magnitudes_estaciones_ayunt.fillna("0")
        
        #DF ESTACIONES AEMET + MAGNITUDES LEIDAS
        magnitudes_estaciones_aemet = pd_estaciones[pd_estaciones["MIDE_CLIMA_AEMET"]>0][["CODIGO_CORTO"]]

        magnitudes_estaciones_aemet["81"] = "X"
        magnitudes_estaciones_aemet["82"] = "X"
        magnitudes_estaciones_aemet["83"] = "X"
        magnitudes_estaciones_aemet["87"] = "X"
        magnitudes_estaciones_aemet["89"] = "X"

        aemet = magnitudes_estaciones_aemet
        
        pd_estaciones = self.agrupamiento_xmagnitud(pd_estaciones,aemet,ayunt)
        
        #Agrupación x magnitud para DIA ACTUAL (Solo utilizamos las estaciones de AEMET)
        #magnitudes_estaciones_aemet_hoy
        aux = self.df_aemet_hoy.withColumn("DIA",self.df_aemet_hoy["fint"][9:2])
        aux = aux.filter(aux["DIA"]=='%02d' % datetime.date.today().day)

        aux = aux.groupBy("idema").mean().toPandas()

        aux = aux.rename(columns={'idema':'CODIGO_CORTO','avg(vv)':"AEMET_HOY_81",'avg(dv)':"AEMET_HOY_82",
                                  'avg(tamax)':"AEMET_HOY_83_MAX",'avg(tamin)':"AEMET_HOY_83_MIN",
                                  'avg(pres)':"AEMET_HOY_87",'avg(prec)':"AEMET_HOY_89"})
        columnas = aux.columns[1:]
        for columna in columnas:
            aux[columna] = ["X" if pd.notna(elem) else "-" for elem in aux[columna]]

        #TEMPERATURA = TEMPERTURA MAX & MIN
        aux["AEMET_HOY_83"] =["X" if (tmax == "X") & (tmin == "X") else "-"
                          for tmax,tmin in zip(aux["AEMET_HOY_83_MAX"].values,aux["AEMET_HOY_83_MIN"].values)]

        aux = aux.drop(columns= ["AEMET_HOY_83_MIN","AEMET_HOY_83_MAX"])
        aemet_hoy = aux
        
        print("[INFO] - Magnitudes leidas por estaciones Aemet HOY") 
        aemet_hoy
        
        pd_estaciones = self.agrupamiento_xmagnitud(pd_estaciones,aemet_hoy)
        
        #Nulos
        pd_estaciones = pd_estaciones.fillna("-1")
        print("TOTAL ESTACIONES: ", pd_estaciones[["CODIGO_CORTO"]].count())
        
        self.pd_estaciones = pd_estaciones
        
    def columnas_uso(self):
        pd_estaciones = self.pd_estaciones
        pd_estaciones_aire = pd_estaciones[pd_estaciones["MIDE_AIRE"]>0]
        
        #Utilizada AEMET
        u_aemet = set(pd_estaciones_aire["E_AEMET"].values)
        pd_estaciones["U_AEMET"] = [elem in u_aemet for elem in pd_estaciones["CODIGO_CORTO"]]
        
        #Utilizada HOY
        l_u_hoy = [pd_estaciones_aire["E_AEMET_HOY_%s" % magnitud].values 
                   for magnitud in self.magnitudes_clima]
        u_aemet_hoy = set([estacion for sublist in l_u_hoy for estacion in sublist])
        pd_estaciones["U_AEMET_HOY"] = [elem in u_aemet_hoy for elem in pd_estaciones["CODIGO_CORTO"]]
        
        #Utilizada TODAS
        l_u_todas = [pd_estaciones_aire['E_%s' % magnitud].values 
                   for magnitud in self.magnitudes_clima]
        u_todas = set([estacion for sublist in l_u_todas for estacion in sublist])
        pd_estaciones["U_TODAS"] = [elem in u_todas for elem in pd_estaciones["CODIGO_CORTO"]]
        
        self.pd_estaciones = pd_estaciones
        
    def carga(self):
        hoy = datetime.date.today().strftime("%Y-%m-%d")
        ayer = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        pd_estaciones = self.pd_estaciones
        
        #Diaria
        #BackUp
        pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/BackUp/Estaciones-" + hoy +".csv")
        pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + hoy +".csv")
        print("[INFO] - Estaciones-"+ hoy +".csv --- Created successfully")
        #Borrar la de ayer
        try:
            os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + ayer + ".csv")
            print("[INFO] - Estaciones-"+ ayer+".csv --- Removed successfully")
        except:
            print("[ERROR] - Estaciones-"+ ayer+".csv --- Could not been removed")
        
        # Estaciones mes cerrado
        if(datetime.date.today().day == 1):
            #Añadir la de este mes
            ult_dia_mes_cerrado = datetime.date.today()- datetime.timedelta(days=1)
            mes_cerrado = str(ult_dia_mes_cerrado.year*100 + ult_dia_mes_cerrado.month)
            
            pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + mes_cerrado +".csv")
            print("[INFO] - Estaciones-"+ mes_cerrado +".csv --- Created successfully")
            #BackUp de este mes
            pd_estaciones.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/BackUp/Estaciones-" + mes_cerrado +".csv")

            #Borrar la del mes anterior
            dia_mes_cerrado_anterior = datetime.date.today() - datetime.timedelta(days=32)
            mes_cerrado_anterior = str(dia_mes_cerrado_anterior.year*100+ dia_mes_cerrado_anterior.month)
            try:
                os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + mes_cerrado_anterior + ".csv")
            except:
                print("[ERROR] - Estaciones-"+ mes_cerrado_anterior+".csv --- Could not been removed")


# In[ ]:


#EJECUTAR
estaciones = Estaciones()
estaciones.process()

