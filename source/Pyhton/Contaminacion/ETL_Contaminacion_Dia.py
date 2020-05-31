#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - ETL_Contaminacion_Dia_[STRC]

# In[ ]:


from __future__ import print_function
import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,IntegerType,FloatType,StructType
from pyspark.ml.feature import StringIndexer
from pyspark.sql import functions as F
import pandas as pandas
import datetime
import requests
import io
import os


# In[ ]:


class contaminacion_dia():
    
    def __init__(self):
        self.spark = SparkSession.builder.appName('contaminacion_hoy').getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        self.df = None
    
    def process(self):
        self.extraccion()
        self.transformacion()
        self.carga()
        
    def extraccion(self):
        url =  "http://www.mambiente.madrid.es/opendata/horario.csv"
        hdr = {"Host": "www.mambiente.madrid.es",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Cookie": "MadridCookiesPolicy=; _hjid=0faf8542-e813-4294-b619-5eb313249e16",
            "Upgrade-Insecure-Requests": "1"}
        
        r = requests.get(url = url, headers = hdr)
        file_object = io.StringIO(r.content.decode(r.apparent_encoding))
        pd_aire_hoy = pandas.read_csv(file_object,sep=';')
        self.df = self.spark.createDataFrame(pd_aire_hoy)
    
    def transformacion(self):
        df = self.df.select('ESTACION','MAGNITUD','ANO','MES','DIA','H01','V01','H02','V02','H03','V03','H04','V04','H05','V05','H06','V06','H07','V07','H08','V08','H09','V09','H10','V10','H11','V11','H12','V12','H13','V13','H14','V14','H15','V15','H16','V16','H17','V17','H18','V18','H19','V19','H20','V20','H21','V21','H22','V22','H23','V23','H24','V24')
        data_schema = [StructField('ESTACION',IntegerType(), False), 
              StructField('MAGNITUD',IntegerType(), False),
              StructField('ANO',IntegerType(), False),
              StructField('MES',IntegerType(), False),
              StructField('DIA',IntegerType(), False),
              StructField('VALOR',FloatType(), True),
              StructField('VALIDO',IntegerType(), False),
              StructField('HORA',IntegerType(), False)]
        struct = StructType(fields = data_schema)
        
        df_v1 = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(),struct)
        for i in range(1,25): #Horas dia
            valor = 'H%02d' % i
            valido = 'V%02d' % i
            df_v1 = df_v1.union(df.select("ESTACION","MAGNITUD","ANO","MES","DIA",valor,valido).withColumn('HORA', F.lit(i)))
        df = df_v1
        
        df = df.withColumn("FECHA",df["ANO"]*10000 + df["MES"]*100 + df["DIA"])
        
        cols = df.columns
        cols = cols[:5] + cols[-2:] + cols[-4:-2]
        df= df[cols]
        
        df = df.withColumn("VALOR VALIDADO",F.when(F.col("VALIDO")== 'N',None).otherwise(F.col("VALOR")) )
        df = df.select("ESTACION","MAGNITUD","ANO","MES","DIA","HORA","FECHA",df["VALOR VALIDADO"].alias("VALOR"))
        
        df = df.groupBy('ESTACION','ANO', 'MES', 'DIA','HORA',"FECHA").pivot("MAGNITUD").sum("VALOR").orderBy("FECHA")
        
        df = df.withColumn("ESTACION",df["ESTACION"].cast(StringType()))
        
        pd = df.toPandas()
        pd = pd.rename(columns={"ESTACION":"CODIGO_CORTO"})
        pd_diaxhoras = pd
        
        #Limpiamos y ordenamos
        cols_magnitudes = pd_diaxhoras.columns.tolist()[6:]
        cols_magnitudes.sort()
        cols = pd_diaxhoras.columns.tolist()[0:6] + cols_magnitudes
        pd_diaxhoras = pd_diaxhoras[cols]
        pd_diaxhoras = pd_diaxhoras.sort_values(by=["ANO","MES","DIA","FECHA","HORA","CODIGO_CORTO"])
        
        self.pd_diaxhoras = pd_diaxhoras
        self.media_diaria()
        
    def media_diaria(self):
        pd_media_dia =  self.pd_diaxhoras.groupby(by=["ANO","MES","DIA","FECHA","CODIGO_CORTO"]).agg('mean')
        
        #Quitamos la columna "HORA"
        cols = pd_media_dia.columns
        cols = cols[1:] 
        self.pd_media_dia = pd_media_dia[cols]
        
    def carga(self):
	pd_diaxhoras = self.pd_diaxhoras
        pd_media_dia = self.pd_media_dia

        hoy = datetime.date.today().strftime("%Y-%m-%d")
        ayer = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        
        #Dia x Horas
        #BackUp
        pd_diaxhoras.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/BackUp/Contaminacion_diaxhoras-" + hoy +".csv")    
        pd_diaxhoras.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_diaxhoras-"+ hoy +".csv")
        print("[INFO] - Contaminacion_diaxhoras-"+ hoy +".csv --- Created successfully")
        #Borrar la de ayer
        try:
            os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_diaxhoras-"+ ayer +".csv")
            print("[INFO] - Contaminacion_diaxhoras-"+ ayer +".csv --- Removed successfully")
        except:
            print("[ERROR] - Contaminacion_diaxhoras-"+ ayer +".csv --- Could not been removed")
        
        #Media dia
        #BackUp
        pd_media_dia.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/BackUp/Contaminacion_mediadia-" + hoy +".csv",float_format = '%.8f')   
        pd_media_dia.to_csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mediadia-" + hoy +".csv",float_format = '%.8f')
        print("[INFO] - Contaminacion_mediadia-"+ hoy +".csv --- Created successfully")
        #Borrar la de ayer
        try:
            os.remove("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Contaminacion/Contaminacion_mediadia-" + ayer +".csv")
            print("[INFO] - Contaminacion_mediadia-"+ ayer+".csv --- Removed successfully")
        except:
            print("[ERROR] - Contaminacion_mediadia-"+ ayer+".csv --- Could not been removed")
        


# In[ ]:


#EJECUTAR
contaminacion_hoy = contaminacion_dia()
contaminacion_hoy.process()

