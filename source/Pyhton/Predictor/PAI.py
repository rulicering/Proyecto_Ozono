#!/usr/bin/env python
# coding: utf-8

# # [o3]- Proyecto Ozono - Prediccion a imagen_[STRC]

# # [0] - Inicialización

# In[ ]:


import findspark
findspark.init('/home/rulicering/BigData/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pandas
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,FloatType
import re as reg
import numpy as np
import datetime
import os
import seaborn as sns
import matplotlib
from matplotlib.colors import ListedColormap
import imgkit


# In[ ]:


class PAI():
    def __init__(self):
        self.spark = SparkSession.builder.appName('prediccion_a_imagen').getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        
        self.dic_magnitudes = { "1": {"formula":"SO<sub>2</sub>", "unidad": "μg/m³","limite": 125},
                  "6": {"formula":"CO", "unidad": "mg/m³","limite": 10},
                  "7": {"formula":"NO", "unidad": "μg/m³","limite": 200},
                  "8": {"formula":"NO<sub>2</sub>", "unidad": "μg/m³","limite": 200},
                  "9": {"formula":"PM2.5", "unidad": "μg/m³","limite": 50},
                  "10": {"formula":"PM10", "unidad": "μg/m³","limite": 50},
                  "12": {"formula":"NOx", "unidad": "μg/m³","limite": 200},
                  "14": {"formula":"O<sub>3</sub>", "unidad": "μg/m³","limite": 180},
                  "20": {"formula":"TOL", "unidad": "μg/m³","limite": None},
                  "30": {"formula":"BEN", "unidad": "μg/m³","limite": 5},
                  "35": {"formula":"EBE", "unidad": "μg/m³","limite": None},
                  "37": {"formula":"MXY", "unidad": "μg/m³","limite": None},
                  "38": {"formula":"PXY", "unidad": "μg/m³","limite": None},
                  "39": {"formula":"OXY", "unidad": "μg/m³","limite": None},
                  "42": {"formula":"TCH", "unidad": "mg/m³","limite": None},
                  "43": {"formula":"CH4", "unidad": "mg/m³","limite": None},
                  "44": {"formula":"NMHC", "unidad": "mg/m³","limite": None}  
                    }
        
        #paleta_5x10
        self.paleta = [(0.0919646289888505, 0.5776239907727797, 0.30411380238369873),
                         (0.3415609381007306, 0.712725874663591, 0.37362552864282983),
                         (0.5771626297577854, 0.8186851211072667, 0.40761245674740476),
                         (0.7803921568627452, 0.906805074971165, 0.49942329873125735),
                         (0.9327950788158401, 0.9717031910803539, 0.6570549788542867),
                         (0.9982314494425222, 0.9451749327181854, 0.6570549788542868),
                         (0.9946943483275664, 0.8092272202998847, 0.48696655132641264),
                         (0.981776239907728, 0.6073817762399077, 0.3457900807381774),
                         (0.9345636293733178, 0.38054594386774326, 0.24121491733948497),
                         (0.8239138792772011, 0.16978085351787778, 0.15255670895809303)]
        
     
    def process(self):
        self.carga()
        self.dar_formato()
        self.crear_tabla_con_estilo()
        self.exportar_a_imagen()
        
    def carga(self):
        hoy = datetime.date.today().strftime("%Y-%m-%d")
        ayer = (datetime.date.today() + datetime.timedelta(days = -1)).strftime("%Y-%m-%d")
        
        df_estaciones = self.spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Estaciones/Estaciones-" + ayer +".csv",inferSchema= True,header=True)
        df_prediccion = self.spark.read.csv("/home/rulicering/Datos_Proyecto_Ozono/Procesado/Predicciones/Prediccion-" + hoy + ".csv",inferSchema= True,header=True)
        
        df_estaciones = df_estaciones.drop("_c0")
        df_prediccion = df_prediccion.drop("_c0")
        
        #Añadimos el nombre de las estaciones
        cols = df_estaciones.columns[0:2]
        df_nombre_estaciones = df_estaciones.filter(df_estaciones["MIDE_AIRE"] > 0).select(cols)
        df_prediccion = df_nombre_estaciones.join(df_prediccion,on = "CODIGO_CORTO")
        self.pd_prediccion = df_prediccion.toPandas() 
        
    def dar_formato(self):
        pd_prediccion = self.pd_prediccion
        cols_magnitudes = pd_prediccion.columns.tolist()[2:]
        for col in cols_magnitudes:
            magnitud = col[2:]
            pd_prediccion = pd_prediccion.round({col:2})
            pd_prediccion = pd_prediccion.rename(columns={col:self.dic_magnitudes[magnitud]["formula"]})
    
        pd_prediccion = pd_prediccion.rename(columns={"CODIGO_CORTO":"COD",
                                              "ESTACION":"ESTACIÓN"})
        pd_prediccion["COD"] = pd_prediccion["COD"].astype(int)
        self.pd_prediccion = pd_prediccion.sort_values(by=["COD"])
        
    def _dar_color_fondo(self,val,limite):
        if(pandas.notna(val)):
            long_tramo = limite/9
            tramo =  int(val/long_tramo)
            tramo = tramo if tramo <= 9 else 9
            return 'background-color: %s' % matplotlib.colors.to_hex(self.paleta[tramo])
        else: return 'background-color: white'
    
    def crear_tabla_con_estilo(self):
        pd_prediccion = self.pd_prediccion
        
        #Nulos y formato celdas numéricas
        styled_table = pd_prediccion.style.highlight_null(null_color='white').                format("{:.4}", subset= pd_prediccion.columns.tolist()[2:], na_rep = "")
        #Ocultamos el indice
        styled_table2 = styled_table.hide_index()
        # Dar color de fondo
        for elem in self.dic_magnitudes:
            if(self.dic_magnitudes[elem]["limite"]):
                styled_table2.applymap(self._dar_color_fondo,
                                       limite = self.dic_magnitudes[elem]["limite"],
                                       subset=[self.dic_magnitudes[elem]["formula"]])
        #CSS
        #Formato headers
        styled_table3 = styled_table2.set_table_styles(     
            [{'props': [("font-family","Sawasdee"),
                       ("color","black"),
                       ("background-color", 'white'),
                       ("border-collapse", "collapse"),
                       ("text-align","center")
                       ]}    
                ,{'selector': f'th',
               'props': [('background-color', 'white'),
                        ("font-size","13px"),
                        ("font-weight","900"),
                        ("border"," 0px solid black"),
                        ("border-bottom-width","2px"),
                        ("column-width", "50px")
                        ]}
                ,{'selector': f'td',
               'props': [("font-size","12px"),
                        ("font-weight","600"),
                        ("border"," 2px solid white")
                        ]}

            ])
        self.styled_table = styled_table3
      
    def exportar_a_imagen(self):
        hoy = datetime.date.today().strftime("%Y-%m-%d")
        ayer = (datetime.date.today() + datetime.timedelta(days = -1)).strftime("%Y-%m-%d")
                
        html = '<meta charset="UTF-8">'+self.styled_table.render()
        config = imgkit.config(wkhtmltoimage='/usr/bin/wkhtmltopdf')
        
        #BackUp
        imgkit.from_string(html, "/home/rulicering/Datos_Proyecto_Ozono/Imagenes/BackUp/PAI-" + hoy + ".png")
        imgkit.from_string(html, "/home/rulicering/Datos_Proyecto_Ozono/Imagenes/PAI-" + hoy + ".png")
        print("[INFO] - PAI-"+ hoy+ ".png --- Generated successfully")
        
        #Borrar la de ayer
        try:
            os.remove("/home/rulicering/Datos_Proyecto_Ozono/Imagenes/PAI-" + ayer + ".png")
            print("[INFO] - PAI-"+ ayer +".png --- Removed successfully")
        except:
            print("[ERROR] - PAI-"+ ayer +".png --- Could not been removed")


# In[ ]:


#EJECUTAR
pai = PAI()
pai.process()

