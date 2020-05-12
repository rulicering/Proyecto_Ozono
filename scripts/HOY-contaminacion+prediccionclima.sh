#!/bin/bash

HOY=$(date  +%Y-%m-%d)
LOG_HOY="/home/rulicering/Datos_Proyecto_Ozono/Logs/Diario/log-$HOY.txt"
LOG="/home/rulicering/Datos_Proyecto_Ozono/Logs/log.txt"

etl_hoy_contaminacionyprediccion(){
printf "\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>  $HOY  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n"
echo "---------------------------ESTACIONES---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Estaciones/ETL_Estaciones.py
echo "---------------------------CONTAMINACION HOY---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Contaminacion/ETL_Contaminacion_Dia_Actual.py
echo "---------------------------PREDICCION CLIMA MAÑANA---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Clima/ETL_Clima_Prediccion.py
}

etl_hoy_contaminacionyprediccion 2>&1 | tee -a $LOG -a $LOG_HOY >/dev/null
