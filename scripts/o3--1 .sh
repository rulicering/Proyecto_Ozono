#!/bin/bash

HOY=$(date  +%Y-%m-%d)
LOG_HOY="/home/rulicering/Datos_Proyecto_Ozono/Logs/Diario/log-$HOY.txt"
LOG="/home/rulicering/Datos_Proyecto_Ozono/Logs/log.txt"

etl_hoy_contaminacionyprediccion(){
printf "\n\n>===========================  $HOY ============================================\n\n"
printf "\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>  P.1 - EX TIME : 23:50 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n"
echo "---------------------------[HOY] - ESTACIONES---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Estaciones/ETL_Estaciones.py
echo "---------------------------[HOY] - CONTAMINACION ---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Contaminacion/ETL_Contaminacion_Dia.py
echo "---------------------------[MAÑANA] - PREDICCION CLIMA---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Clima/ETL_Clima_Prediccion.py
}

etl_hoy_contaminacionyprediccion 2>&1 | tee -a $LOG -a $LOG_HOY >/dev/null
