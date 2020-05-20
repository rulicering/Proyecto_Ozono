#!/bin/bash

HOY=$(date  +%Y-%m-%d)
LOG_HOY="/home/rulicering/Datos_Proyecto_Ozono/Logs/Diario/log-$HOY.txt"
LOG="/home/rulicering/Datos_Proyecto_Ozono/Logs/log.txt"

etl_hoy_contaminacionyprediccion(){
printf "\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>  $HOY  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n"

printf "=============================== [ACT] - MES CERRADO  ==============================="
echo "---------------------------CLIMA MES CERRADO ---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Clima/ETL_CLima_Mes_Cerrado.py
echo "---------------------------CONTAMINACION MES CERRADO ---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Contaminacion/ETL_Contaminacion_Mes_Cerrado.py
echo "---------------------------JOIN AND ACTUALIZAR DATOS ---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Clima/ETL_Join_Mes_Cerrado.py
}

etl_hoy_contaminacionyprediccion 2>&1 | tee -a $LOG -a $LOG_HOY >/dev/null
