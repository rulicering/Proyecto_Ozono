#!/bin/bash
# DATOS HOY  -> SE EJECUTA A LAS 3AM DEL DIA SIGUIENTE.
# SE GUARDA EN EL LOG DIARIO DEL DIA ANTERIORQUE ES EL QUE CORRESPONDE A LOS DATOS QUE SE CARGAN


AYER=$(date --date "yesterday" +%Y-%m-%d)
LOG_HOY="/home/rulicering/Datos_Proyecto_Ozono/Logs/Diario/log-$AYER.txt"
LOG="/home/rulicering/Datos_Proyecto_Ozono/Logs/log.txt"

etl_hoy_climayjoin(){
echo "---------------------------CLIMA HOY---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Clima/ETL_Clima_Dia_Actual.py
echo "---------------------------JOIN ---------------------------"
python3 /home/rulicering/Proyecto_Ozono/source/Pyhton/Join/ETL_Join_Dia_Actual.py
}

etl_hoy_climayjoin 2>&1 | tee -a $LOG -a $LOG_HOY >/dev/null
