#!/bin/bash

###################
#
# Name: extract_csv_files_from_pexdev_load_into_pexstat.sh
# Description: Script to insert into $DB_SYNCSCHEMA.$DB_SYNCTABLE table the date when it start, to extract the csv files and to  load into PEX Stat database. 
#              The script need to be called as postgres OS user   
# Version 1.0
# Author: Mohamed Chiguer, Victor Ghita
#
#  How to call:  ./extract_csv_files_from_pexrex_load_into_pexstat.sh
#
# Date:					Name:               Version:      Modification:
# 12.06.2023            Ghita Victor        1.0           Implement the first version of the script        
#
###################
clear screen;
echo -e "INFO: Script $0 started at ${DATE_PASSAGE}\n\n"

# Variables de connexion à la base de données bpexdev1
DB_HOST="10.25.93.45"
DB_PORT="5432"
DB_NAME="bpexrec1"
DB_USER="admin"
DB_SYNCSCHEMA="pex_trc_synch"
DB_SYNCTABLE="T_SYNCHRONISATION"

# PG STAT info
TARGET_DB_HOST="10.25.72.36"
TARGET_DB_NAME="bpexstat1"
TARGET_DB_PORT="5432"
TARGET_DB_USER="pex_trace"
TARGET_DB_SYNCSCHEMA="PEX_TRC"

# Variables de date
CURRENT_DATE=$(date +"%d-%m-%y_%H-%M-%S")
DATE_PASSAGE=$(date +"%Y-%m-%d %H:%M:%S")
HOUR_DEBUT=$(date -d "$DATE_PASSAGE" +"%H")
HOUR_DEBUT=$((${HOUR_DEBUT} - 1))
DATE_FIN=$(date +"%Y-%m-%d ${HOUR_DEBUT}:59:59.999")

 
# CSV export path
CSV_EXPORT_PATH="/data/scripts/pex_stat/csv"
PEX_TRC_T_TRACE_FLUX_CSV_FILE=${CSV_EXPORT_PATH}/pex_trc_t_trace_flux.csv
PEX_TRC_T_TRACE_SERVICE_CSV_FILE=${CSV_EXPORT_PATH}/pex_trc_t_trace_service.csv
PEX_TRC_T_TRACE_DONNEES_METIER_CSV_FILE=${CSV_EXPORT_PATH}/pex_trc_t_trace_donnees_metier.csv
PEX_TRC_T_TRACE_INFO_CSV_FILE=${CSV_EXPORT_PATH}/pex_trc_t_trace_info.csv
PEX_TRC_T_TRACE_LOG_CSV_FILE=${CSV_EXPORT_PATH}/pex_trc_t_trace_log.csv
PEX_TRC_T_TRACE_LOG_ERREUR_CSV_FILE=${CSV_EXPORT_PATH}/pex_trc_t_trace_log_erreur.csv
 
# LOAD configuration files for pgloader
LOAD_CFG_FILES_PATH="/data/scripts/pex_stat/load_files" 
PEX_TRC_T_TRACE_FLUX_LOAD_FILE=${LOAD_CFG_FILES_PATH}/commandes_pex_trc_t_trace_flux.load
PEX_TRC_T_TRACE_SERVICE_LOAD_FILE=${LOAD_CFG_FILES_PATH}/commandes_pex_trc_t_trace_service.load
PEX_TRC_T_TRACE_DONNEES_METIER_LOAD_FILE=${LOAD_CFG_FILES_PATH}/commandes_pex_trc_t_trace_donnees_metier.load
PEX_TRC_T_TRACE_INFO_LOAD_FILE=${LOAD_CFG_FILES_PATH}/commandes_pex_trc_t_trace_info.load
PEX_TRC_T_TRACE_LOG_LOAD_FILE=${LOAD_CFG_FILES_PATH}/commandes_pex_trc_t_trace_log.load
PEX_TRC_T_TRACE_LOG_ERREUR_LOAD_FILE=${LOAD_CFG_FILES_PATH}/commandes_pex_trc_t_trace_log_erreur.load


# LOAD configuration files for pgloader
LOAD_CFG_LOGS_PATH_ROOT="/data/scripts/pex_stat/logs" 
LOAD_CFG_LOGS_PATH="/data/scripts/pex_stat/logs/${CURRENT_DATE}" 
LOG_RETENTION_IN_DAYS=1

# Chemin vers le binaire psql
PSQL="/moteurs/postgresql-11/bin/psql"
TARGET_DB_PWD="pex_trace"


# Variables
RUNNING_OS_USER="postgres"

function return_current_date {
  DATE_SCRIPT_COMPLETE=$(date +"%Y-%m-%d %H:%M:%S")
  echo "${DATE_SCRIPT_COMPLETE}"
}


function insert_start_time {
  # Check if the current user is $RUNNING_OS_USER (whic in this case it's postgres)
  if [ "$RUNNING_OS_USER" != "" -a "$(id -un)" != "${RUNNING_OS_USER}" ]; then
    echo -e "ERROR: This script must be run as ${RUNNING_OS_USER}\nExiting from the scrip..\n" 
    exit 1;
  fi;
   
   
  # Important: The date_debut = last date_fin with OK status | date_fin it's defined in the header section
  # Select MAX ID,
  echo -e "Executing the statement:   SELECT MAX(id) FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE});"
  MAX_ID=$(${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "SELECT MAX(ID) FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE};" 2>&1 )
  MAX_ID=$(echo ${MAX_ID//[[:blank:]]/})
  echo -e "INFO: MAX_ID=${MAX_ID}\n"
  

  if [ "$MAX_ID" == "" ] || [ -z "$MAX_ID" ]; then
    echo -e "ERROR: MAX_ID is null. There is no data into ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} table"
    # when script it scall 12:30 
	# date_debut= 11:00:00:0
	# date_fin= 11:59:59:999
	HOUR_DEBUT=$(date -d "$DATE_PASSAGE" +"%H")
	HOUR_DEBUT=$((${HOUR_DEBUT} - 1))
    DATE_DEBUT=$(date +"%Y-%m-%d ${HOUR_DEBUT}:00:00.000")
    DATE_FIN=$(date +"%Y-%m-%d ${HOUR_DEBUT}:59:59.999")
  else 
    echo -e "INFO: Executing the statement: ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c \"SELECT date_debut FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} WHERE statut='OK' order by id desc limit 1;\""
    LAST_SUCCESFULL_DATE_FIN=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "SELECT date_fin FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} WHERE statut='OK' order by id desc limit 1;" 2>&1 )
    echo -e "INFO: LAST_SUCCESFULL_DATE_FIN= ${LAST_SUCCESFULL_DATE_FIN}. Assign variable DATE_DEBUT with value ${LAST_SUCCESFULL_DATE_FIN}.\n"
    DATE_DEBUT=${LAST_SUCCESFULL_DATE_FIN}
  fi 
  
  
  echo -e "INFO: Inserting data: INSERT INTO ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} (DATE_PASSAGE, DATE_DEBUT, DATE_FIN, STATUT, ETAPE) VALUES ('$DATE_PASSAGE', '$DATE_DEBUT', '$DATE_FIN', 'IN PROGRESS', E'Job Started\n') into ${DB_NAME}@${DB_HOST}\n"
  InsertOutput=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER}  -c "INSERT INTO ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} (DATE_PASSAGE, DATE_DEBUT, DATE_FIN, STATUT, ETAPE) VALUES ('$DATE_PASSAGE', '$DATE_DEBUT', '$DATE_FIN', 'IN PROGRESS','Job Started');" 2>&1 )
  
  if [ "$InsertOutput" != "INSERT 0 1" ]; then
    echo -e "ERROR: Script not able to insert data into ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} from ${DB_NAME}@${DB_HOST}\nExiting from the script..\n"
	exit 1;
  fi
  
}


function extract_csv_file {
  # Create & check ${CSV_EXPORT_PATH} if not exist 
  mkdir -pv ${CSV_EXPORT_PATH}
  if [ ! -w ${CSV_EXPORT_PATH} ]; then 
    echo -e "ERROR: The export directory for CSV files doesn't exist. Path: ${CSV_EXPORT_PATH}\nExiting from the script..\n\n" 
	exit 1;
  fi
  
  
  
  # Select MAX ID, in this order assure that the will be the same ID for the current run
  echo -e "Executing the statement:   SELECT MAX(id) FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE});"
  MAX_ID=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "SELECT MAX(ID) FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE};" 2>&1 )
   MAX_ID=$(echo ${MAX_ID//[[:blank:]]/})
  echo -e "INFO: MAX_ID ${MAX_ID}\n"
  
  # select the DATE_DEBUT & DATE_FIN
  echo -e "INFO: Executing the statement:   SELECT DATE_DEBUT FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} WHERE id = ${MAX_ID};"
  LAST_DATE_DEBUT=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "SELECT DATE_DEBUT FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} WHERE id = ${MAX_ID};" 2>&1 )
  echo -e "INFO: LAST_DATE_DEBUT = ${LAST_DATE_DEBUT}\n"
  
  echo -e "INFO: Executing the statement:   SELECT DATE_FIN FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} WHERE id = ${MAX_ID};"
  LAST_DATE_FIN=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "SELECT DATE_FIN FROM ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} WHERE id = ${MAX_ID};" 2>&1 )
  echo -e "INFO: LAST_DATE_FIN = ${LAST_DATE_FIN}\n\n"
  
echo -e "INFO: DATE_PASSAGE = ${DATE_PASSAGE}\n"
echo -e "INFO: HOUR_DEBUT = ${HOUR_DEBUT}\n"
echo -e "INFO: DATE_DEBUT = ${DATE_DEBUT}\n"
echo -e "INFO: DATE_FIN = ${DATE_FIN}\n\n"

 
 #exit 1;
 
 
  # Clean the old csv files
  rm ${CSV_EXPORT_PATH}/*.csv
  
  # Extract the CSV file for For table PEX_TRC.T_TRACE_FLUX
  echo -e "INFO: Export CSV file for table PEX_TRC.T_TRACE_FLUX. Running command:"
  
  echo "COPY (
SELECT *
FROM PEX_TRC.T_TRACE_FLUX TF
WHERE TF.RECEPTION_DATE >= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE <= '${LAST_DATE_FIN}'
ORDER BY TF.ID_TRACE_FLUX
) TO '${PEX_TRC_T_TRACE_FLUX_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');"
EXPORT_PEX_TRC_T_TRACE_FLUX_TO_CSV=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "COPY (
SELECT *
FROM PEX_TRC.T_TRACE_FLUX TF
WHERE TF.RECEPTION_DATE >= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE <= '${LAST_DATE_FIN}'
ORDER BY TF.ID_TRACE_FLUX
) TO '${PEX_TRC_T_TRACE_FLUX_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 2>&1 )
if [ $(echo ${EXPORT_PEX_TRC_T_TRACE_FLUX_TO_CSV} | grep -wic "ERROR") -gt 0 ]; then
  echo -e "ERROR: Error while exporting table. Error Message: ${EXPORT_PEX_TRC_T_TRACE_FLUX_TO_CSV}\nUPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_FLUX_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_FLUX' where id=${MAX_ID};\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_FLUX_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_FLUX' where id=${MAX_ID};"
  echo -e "";
else 
  echo -e "INFO: EXPORT_PEX_TRC_T_TRACE_FLUX_TO_CSV ${EXPORT_PEX_TRC_T_TRACE_FLUX_TO_CSV}\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE, E'\nCSV exported for table PEX_TRC.T_TRACE_FLUX') where id=${MAX_ID};"
  echo -e "";
fi 


  # Extract the CSV file for For table PEX_TRC.T_TRACE_SERVICE
  echo -e "INFO: Export CSV file for table PEX_TRC.T_TRACE_SERVICE. Running command:"
echo "COPY (
  SELECT *
  FROM PEX_TRC.T_TRACE_SERVICE TS
  WHERE TS.RECEPTION_DATE>=  '${LAST_DATE_DEBUT}'
  AND TS.RECEPTION_DATE<=  '${LAST_DATE_FIN}'
  ORDER BY TS.ID_TRACE_SERVICE 
  ) TO '${PEX_TRC_T_TRACE_SERVICE_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 
EXPORT_PEX_TRC_T_TRACE_SERVICE_TO_CSV=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "COPY (
  SELECT *
  FROM PEX_TRC.T_TRACE_SERVICE TS
  WHERE TS.RECEPTION_DATE>=  '${LAST_DATE_DEBUT}'
  AND TS.RECEPTION_DATE<=  '${LAST_DATE_FIN}'
  ORDER BY TS.ID_TRACE_SERVICE 
  ) TO '${PEX_TRC_T_TRACE_SERVICE_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 2>&1 )
if [ $(echo ${EXPORT_PEX_TRC_T_TRACE_SERVICE_TO_CSV} | grep -wic "ERROR") -gt 0 ]; then
  echo -e "ERROR: Error while exporting table. Error Message: ${EXPORT_PEX_TRC_T_TRACE_SERVICE_TO_CSV}\nUPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_SERVICE_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_SERVICE' where id=${MAX_ID};\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_SERVICE_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_SERVICE' where id=${MAX_ID};"
  echo -e "";
else 
  echo -e "INFO: EXPORT_PEX_TRC_T_TRACE_SERVICE_TO_CSV ${EXPORT_PEX_TRC_T_TRACE_SERVICE_TO_CSV}\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nCSV exported for table PEX_TRC.T_TRACE_SERVICE') where id=${MAX_ID};"
  echo -e "";
fi 


  # Extract the CSV file for For table PEX_TRC.T_TRACE_DONNEES_METIER
echo -e "INFO: Export CSV file for table PEX_TRC.T_TRACE_DONNEES_METIER. Running command:"
echo "COPY (
(SELECT TDM.ID_TRACE_DONNEES_METIER, TDM.ID_TRACE_SERVICE, TDM. ID_TRACE_FLUX, TDM.NOM, TDM.VALEUR
FROM PEX_TRC.T_TRACE_DONNEES_METIER TDM JOIN  PEX_TRC.T_TRACE_FLUX TF USING (ID_TRACE_FLUX) 
WHERE 
TF.RECEPTION_DATE>= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE<= '${LAST_DATE_FIN}'
)
UNION 
(SELECT TDM.ID_TRACE_DONNEES_METIER, TDM.ID_TRACE_SERVICE, TDM. ID_TRACE_FLUX, TDM.NOM, TDM.VALEUR
FROM PEX_TRC.T_TRACE_DONNEES_METIER TDM JOIN  PEX_TRC.T_TRACE_SERVICE TF USING (ID_TRACE_SERVICE) 
WHERE 
TF.RECEPTION_DATE>= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE<= '${LAST_DATE_FIN}'
) ORDER BY ID_TRACE_DONNEES_METIER
) TO '${PEX_TRC_T_TRACE_DONNEES_METIER_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 
EXPORT_PEX_TRC_T_TRACE_DONNEES_METIER_TO_CSV=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "COPY (
(SELECT TDM.ID_TRACE_DONNEES_METIER, TDM.ID_TRACE_SERVICE, TDM. ID_TRACE_FLUX, TDM.NOM, TDM.VALEUR
FROM PEX_TRC.T_TRACE_DONNEES_METIER TDM JOIN  PEX_TRC.T_TRACE_FLUX TF USING (ID_TRACE_FLUX) 
WHERE 
TF.RECEPTION_DATE>= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE<= '${LAST_DATE_FIN}'
)
UNION 
(SELECT TDM.ID_TRACE_DONNEES_METIER, TDM.ID_TRACE_SERVICE, TDM. ID_TRACE_FLUX, TDM.NOM, TDM.VALEUR
FROM PEX_TRC.T_TRACE_DONNEES_METIER TDM JOIN  PEX_TRC.T_TRACE_SERVICE TF USING (ID_TRACE_SERVICE) 
WHERE 
TF.RECEPTION_DATE>= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE<= '${LAST_DATE_FIN}'
) ORDER BY ID_TRACE_DONNEES_METIER
) TO '${PEX_TRC_T_TRACE_DONNEES_METIER_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 2>&1 )

if [ $(echo ${EXPORT_PEX_TRC_T_TRACE_DONNEES_METIER_TO_CSV} | grep -wic "ERROR") -gt 0 ]; then
  echo -e "ERROR: Error while exporting table. Error Message: ${EXPORT_PEX_TRC_T_TRACE_DONNEES_METIER_TO_CSV}\nUPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_DONNEES_METIER_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_DONNEES_METIER' where id=${MAX_ID};\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_DONNEES_METIER_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_DONNEES_METIER' where id=${MAX_ID};"
  echo -e "";
else 
  echo -e "INFO: EXPORT_PEX_TRC_T_TRACE_DONNEES_METIER_TO_CSV ${EXPORT_PEX_TRC_T_TRACE_DONNEES_METIER_TO_CSV}\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nCSV exported for table PEX_TRC.T_TRACE_DONNEES_METIER') where id=${MAX_ID};"
  echo -e "";
fi 


  # Extract the CSV file for For table PEX_TRC.T_TRACE_INFO
  echo -e "INFO: Export CSV file for table PEX_TRC.T_TRACE_INFO. Running command:"
  echo "COPY (
(SELECT TI.ID_TRACE_INFO, TI.ID_TRACE_SERVICE, TI.ID_TRACE_FLUX, TI.CLE, TI.VALEUR
FROM PEX_TRC.T_TRACE_INFO TI JOIN PEX_TRC.T_TRACE_FLUX TF USING(ID_TRACE_FLUX)
WHERE TF.RECEPTION_DATE >= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE <= '${LAST_DATE_FIN}'
)
UNION 
(SELECT TI.ID_TRACE_INFO, TI.ID_TRACE_SERVICE, TI.ID_TRACE_FLUX, TI.CLE, TI.VALEUR
FROM PEX_TRC.T_TRACE_INFO TI JOIN PEX_TRC.T_TRACE_SERVICE TS USING (ID_TRACE_SERVICE) 
WHERE 
TS.RECEPTION_DATE>= '${LAST_DATE_DEBUT}'
AND TS.RECEPTION_DATE<= '${LAST_DATE_FIN}'
) order by 1
) TO '${PEX_TRC_T_TRACE_INFO_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 
  EXPORT_PEX_TRC_T_TRACE_INFO_TO_CSV=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "COPY (
(SELECT TI.ID_TRACE_INFO, TI.ID_TRACE_SERVICE, TI.ID_TRACE_FLUX, TI.CLE, TI.VALEUR
FROM PEX_TRC.T_TRACE_INFO TI JOIN PEX_TRC.T_TRACE_FLUX TF USING(ID_TRACE_FLUX)
WHERE TF.RECEPTION_DATE >= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE <= '${LAST_DATE_FIN}'
)
UNION 
(SELECT TI.ID_TRACE_INFO, TI.ID_TRACE_SERVICE, TI.ID_TRACE_FLUX, TI.CLE, TI.VALEUR
FROM PEX_TRC.T_TRACE_INFO TI JOIN PEX_TRC.T_TRACE_SERVICE TS USING (ID_TRACE_SERVICE) 
WHERE 
TS.RECEPTION_DATE>= '${LAST_DATE_DEBUT}'
AND TS.RECEPTION_DATE<= '${LAST_DATE_FIN}'
) order by 1
) TO '${PEX_TRC_T_TRACE_INFO_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 2>&1 )

if [ $(echo ${EXPORT_PEX_TRC_T_TRACE_INFO_TO_CSV} | grep -wic "ERROR") -gt 0 ]; then
  echo -e "ERROR: Error while exporting table. Error Message: ${EXPORT_PEX_TRC_T_TRACE_INFO_TO_CSV}\nUPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_INFO_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_INFO' where id=${MAX_ID};\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_INFO_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_INFO' where id=${MAX_ID};"
  echo -e "";
else 
  echo -e "INFO: EXPORT_PEX_TRC_T_TRACE_INFO_TO_CSV ${EXPORT_PEX_TRC_T_TRACE_INFO_TO_CSV}\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nCSV exported for table PEX_TRC.T_TRACE_INFO') where id=${MAX_ID};"
  echo -e "";
fi 



  # Extract the CSV file for For table PEX_TRC.T_TRACE_LOG
echo -e "INFO: Export CSV file for table PEX_TRC.T_TRACE_LOG. Running command:"
echo "COPY (
  (SELECT TL.ID_TRACE_LOG, TL.ID_TRACE_SERVICE, TL.ID_TRACE_FLUX, TL.ID_TRACE_ORDRE, TL.DATE_MESSAGE, TL.ETAPE, TL.TYPE_MESSAGE, TL.ENDPOINT_NAME, TL.MESSAGE
  FROM PEX_TRC.T_TRACE_LOG TL JOIN PEX_TRC.T_TRACE_FLUX TF USING (ID_TRACE_FLUX)
  WHERE TF.RECEPTION_DATE>= '${LAST_DATE_DEBUT}' 
  AND TF.RECEPTION_DATE<= '${LAST_DATE_FIN}'
  )
  UNION
  (SELECT TL.ID_TRACE_LOG, TL.ID_TRACE_SERVICE, TL.ID_TRACE_FLUX, TL.ID_TRACE_ORDRE, TL.DATE_MESSAGE, TL.ETAPE, TL.TYPE_MESSAGE, TL.ENDPOINT_NAME, TL.MESSAGE
  FROM PEX_TRC.T_TRACE_LOG TL JOIN PEX_TRC.T_TRACE_SERVICE TS USING (ID_TRACE_SERVICE)
  WHERE TS.RECEPTION_DATE>= '${LAST_DATE_DEBUT}' 
  AND TS.RECEPTION_DATE<= '${LAST_DATE_FIN}'
  ) ORDER BY 1
)  TO '${PEX_TRC_T_TRACE_LOG_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 
EXPORT_PEX_TRC_T_TRACE_LOG_TO_CSV=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "COPY (
  (SELECT TL.ID_TRACE_LOG, TL.ID_TRACE_SERVICE, TL.ID_TRACE_FLUX, TL.ID_TRACE_ORDRE, TL.DATE_MESSAGE, TL.ETAPE, TL.TYPE_MESSAGE, TL.ENDPOINT_NAME, TL.MESSAGE
  FROM PEX_TRC.T_TRACE_LOG TL JOIN PEX_TRC.T_TRACE_FLUX TF USING (ID_TRACE_FLUX)
  WHERE TF.RECEPTION_DATE>= '${LAST_DATE_DEBUT}' 
  AND TF.RECEPTION_DATE<= '${LAST_DATE_FIN}'
  )
  UNION
  (SELECT TL.ID_TRACE_LOG, TL.ID_TRACE_SERVICE, TL.ID_TRACE_FLUX, TL.ID_TRACE_ORDRE, TL.DATE_MESSAGE, TL.ETAPE, TL.TYPE_MESSAGE, TL.ENDPOINT_NAME, TL.MESSAGE
  FROM PEX_TRC.T_TRACE_LOG TL JOIN PEX_TRC.T_TRACE_SERVICE TS USING (ID_TRACE_SERVICE)
  WHERE TS.RECEPTION_DATE>= '${LAST_DATE_DEBUT}' 
  AND TS.RECEPTION_DATE<= '${LAST_DATE_FIN}'
  ) ORDER BY 1
)  TO '${PEX_TRC_T_TRACE_LOG_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 2>&1 )

if [ $(echo ${EXPORT_PEX_TRC_T_TRACE_LOG_TO_CSV} | grep -wic "ERROR") -gt 0 ]; then
  echo -e "ERROR: Error while exporting table. Error Message: ${EXPORT_PEX_TRC_T_TRACE_LOG_TO_CSV}\nUPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_LOG_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_LOG' where id=${MAX_ID};\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_LOG_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_LOG' where id=${MAX_ID};"
  echo -e "";
else 
  echo -e "INFO: EXPORT_PEX_TRC_T_TRACE_LOG_TO_CSV ${EXPORT_PEX_TRC_T_TRACE_LOG_TO_CSV}\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nCSV exported for table PEX_TRC.T_TRACE_LOG') where id=${MAX_ID};"
  echo -e "";
fi 



  # Extract the CSV file for For table PEX_TRC.T_TRACE_LOG_ERREUR
echo -e "INFO: Export CSV file for table PEX_TRC.T_TRACE_LOG_ERREUR. Running command:"
echo "COPY (
(SELECT TLE.ID_TRACE_LOG_ERREUR, TLE.ID_TRACE_SERVICE, TLE.ID_TRACE_FLUX, TLE.ID_TRACE_ORDRE, TLE.ENDPOINT_NAME, TLE.MESSAGE, TLE.STACKTRACE, TLE.ID_JMS_MESSAGE, TLE.JMS_FILE_NAME, TLE.DATE_MESSAGE, TLE.ETAPE
FROM PEX_TRC.T_TRACE_LOG_ERREUR TLE JOIN PEX_TRC.T_TRACE_FLUX TF USING(ID_TRACE_FLUX)
WHERE TF.RECEPTION_DATE >= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE <= '${LAST_DATE_FIN}'
)
UNION 
(SELECT TLE.ID_TRACE_LOG_ERREUR, TLE.ID_TRACE_SERVICE, TLE.ID_TRACE_FLUX, TLE.ID_TRACE_ORDRE, TLE.ENDPOINT_NAME, TLE.MESSAGE, TLE.STACKTRACE, TLE.ID_JMS_MESSAGE, TLE.JMS_FILE_NAME, TLE.DATE_MESSAGE, TLE.ETAPE
FROM PEX_TRC.T_TRACE_LOG_ERREUR TLE JOIN PEX_TRC.T_TRACE_SERVICE TS USING (ID_TRACE_SERVICE) 
WHERE 
TS.RECEPTION_DATE>= '${LAST_DATE_DEBUT}'
AND TS.RECEPTION_DATE<= '${LAST_DATE_FIN}'
) order by 1
)  TO '${PEX_TRC_T_TRACE_LOG_ERREUR_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 
EXPORT_PEX_TRC_T_TRACE_LOG_ERREUR_TO_CSV=$( ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -t -c "COPY (
(SELECT TLE.ID_TRACE_LOG_ERREUR, TLE.ID_TRACE_SERVICE, TLE.ID_TRACE_FLUX, TLE.ID_TRACE_ORDRE, TLE.ENDPOINT_NAME, TLE.MESSAGE, TLE.STACKTRACE, TLE.ID_JMS_MESSAGE, TLE.JMS_FILE_NAME, TLE.DATE_MESSAGE, TLE.ETAPE
FROM PEX_TRC.T_TRACE_LOG_ERREUR TLE JOIN PEX_TRC.T_TRACE_FLUX TF USING(ID_TRACE_FLUX)
WHERE TF.RECEPTION_DATE >= '${LAST_DATE_DEBUT}'
AND TF.RECEPTION_DATE <= '${LAST_DATE_FIN}'
)
UNION 
(SELECT TLE.ID_TRACE_LOG_ERREUR, TLE.ID_TRACE_SERVICE, TLE.ID_TRACE_FLUX, TLE.ID_TRACE_ORDRE, TLE.ENDPOINT_NAME, TLE.MESSAGE, TLE.STACKTRACE, TLE.ID_JMS_MESSAGE, TLE.JMS_FILE_NAME, TLE.DATE_MESSAGE, TLE.ETAPE
FROM PEX_TRC.T_TRACE_LOG_ERREUR TLE JOIN PEX_TRC.T_TRACE_SERVICE TS USING (ID_TRACE_SERVICE) 
WHERE 
TS.RECEPTION_DATE>= '${LAST_DATE_DEBUT}'
AND TS.RECEPTION_DATE<= '${LAST_DATE_FIN}'
) order by 1
)  TO '${PEX_TRC_T_TRACE_LOG_ERREUR_CSV_FILE}' WITH (FORMAT CSV, HEADER, DELIMITER ';');" 2>&1 )

if [ $(echo ${EXPORT_PEX_TRC_T_TRACE_LOG_ERREUR_TO_CSV} | grep -wic "ERROR") -gt 0 ]; then
  echo -e "ERROR: Error while exporting table. Error Message: ${EXPORT_PEX_TRC_T_TRACE_LOG_ERREUR_TO_CSV}\nUPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_LOG_ERREUR_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_LOG_ERREUR' where id=${MAX_ID};\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set MESSAGE_ERREUR = '${EXPORT_PEX_TRC_T_TRACE_LOG_ERREUR_TO_CSV}', ETAPE = 'ERROR exporting table PEX_TRC.T_TRACE_LOG_ERREUR' where id=${MAX_ID};"
  echo -e "";
else 
  echo -e "INFO: EXPORT_PEX_TRC_T_TRACE_LOG_ERREUR_TO_CSV ${EXPORT_PEX_TRC_T_TRACE_LOG_ERREUR_TO_CSV}\n\n"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nCSV exported for table PEX_TRC.T_TRACE_LOG_ERREUR') where id=${MAX_ID};"
  echo -e "";
fi 

}


# Function to detect the old entries (older than 1 days) with progress status, set them to KO
function clean_old_status_records {
  echo -e "INFO: Detect the old record IN PROGRESS and set them to KO status\n" 
  echo -e "INFO: Running the command: UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set statut='KO' where upper(statut)=upper('IN PROGRESS') and date_fin > (CURRENT_DATE - INTERVAL '2 days');"
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set statut='KO', MESSAGE_ERREUR = 'ERROR: JOB set on KO status due to time', ETAPE = CONCAT(ETAPE,E'\nEvaluate old IN PROGRESS jobs')  where upper(statut)=upper('IN PROGRESS') and date_fin > (CURRENT_DATE - INTERVAL '1 days')";
}


# Function to delete old logs from target and source
function clean_old_logs {
  echo -e "INFO: Delete older logs than 30 days. Running commands:"
  echo -e "find ${LOAD_CFG_LOGS_PATH_ROOT}/*/ -name \"*.log\" -mtime +${LOG_RETENTION_IN_DAYS} -exec rm {} \;"
  echo -e "find ${LOAD_CFG_LOGS_PATH_ROOT}/*/ -name \"*.summary\" -mtime +${LOG_RETENTION_IN_DAYS} -exec rm {} \;"
  echo -e "find ${LOAD_CFG_LOGS_PATH_ROOT}/ -empty -type d -delete" 
  find ${LOAD_CFG_LOGS_PATH_ROOT}/*/ -name "*.log" -mtime +${LOG_RETENTION_IN_DAYS} -exec rm {} \;
  find ${LOAD_CFG_LOGS_PATH_ROOT}/*/ -name "*.summary" -mtime +${LOG_RETENTION_IN_DAYS} -exec rm {} \;
  find ${LOAD_CFG_LOGS_PATH_ROOT}/ -empty -type d -delete
}


# Function to call the pgloader and import the csv files
function generate_load_files {
   # Create the  load directory where the load configuration files will be stored
  mkdir -pv ${LOAD_CFG_FILES_PATH}
  if [ ! -w ${LOAD_CFG_FILES_PATH} ]; then 
    echo -e "ERROR: The directory for load configuration files doesn't exist. Path: ${LOAD_CFG_FILES_PATH}\nExiting from the script..\n\n" 
	exit 1;
  else 
    echo -e "INFO: The directory where load configuration files are generated: ${LOAD_CFG_FILES_PATH}\n"
  fi
  
  # Create the load config file for TABLE PEX_TRC.T_TRACE_FLUX:
  echo -e "INFO: Generating the load configuration file for PEX_TRC.T_TRACE_FLUX ${PEX_TRC_T_TRACE_FLUX_LOAD_FILE}\n"
  echo "LOAD CSV
     FROM '${PEX_TRC_T_TRACE_FLUX_CSV_FILE}' (
       id_trace_flux,
       id_message,
       id_demi_flux_in_message,
       id_demi_flux_out_message,
       id_parent,
       id_batch,
       reception_date,
       statut,
       criticite,
       demi_flux_in_name,
       demi_flux_out_name,
       protocole_entree,
       emetteur,
       nom_fichier_entree,
       protocole_sortie,
       destinataire,
       nom_fichier_sortie,
       pattern,
       rejeu_nombre,
       derniere_etape,
       dernier_enregistrement,
       plan_routage,
       criteres,
       id_elasticsearch,
       rejeu_message_header,
       trace_original_message)
INTO postgresql://${TARGET_DB_USER}:${TARGET_DB_PWD}@${TARGET_DB_HOST}/${TARGET_DB_NAME}?${TARGET_DB_SYNCSCHEMA}.T_TRACE_FLUX
(
       id_trace_flux,
       id_message,
       id_demi_flux_in_message,
       id_demi_flux_out_message,
       id_parent,
       id_batch,
       reception_date,
       statut,
       criticite,
       demi_flux_in_name,
       demi_flux_out_name,
       protocole_entree,
       emetteur,
       nom_fichier_entree,
       protocole_sortie,
       destinataire,
       nom_fichier_sortie,
       pattern,
       rejeu_nombre,
       derniere_etape,
       dernier_enregistrement,
       plan_routage,
       criteres,
       id_elasticsearch,
       rejeu_message_header,
       trace_original_message)
     WITH skip header = 1,
	 fields terminated by ';' ;" > ${PEX_TRC_T_TRACE_FLUX_LOAD_FILE}


  # Create the load config file for TABLE PEX_TRC.T_TRACE_SERVICE:
  echo -e "INFO: Generating the load configuration file for PEX_TRC.T_TRACE_SERVICE ${PEX_TRC_T_TRACE_SERVICE_LOAD_FILE}\n"
  echo "LOAD CSV
     FROM '${PEX_TRC_T_TRACE_SERVICE_CSV_FILE}' (
       id_trace_service,
       id_message,
       service_name,
       reception_date,
       statut,
       criticite,
       derniere_etape,
       consommateur)
INTO postgresql://${TARGET_DB_USER}:${TARGET_DB_PWD}@${TARGET_DB_HOST}/${TARGET_DB_NAME}?${TARGET_DB_SYNCSCHEMA}.T_TRACE_SERVICE
(
       id_trace_service,
       id_message,
       service_name,
       reception_date,
       statut,
       criticite,
       derniere_etape,
       consommateur)
     WITH skip header = 1,
	 fields terminated by ';' ;" > ${PEX_TRC_T_TRACE_SERVICE_LOAD_FILE}
   
  
  # Create the load config file for TABLE PEX_TRC.T_TRACE_DONNEES_METIER:
  echo -e "INFO: Generating the load configuration file for PEX_TRC.T_TRACE_DONNEES_METIER ${PEX_TRC_T_TRACE_DONNEES_METIER_LOAD_FILE}\n"
  echo "LOAD CSV
  FROM '${PEX_TRC_T_TRACE_DONNEES_METIER_CSV_FILE}' (
    id_trace_donnees_metier,
    id_trace_service,
    id_trace_flux,
    nom,
    valeur)
INTO postgresql://${TARGET_DB_USER}:${TARGET_DB_PWD}@${TARGET_DB_HOST}/${TARGET_DB_NAME}?${TARGET_DB_SYNCSCHEMA}.T_TRACE_DONNEES_METIER
(
    id_trace_donnees_metier,
    id_trace_service,
    id_trace_flux,
    nom,
    valeur)
WITH skip header = 1, 
fields terminated by ';' ;" > ${PEX_TRC_T_TRACE_DONNEES_METIER_LOAD_FILE}


  # Create the load config file for TABLE PEX_TRC.T_TRACE_INFO:
  echo -e "INFO: Generating the load configuration file for PEX_TRC.T_TRACE_INFO ${PEX_TRC_T_TRACE_INFO_LOAD_FILE}\n"
  echo "LOAD CSV
  FROM '${PEX_TRC_T_TRACE_INFO_CSV_FILE}' (
    id_trace_info,
    id_trace_service,
    id_trace_flux,
	cle,
    valeur)
INTO postgresql://${TARGET_DB_USER}:${TARGET_DB_PWD}@${TARGET_DB_HOST}/${TARGET_DB_NAME}?${TARGET_DB_SYNCSCHEMA}.T_TRACE_INFO
(
    id_trace_info,
    id_trace_service,
    id_trace_flux,
	cle,
    valeur)
WITH skip header = 1,
fields terminated by ';' ;" > ${PEX_TRC_T_TRACE_INFO_LOAD_FILE}
  
  
  # Create the load config file for TABLE PEX_TRC.T_TRACE_LOG:
  echo -e "INFO: Generating the load configuration file for PEX_TRC.T_TRACE_LOG ${PEX_TRC_T_TRACE_LOG_LOAD_FILE}\n"
  echo "LOAD CSV
  FROM '${PEX_TRC_T_TRACE_LOG_CSV_FILE}' (
    id_trace_log,
    id_trace_service,
    id_trace_flux,
    id_trace_ordre,
    date_message,
    etape,
    type_message,
    endpoint_name,
    message)
INTO postgresql://${TARGET_DB_USER}:${TARGET_DB_PWD}@${TARGET_DB_HOST}/${TARGET_DB_NAME}?${TARGET_DB_SYNCSCHEMA}.T_TRACE_LOG
(
    id_trace_log,
    id_trace_service,
    id_trace_flux,
    id_trace_ordre,
    date_message,
    etape,
    type_message,
    endpoint_name,
    message)
WITH skip header = 1,
	 fields terminated by ';' ;" > ${PEX_TRC_T_TRACE_LOG_LOAD_FILE}
  
  
  # Create the load config file for TABLE PEX_TRC.T_TRACE_LOG_ERREUR:
  echo -e "INFO: Generating the load configuration file for PEX_TRC.T_TRACE_LOG_ERREUR ${PEX_TRC_T_TRACE_LOG_ERREUR_LOAD_FILE}\n"
  echo "LOAD CSV
  FROM '${PEX_TRC_T_TRACE_LOG_ERREUR_CSV_FILE}' (
  id_trace_log_erreur,
  id_trace_service,
  id_trace_flux,
  id_trace_ordre,
  endpoint_name,
  message,
  stacktrace,    
  id_jms_message,
  jms_file_name,
  date_message,
  etape)
INTO postgresql://${TARGET_DB_USER}:${TARGET_DB_PWD}@${TARGET_DB_HOST}/${TARGET_DB_NAME}?${TARGET_DB_SYNCSCHEMA}.T_TRACE_LOG_ERREUR
(
  id_trace_log_erreur,
  id_trace_service,
  id_trace_flux,
  id_trace_ordre,
  endpoint_name,
  message,
  stacktrace,    
  id_jms_message,
  jms_file_name,
  date_message,
  etape)
WITH skip header = 1,
fields terminated by ';' ;" > ${PEX_TRC_T_TRACE_LOG_ERREUR_LOAD_FILE}
  
  #check status and update into sync table
}


function copy_load_files {
  CreateLoadRemoteDirectory=$(ssh -o LogLevel=ERROR ${TARGET_DB_HOST} "mkdir -pv ${LOAD_CFG_FILES_PATH}"  2>&1)
  echo -e "INFO: CreateLoadRemoteDirectory ${CreateLoadRemoteDirectory}\n"
  
  CreateCSVRemoteDirectory=$(ssh -o LogLevel=ERROR ${TARGET_DB_HOST} "mkdir -pv ${CSV_EXPORT_PATH}"  2>&1)
  echo -e "INFO: CreateCSVRemoteDirectory ${CreateCSVRemoteDirectory}\n"

  CreateLoadLogsRemoteDirectory=$(ssh -o LogLevel=ERROR ${TARGET_DB_HOST} "mkdir -pv ${LOAD_CFG_LOGS_PATH}"  2>&1)
  echo -e "INFO: CreateLoadLogsRemoteDirectory ${CreateLoadLogsRemoteDirectory}\n"
  
  # Delete all CSV files on target
  ssh -o LogLevel=ERROR ${TARGET_DB_HOST} "rm ${CSV_EXPORT_PATH}/*.csv"
  
  # Copy the CSV files 
  echo -e "INFO: Copy the CSV files to remote server. Running command: scp ${CSV_EXPORT_PATH}/*.csv ${TARGET_DB_HOST}:${CSV_EXPORT_PATH}/ \n"
  scp ${CSV_EXPORT_PATH}/*.csv ${TARGET_DB_HOST}:${CSV_EXPORT_PATH}/

  if [ $? -eq 0 ]; then
    echo -e "INFO: The CSV files have been copied succesfully.\n"
	${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nThe CSV files have been copied succesfully to ${TARGET_DB_HOST}') where id=${MAX_ID};"; echo -e "";
  else
    echo -e "ERROR: Not able to copy the CSV files to ${TARGET_DB_NAME}@${TARGET_DB_HOST}. Exiting from the script..\n"
    ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set STATUT='KO', MESSAGE_ERREUR = 'Not able to copy the CSV file to ${TARGET_DB_NAME}@${TARGET_DB_HOST}', ETAPE = CONCAT(ETAPE,E'\nERROR: Not able to copy the CSV files to ${TARGET_DB_NAME}@${TARGET_DB_HOST}') where id=${MAX_ID};"	
	exit 1;
  fi


  # Copy the load files 
  echo -e "INFO: Copy the load configuration files to remote server. Running command: scp ${LOAD_CFG_FILES_PATH}/*.load ${TARGET_DB_HOST}:${LOAD_CFG_FILES_PATH}/ \n"
  scp ${LOAD_CFG_FILES_PATH}/*.load ${TARGET_DB_HOST}:${LOAD_CFG_FILES_PATH}/

  if [ $? -eq 0 ]; then
    echo -e "INFO: The load configuraton files have been copied succesfully.\n"
	${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nThe load configuraton files have been copied succesfully to ${TARGET_DB_HOST}') where id=${MAX_ID};" ; echo -e "";
  else
    echo -e "ERROR: Not able to copy the load configuraton files to ${TARGET_DB_NAME}@${TARGET_DB_HOST}. Exiting from the script..\n"
    ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set STATUT='KO', MESSAGE_ERREUR = 'Not able to copy the load configuraton file to ${TARGET_DB_NAME}@${TARGET_DB_HOST}', ETAPE = CONCAT(ETAPE,E'\nERROR: Not able to copy the load configuraton files to ${TARGET_DB_NAME}@${TARGET_DB_HOST}') where id=${MAX_ID};"	
	exit 1;
  fi
 
}


# Function to check the log of the load processes and the summary file.
function check_load_log_file_and_summary_file {
  load_log_file="$1"
  summary_file="$2"
  table_name="$3"
  echo -e "INFO: Checking the load log and summary file"
  echo -e "INFO: Load log file: ${load_log_file}"
  echo -e "INFO: Summary file: ${summary_file}"
  echo -e "INFO: Table name: ${table_name}"
  
  
  # If the summary file is not generated means that we have an issue
  if [ ! -f "${summary_file}" ]; then
    echo -e "ERROR: The summary file ${summary_file} is not existing. Exiting from the script."
	${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set STATUT='KO', MESSAGE_ERREUR = 'ERROR: The summary file ${summary_file} is not existing.', ETAPE = CONCAT(ETAPE,E'\nEvaluate ${summary_file}') where id=${MAX_ID};"
	exit 1;
  fi
  
  # If the load log file not generated means that we have an issue
  if [ ! -f "${load_log_file}" ]; then
    echo -e "ERROR: The load log file ${load_log_file} is not existing. Exiting from the script."
	${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set STATUT='KO', MESSAGE_ERREUR = 'ERROR: The load log file ${load_log_file} is not existing.', ETAPE = CONCAT(ETAPE,E'\nEvaluate ${load_log_file}') where id=${MAX_ID};"
	exit 1;
  fi
  
  # Check the log file
  NoOfErros=$( cat ${load_log_file} |  grep -v "report summary reset" | grep -v " pgloader version" | grep -v  "Database error 23505: duplicate key value violates unique constraint"  | grep -wic "ERROR")
  NoOfCriticalErros=$( cat ${load_log_file} | grep -v "report summary reset" | grep -v " pgloader version" | grep -wic "CRITICAL")
  echo -e "INFO: NoOfErros ${NoOfErros}"
  echo -e "INFO: NoOfCriticalErros ${NoOfCriticalErros}"
  if [ "$NoOfErros" -gt 0 -o "$NoOfCriticalErros" -gt 0 ]; then
    echo -e "ERROR: The import has errors. Please check the logfile. Exiting from the script\n"
	${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set STATUT='KO', MESSAGE_ERREUR = 'ERROR: The ${load_log_file} contains erros', ETAPE = CONCAT(ETAPE,E'\nEvaluate ${load_log_file}') where id=${MAX_ID};"
	exit 1;
  else 
    echo -e "INFO: Data for the table ${table_name} has been imported succesfully\n"
    ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nData for the table ${table_name} has been imported succesfully') where id=${MAX_ID};" ; echo -e "";
  fi 
}

# Function to run the pgloader 
function run_load_csv_file {
  load_log_file="$1"
  summary_file="$2"
  table_name="$3" 
  load_cfg_file="$4"
  #echo -e "INFO: Run the pgloader function"
  
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nImporting data for table ${TARGET_DB_SYNCSCHEMA}.${table_name}') where id=${MAX_ID};" ; echo -e "";
  echo -e "INFO: Executing ssh -o LogLevel=ERROR ${TARGET_DB_HOST} \"pgloader --client-min-messages critical --logfile ${LOAD_CFG_LOGS_PATH}/${load_log_file} --quiet --summary ${LOAD_CFG_LOGS_PATH}/${summary_file} ${load_cfg_file}\" \n"
  ImportTraceFlux=$(ssh -o LogLevel=ERROR ${TARGET_DB_HOST} "pgloader --client-min-messages critical --logfile ${LOAD_CFG_LOGS_PATH}/${load_log_file} --quiet --summary ${LOAD_CFG_LOGS_PATH}/${summary_file} ${load_cfg_file}" 2>&1)  
  
}

# Function to load remotly the CSV files. Call pgloader from ${TARGET_DB_HOST}
function load_csv_files {

  #Check which pgloader 
  CheckWhichPGLOADER=$(ssh -o LogLevel=ERROR ${TARGET_DB_HOST} "which pgloader; pgloader --version")
  echo -e "INFO: pgloader information ${CheckWhichPGLOADER}\n"
  
  # t_trace_flux
  run_load_csv_file pex_trc_t_trace_flux.log pex_trc_t_trace_flux.summary "t_trace_flux" ${PEX_TRC_T_TRACE_FLUX_LOAD_FILE}
  # t_trace_service
  run_load_csv_file pex_trc_t_trace_service.log pex_trc_t_trace_service.summary "t_trace_service" ${PEX_TRC_T_TRACE_SERVICE_LOAD_FILE}
  # t_trace_donnees_metier
  run_load_csv_file pex_trc_t_trace_donnees_metier.log pex_trc_t_trace_donnees_metier.summary "t_trace_donnees_metier" ${PEX_TRC_T_TRACE_DONNEES_METIER_LOAD_FILE}
  # t_trace_info
  run_load_csv_file pex_trc_t_trace_info.log pex_trc_t_trace_info.summary "t_trace_info" ${PEX_TRC_T_TRACE_INFO_LOAD_FILE}
  # t_trace_log
  run_load_csv_file pex_trc_t_trace_log.log pex_trc_t_trace_log.summary "t_trace_log" ${PEX_TRC_T_TRACE_LOG_LOAD_FILE}
  # t_trace_log_erreur
  run_load_csv_file pex_trc_t_trace_log_erreur.log pex_trc_t_trace_log_erreur.summary "t_trace_log_erreur" ${PEX_TRC_T_TRACE_LOG_ERREUR_LOAD_FILE}
  
 
  # mkdir -pv ${LOAD_CFG_LOGS_PATH} locally
  mkdir -pv ${LOAD_CFG_LOGS_PATH}
  if [ ! -w ${LOAD_CFG_LOGS_PATH} ]; then 
    echo -e "ERROR: The directory for pgloader logs doesn't exist. Path: ${LOAD_CFG_FILES_PATH}\nExiting from the script..\n\n" 
	${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set STATUT='KO', MESSAGE_ERREUR = 'ERROR: The directory for pgloader logs doesn't exist. Path: ${LOAD_CFG_FILES_PATH}', CONCAT(ETAPE,E'\nCreate the directory for the logs of load files') where id=${MAX_ID};"
	exit 1;
  else 
    echo -e "INFO: The directory where load pgloader log files are generated: ${LOAD_CFG_LOGS_PATH}\n"
  fi
  
  # Copy the load logs files from target locally
  echo -e "INFO: Copy the load logs files from the remote server. Running command: scp -r ${TARGET_DB_HOST}:${LOAD_CFG_LOGS_PATH}/ ${LOAD_CFG_LOGS_PATH}/ \n"
  scp -r ${TARGET_DB_HOST}:${LOAD_CFG_LOGS_PATH}/ ${LOAD_CFG_LOGS_PATH_ROOT}/
   if [ $? -eq 0 ]; then
    echo -e "INFO: The load logs files from the remote server have been copied succesfully.\n"
	${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set ETAPE = CONCAT(ETAPE,E'\nThe load logs files have been copied succesfully to ${DB_HOST} from ${TARGET_DB_HOST}') where id=${MAX_ID};"; echo -e "";
  else
    echo -e "ERROR: Not able to copy the load logs files from ${TARGET_DB_HOST} to ${DB_HOST}. Exiting from the script..\n"
    ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set STATUT='KO', MESSAGE_ERREUR = 'Error code: $?', ETAPE = CONCAT(ETAPE,E'\nERROR: Not able to copy the load logs files files from  ${TARGET_DB_NAME} to ${DB_HOST}') where id=${MAX_ID};"	
	exit 1;
  fi
  
  
  check_load_log_file_and_summary_file ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_flux.log ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_flux.summary "pex_trc.t_trace_flux"
  check_load_log_file_and_summary_file ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_service.log ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_service.summary "pex_trc.t_trace_service"
  check_load_log_file_and_summary_file ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_donnees_metier.log ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_donnees_metier.summary "pex_trc.t_trace_donnees_metier"
  check_load_log_file_and_summary_file ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_info.log ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_info.summary "pex_trc.t_trace_info"
  check_load_log_file_and_summary_file ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_log.log ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_log.summary "pex_trc.t_trace_log"
  check_load_log_file_and_summary_file ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_log_erreur.log ${LOAD_CFG_LOGS_PATH}/pex_trc_t_trace_log_erreur.summary "pex_trc.t_trace_log_erreur"
  
  
  DATE_SCRIPT_COMPLETE=$(date +"%Y-%m-%d %H:%M:%S")
  ${PSQL} -h ${DB_HOST} -p ${DB_PORT} -d ${DB_NAME} -U ${DB_USER} -c "UPDATE ${DB_SYNCSCHEMA}.${DB_SYNCTABLE} set date_script_complete='${DATE_SCRIPT_COMPLETE}' ,statut='OK', ETAPE = CONCAT(ETAPE,E'\nThe CSV files imported.') where id=${MAX_ID};" ; echo -e "";
}




### Main
insert_start_time
extract_csv_file
generate_load_files
copy_load_files
load_csv_files
clean_old_logs
clean_old_status_records
echo -e "INFO: Script $0 completed at $(date +"%Y-%m-%d %H:%M:%S")\n"



