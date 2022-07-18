#!/bin/bash 

####################################################################
#                        START CODE                                #
####################################################################
#GET bambo variables
echo "[INFO] - Running Deploy ETL cluster version ${version}..."
echo "[INFO] - ETL Setup - Set Variables"
folder_deploy=$1
folder_deploy_temp=$2

#Print bambo variables
echo "${folder_deploy_temp}"
echo "${folder_deploy}"

#Confirm if variables were setted
echo "[INFO] - ETL  Setup - Check Bamboo Variables"
if [[ -z "$folder_deploy_temp" ]] ||  [[ -z "$folder_deploy" ]] 
 then echo "[Error] - ETL Setup -mandatory variables Empty"
 exit 1
fi 

# ----------------- Start Deploy -----------------------
#Remove Folder
#rm -rf $folder_deploy/mlops-odsds/
mv -f $folder_deploy_temp/docker/dataprep/files/app-default/dataprep/  ${folder_deploy}
chmod +x $folder_deploy/dataprep/init.sh
dos2unix $folder_deploy/dataprep/init.sh

# ----------------- Deploy Finished -----------------------  

#####

echo "[INFO] - ETL Setup  for version ${version} was successful!"