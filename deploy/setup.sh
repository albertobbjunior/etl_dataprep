#!/bin/bash
version='0.0.3'
chmod +x ./$version/setup.sh
echo "[INFO] -  - CURRENT_VERSION: ${version}"
folder_deploy="${1}"
[[ -z ${folder_deploy} ]] && { echo "[ERROR] - ${FUNCNAME}(): user name empty"; exit 1; }
folder_deploy_temp="${2}"
[[ -z ${folder_deploy_temp} ]] && { echo "[ERROR] - ${FUNCNAME}(): password name empty"; exit 1; }
#Call setup version with deploy that will be executed.
./$version/setup.sh ${folder_deploy} ${folder_deploy_temp} 
