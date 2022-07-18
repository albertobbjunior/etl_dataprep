#!/bin/bash 

####################################################################
#                 START FUNCTIONS                                  #
####################################################################
function create_docker_build {
    bamboo_host_registy=${1}
    [[ -z ${bamboo_host_registy} ]] && { echo "[ERROR] - ${FUNCNAME}(): host registy name empty"; exit 1; }
    bamboo_project_name=${2}
    [[ -z ${bamboo_project_name} ]] && { echo "[ERROR] - ${FUNCNAME}(): project name empty"; exit 1; }
    docker_name="${3}"
    [[ -z ${docker_name} ]] && { echo "[ERROR] - ${FUNCNAME}(): docker name empty"; exit 1; }
    echo 'Building a new docker file' $docker_name:latest
    echo ${bamboo_host_registy}/${bamboo_project_name}/mlops_${docker_name}:latest .
    (cd ../docker/${docker_name} \
      && docker build -t ${bamboo_host_registy}/${bamboo_project_name}/mlops_${docker_name}:latest .)
    echo "[INFO] - PKS Cluster Setup - Finished the docker build $docker_name:latest"
}


function registry_docker_harbor {
    bamboo_user_name="${1}"
    [[ -z ${bamboo_user_name} ]] && { echo "[ERROR] - ${FUNCNAME}(): user name empty"; exit 1; }
    bamboo_password="${2}"
    [[ -z ${bamboo_password} ]] && { echo "[ERROR] - ${FUNCNAME}(): password name empty"; exit 1; }
    bamboo_host_registy=${3}
    [[ -z ${bamboo_host_registy} ]] && { echo "[ERROR] - ${FUNCNAME}(): host registy name empty"; exit 1; }
    bamboo_project_name=${4}
    [[ -z ${bamboo_project_name} ]] && { echo "[ERROR] - ${FUNCNAME}(): project name empty"; exit 1; }
    docker_name=${5}
    [[ -z ${docker_name} ]] && { echo "[ERROR] - ${FUNCNAME}(): docker name  empty"; exit 1; }
    echo "[INFO] - PKS Cluster Setup - Login in Harbor Cluster $docker_name:latest"
    docker login ${bamboo_host_registy} -u=${bamboo_user_name} -p=${bamboo_password}
    echo "[INFO] - PKS Cluster Setup - Push docker to Harbor Cluster in Harbor Cluster $docker_name:latest"
    docker push ${bamboo_host_registy}/${bamboo_project_name}/mlops_${docker_name}:latest
    echo "[INFO] - PKS Cluster Setup - Finished the docker push $docker_name:latest"
}

####################################################################
#                        START CODE                                #
####################################################################
#GET bambo variables
bamboo_user_name=$1
bamboo_password=$2
bamboo_host_register=$3
bamboo_project_name=$4
version=$5
echo "[INFO] - Running PKS cluster Setup for version ${version}..."
echo "[INFO] - PKS Cluster Setup - Set Variables"

#Print bambo variables
echo "${1}"
echo "${2}"
echo "${3}"
echo "${4}"

#Confirm if variables were setted
echo "[INFO] - PKS Cluster Setup - Check Bamboo Variables"
if [[ -z "$bamboo_user_name" ]] ||  [[ -z "$bamboo_password" ]] || [[ -z "$bamboo_host_register" ]] || [[ -z "$bamboo_project_name" ]]
 then echo "[Error] - PKS Cluster Setup -mandatory variables Empty"
 exit 1
fi 


# ----------------- Start Docker Deploy -----------------------
echo "[INFO] - PKS Cluster Setup - Enter in Deploy folders"
pwd
echo "[INFO] - PKS Cluster Setup - List Docker folders "
list_docker_files=`echo $(cd ../docker && ls -d */)`
list_docker_files=`echo $(cd ../docker && ls -d */ |tr -d "/")`
# ----------------- Create loop among folders -----------------------
echo "[INFO] - PKS Cluster Setup - List of docker -> ${list_docker_files} " 
for folder_name in $list_docker_files
  do
    echo "[INFO] - PKS Cluster Setup - Start docker process -> ${folder_name}"
    create_docker_build "${bamboo_host_register}" "${bamboo_project_name}" "${folder_name}" 
    registry_docker_harbor "${bamboo_user_name}" "${bamboo_password}" "${bamboo_host_register}" "${bamboo_project_name}" "${folder_name}"
  done

# ----------------- Deploy Finished -----------------------  

#####

echo "[INFO] - PKS Cluster Setup  for version ${version} was successful!"
