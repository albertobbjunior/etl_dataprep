#!/bin/bash 
####################################################################
#                 Function ETL Process                             #
#              Move data from HBASE TO HDFS                        #
####################################################################

function etl_process_data_from_hbase_to_hdfs {
    BASEDIR=$(dirname $0)    
    cd ${BASEDIR}
    SPARK_SUBMIT_CONF=${BASEDIR}/spark_submit.conf
    #export JAVA_HOME="/tech/java/oracle/1.8/"
    #export SPARK_DIST_CLASSPATH=$(hadoop --config /usr/hdp/current/hadoop-client/conf classpath)
    #export HADOOP_CONF_DIR="/usr/hdp/current/hadoop-client/conf"
    #export SPARK_HOME=/app/apache/spark/spark-3.1.2/
    export PATH=$SPARK_HOME/bin:$PATH
    export PYSPARK_PYTHON=/tech/local/miniconda/bin/python

    
    spark-submit \
    --jars jars/sqljdbc42.jar \
    --master yarn \
    --conf spark.shuffle.service.enabled=true \
    --queue preference \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=8g \
    --conf spark.executor.cores=9 \
    --conf spark.speculation=true \
    --conf spark.executor.instances=5 \
    --conf spark.driver.memoryOverhead=800 \
    --conf spark.yarn.memoryOverhead=800 \
    --conf spark.sql.crossJoin.enabled="true" \
    --conf spark.dynamicAllocation.executorIdleTimeout=2m \
    --conf spark.dynamicAllocation.enable=true \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/tech/local/miniconda/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/tech/local/miniconda/bin/python \
    --py-files ${BASEDIR}/lib \
    --properties-file ${SPARK_SUBMIT_CONF} \
    ${BASEDIR}/process_data.py -s ${1} -e ${2} -p ${3}
     
} 
PROJECT_NAME=$1

if [ -z "$2" ]; then
        STARTDATE=$(date +%Y-%m-%d)
else 
        STARTDATE=$2
fi    
if [ -z "$3" ]; then
        ENDDATE=$STARTDATE
else 
        ENDDATE=$3
fi

####################################################################
#                 Call Functions                                   #
####################################################################
echo "-----------Start ETL Process------------------"
echo "Start Model Process  -> $PROJECT_NAME " 
#CALL FUNCTION
if [[ -z "${ENV}" ]]; then
   etl_process_data_from_hbase_to_hdfs  $STARTDATE $ENDDATE $PROJECT_NAME 
else
    spark-submit --jars jars/sqljdbc42.jar process_data.py -s ${STARTDATE} -e ${ENDDATE} -p ${PROJECT_NAME}
fi
        

echo "ETL process finished"
echo "------------End ETL Process-----------------"

echo "                                              "
echo "                                              "
echo " -                                             "

