
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,row_number,lit
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark import StorageLevel
import yaml
import logging
from datetime import datetime
import sys
import os

def start_spark_with_retries(script_name: str, num_attempts: int = 5) -> SparkSession:
    attempt_count = 1
    while attempt_count <= num_attempts:
        try:
            spark = start_spark(script_name)
            logging.info("Spark context initialized: {}".format(spark.version))
            application_log = "http://kl143fv2.is.klmcorp.net:8188/applicationhistory/app/{}".format(
                spark.sparkContext.applicationId
            )
            logging.info(f"Application log can be found here: {application_log}")
            return spark
        except Exception as e:
            logging.warning(f"Could not establish spark context on attempt {attempt_count}")
            logging.warning(f"\t{e}")
            attempt_count += 1

    failure_msg = f"Could not establish spark context after {num_attempts} attempts. THE SCRIPT DID NOT RUN!"
    logging.critical(failure_msg)
    raise Exception(failure_msg)


def start_spark(app_name: str) -> SparkSession:
    """
    Obtain a spark session. This function is mainly used for the scripts.

    :param app_name: name of the spark session
    :return: SparkSession
    """
    builder = SparkSession.builder.appName(app_name)
    session = builder.master('yarn').getOrCreate()
    session.sparkContext.setLogLevel("INFO")
    return session



def read_config_file(file_path:str)->dict:
    """Returns a dataset with filters applied in config.yaml file.
    :param file_path: Config file.
    :return: Python dict with all config data file.
    """
    with open(file_path) as file:
        config_dict = yaml.load(file,Loader=yaml.FullLoader)
    return config_dict


def getlogger(name,level=logging.INFO,project_name:str='prep_pythia_partners_ams_inbound'):
    

    logger = logging.getLogger('')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    date_log = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    folder=f'../log/{project_name}/'
    if logger.handlers:
        # or else, as I found out, we keep adding handlers and duplicate messages
        pass
    else:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.setFormatter(formatter)
        os.makedirs(os.path.dirname(folder), exist_ok=True)
        file_handler = logging.FileHandler(f'../log/{folder}/{date_log}.log')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stdout_handler)
    return logger
