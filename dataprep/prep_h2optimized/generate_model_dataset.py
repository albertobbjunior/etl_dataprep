
import sys
import os
import logging
from pyspark.sql import DataFrame,SparkSession
sys.path.append(os.path.join(os.path.dirname("__file__"),'../../'))


def run_dataprep_model_dataset(dataframe:DataFrame,
                               start_date:str,
                               end_date:str,
                               spark:SparkSession
                              )  -> DataFrame:
    """implement targets, filters in the end the dataframe will be return to
    baseline project."""
    return dataframe
