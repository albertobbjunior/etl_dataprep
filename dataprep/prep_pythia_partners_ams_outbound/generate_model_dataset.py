
import sys
import os
import logging
from pyspark.sql import DataFrame,SparkSession
sys.path.append(os.path.join(os.path.dirname("__file__"),'../../'))
from lib.dataprep_sql_targets import fetch_and_save_targets



def provide_hub_target(dataframe:DataFrame,
                       start_date:str,
                       end_date:str,
                       spark:SparkSession
                       ) -> DataFrame:
    df_sql_targets = fetch_and_save_targets(
            spark=spark,
            start_date=start_date,
            end_date=end_date,
            hubdb_username=os.environ["user_sql_dataprep"],
            hubdb_password=os.environ["password_sql_dataprep"],
            hubdb_database='BlueLagoonMart'
        )
    df = dataframe.join(df_sql_targets, on="row_key:full", how="left")
    return df



def run_dataprep_model_dataset(dataframe:DataFrame,
                               start_date:str,
                               end_date:str,
                               spark:SparkSession
                              ) -> DataFrame:

    return provide_hub_target(dataframe=dataframe,
                             start_date=start_date,
                             end_date=end_date,
                             spark=spark
                             )
    