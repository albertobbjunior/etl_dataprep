
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,first
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from lib.dataprep_utils import filter_list_by_find_word
from lib.dataprep_utils import format_column_name_atribute
import yaml


def apply_filters(input_dataframe:DataFrame,
                  config_filters_dict:list)->DataFrame:
    """
    Returns a dataset with type filter ex. = Isnull, NoNull or List of Values.
    The target column will be created using the latest row_key data.
    :param input_dataframe: Raw Dataframe data.
    :param config_filters_dict: Filter Applied.
    :return: A DataFrame filtered (Config File)
    """
    column_name = format_column_name_atribute(config_filters_dict,filter_level='target')     
    if "operator" in config_filters_dict:
         df_filter =  col(column_name).isin(config_filters_dict.get('filter_value').split(","))
    elif "include_missing" in config_filters_dict:
        if config_filters.get('include_missing')=='False':
            df_filter = col(column_name).isNotnull()
        else: 
            df_filter = col(column_name).isNotnull()
    else:
         print('error')   
    return input_dataframe.filter(df_filter)
    
def apply_target_filters(input_dataframe:DataFrame,config_file_filters:list)->DataFrame:
    """
    Returns a dataset with filters applied in config.yaml file.
    The target column will be created using the latest row_key data.
    :param input_dataframe: Raw Dataframe data.
    :param config_file_filters: Filters applied in target level.
    :return: A DataFrame filtered (Config File)
    """
    for config_filters_dict in config_file_filters:
        input_dataframe = apply_filters(input_dataframe=input_dataframe,
                                                 config_filters_dict=config_filters_dict)
    return input_dataframe

#Explode Array in  value column -  change it  [value="a",timestamp=1746548] - to - "a"
def add_targets_columns(input_dataframe:DataFrame,
                        targets_columns:list,
                        partition_data:str,order_by_data:str
                        )-> DataFrame:
    """
    Returns a dataset containing targets on config.yaml file.
    The target column will be created using the latest row_key data.
    :param input_dataframe: Raw Dataframe data.
    :param targets_columns: Columns applied on Config file.
    :param partition_data: Group of data that will be used in analytical function.
    :param order_by_data: How the data will be classified as latest data.
    :return: a DataFrame containing all datas and new target columns
    """
    w = Window.partitionBy(partition_data).orderBy(col(order_by_data).desc())
    for target_column_familiy in targets_columns:
        for column_name in targets_columns[target_column_familiy]:
            attribute = target_column_familiy+':'+column_name
            input_dataframe = input_dataframe.withColumn('target:'+attribute,first(col(attribute+'.value').getItem(0),True).over(w))
    return input_dataframe

def drop_target_columns(df:DataFrame)-> DataFrame:
    """
    Returns a dataset removing all target columns and row_key columns.
    :param input_dataframe: Dataframe data.
    :return: a DataFrame without target and row_key column
    """
    list_schema_column_names = df.schema.names
    list_schema_column_names_targets = filter_list_by_find_word(list_schema_column_names,['target','row_key:'])
    for column_name in list_schema_column_names_targets:
        df = df.drop(col(column_name))
    return df
