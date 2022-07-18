
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,row_number,lit
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark import StorageLevel
import yaml
import logging

import importlib



def slice_rowkey(input_dataframe:DataFrame,row_key_name_columns:list)->DataFrame:
    """Get row_key data and convert in 7 new columns
    :param input_dataframe: Raw dataframe read from HDFS files.
    :param row_key_name_columns: Parts of row_key in list of string.
    :return: Dataframe with new row_key columns
    """
    split_col = split(col('rowkey'), '\|')
    for idx,attribute in enumerate(row_key_name_columns):
        input_dataframe = input_dataframe.withColumn(attribute,split_col.getItem(idx))
    return input_dataframe.withColumnRenamed('rowKey', 'row_key:full')   


def list_sub(list1,list2) -> list:
    """Gets two list and return diff between list1 and list2
    :param list1: List.
    :param list2: List.
    :return: List with diff between  list1 and list2
    Ex.: a=[1,2,3] b=[1,2] = list(a,b) = 3 
    """
    return sorted(set(list1)-set(list2))

def filter_list_by_find_word(list_1:list,list_2:list)->list:
    """Method returns strings in List_1 that between 
    the start and end indexes  in strings List_2
    :param list1: List.
    :param list2: List.
    Return List filtered by b
    Ex.:   a = ['Barack Obama','Johan Cruyff','Dennis Bergkamp']
            b = ['Barack','Bergkamp']
        Return ['Barack Obama', 'Dennis Bergkamp']
    """
    return [x for x in list_1 if any(x for y in list_2 if  str(y) in x)]

def format_column_name_atribute(filters_dict:list,filter_level:str) ->str:
    """
    Convert Config_Filter.yaml filter in same layout as Dataframe: level:column_family:qualifier 

    :param filters_dict: filter dictionary
    :param filter_level: Target or feature
    :return: Str as filter_level:column_family:qualifier layout
    """  
    if (filters_dict.get('column_family') == 'row_key') |  (filter_level is None) :
        column_name_atribute = f"{filters_dict.get('column_family')}:{filters_dict.get('qualifier')}"
    else:   
        column_name_atribute = f"{filter_level}:{filters_dict.get('column_family')}:{filters_dict.get('qualifier')}"
    return column_name_atribute

def genarate_end_result(input_dataframe:DataFrame,
                    query_moment_dataframe:DataFrame
                    )->DataFrame:
    """
    Process the join features, query_moment_dataframe(query_moment and targets) and provides a end_result dataframe
    :param input_dataframe: features dataframe
    :param query_moment_dataframe: query_moment_dataframe(query_moment and targets) dataframe
    :return: DataFrame
    """  
    w = Window.partitionBy('row_key:full','query_moment_value').orderBy(col("etl:event_timestamp").desc())

    df_result =  query_moment_dataframe\
                            .join(input_dataframe,(query_moment_dataframe['row_key:full'] == input_dataframe['feature_rowkey:full']) &
                                  (query_moment_dataframe.qt_hours <= input_dataframe.diff_hours)
                                  ,'left')\
                            .withColumn('rn',row_number().over(w))\
                            .filter('rn == 1')    

    list_schema_column_names = df_result.schema.names
    list_default_columns = ['target','row_key:','feature','query_moment_value']
    return  df_result\
                    .select(filter_list_by_find_word(list_schema_column_names,list_default_columns))\



def generate_model_dataset(
                            input_dataframe:DataFrame,
                            project_name:str,
                            start_date:str,
                            end_date:str,
                            spark:SparkSession) -> DataFrame:
    logging.info(f"Providing specific filters, targets and features to Project-Name: {project_name} ")
    library_model = importlib.import_module(f'{project_name}.generate_model_dataset')
    return library_model.run_dataprep_model_dataset(dataframe=input_dataframe,
                                                    start_date=start_date,
                                                    end_date=end_date,
                                                    spark=spark
                                                    )
