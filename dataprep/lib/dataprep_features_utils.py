
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,first
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import yaml
from lib.dataprep_utils import format_column_name_atribute



#format features columns as flight720 template
def add_features_columns(input_dataframe:DataFrame,
                          columns_features:list)-> DataFrame:
    """
    Format Feature columns as same as Fligh720 format

    :param input_dataframe: Dataframe processed.
    :param columns_features: Config File - feature
    :return: Dataframe
    """
    for feature_column_familiy in columns_features:
        for column_name in columns_features[feature_column_familiy]:
            attribute = feature_column_familiy+':'+column_name
            input_dataframe = input_dataframe.withColumn('feature:'+attribute, col(attribute))\
                                         .drop(col(attribute))
    return input_dataframe

#def format_column_name_atribute(filters_dict:list,filter_level='target') ->str:
#    if (filters_dict.get('column_family') == 'row_key') |  (filter_level is None) :
#        column_name_atribute = f"{filters_dict.get('column_family')}:{filters_dict.get('qualifier')}"
#    else:   
#        column_name_atribute = f"{filter_level}:{filters_dict.get('column_family')}:{filters_dict.get('qualifier')}"
#    return column_name_atribute

def apply_features_filters(input_dataframe:DataFrame,
                                          features_filters:list)-> DataFrame:
    """Applies filters in feature level - Filters were declared in config file.
    Note: In general filters are applied in target level, but this function was
    implemented to remove all null columns informed on gerenic_filters_list. 
    :param input_dataframe: Dataframe processed.
    :param columns_features: Config File Filters
    :return: Dataframe filtered

    """
    if(features_filters):
        w = Window.partitionBy('row_key:full').orderBy(col("etl:event_timestamp").desc())
        for config_filters_dict in features_filters:
                column_name_atribute =format_column_name_atribute(config_filters_dict,None)
                input_dataframe = input_dataframe.withColumn('filter'+column_name_atribute,first(col(column_name_atribute+'.value').getItem(0),True).over(w))\
                    .filter(col('filter'+column_name_atribute).isNotNull())\
                    .drop(col('filter'+column_name_atribute))
    return input_dataframe