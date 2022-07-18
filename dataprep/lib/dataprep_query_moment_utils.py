
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_extract,when,last,unix_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.types import StringType,IntegerType, DoubleType
from pyspark.sql.functions import *
import yaml

from lib.dataprep_utils import filter_list_by_find_word

def add_diff_hours_column(input_dataframe:DataFrame,row_key:str,query_moment_reference:dict)->DataFrame:
    """Apply diff between event timestamp and query_moment_reference datetime.
    :param input_dataframe: Dataframe processed.
    :param row_key: Key in order to provide a new column
    :param query_moment_reference: Datetime will be use in order to get diff between the timestamp and reference date
    :return: Dataframe filtered
    """ 
    df_column_name= 'target:'+query_moment_reference.get('column_family')+":"+query_moment_reference.get('qualifier')
    input_dataframe =  input_dataframe\
                            .withColumnRenamed('row_key:full', f'feature_{row_key}')\
                            .withColumn('diff_hours', 
                                    ((unix_timestamp(substring(df_column_name,0,16),"yyyy-MM-dd'T'HH:mm")
                                      *1000) - col('etl:event_timestamp')
                                      )/3600000#Convert milliseconds in hours
                        )   
    return input_dataframe


#Create frame with query moment frames
def get_query_moment(spark_session:SparkSession,query_moment_conf:list) -> DataFrame:
    """Apply query moment column.
    :param spark_session: SparkSession process.
    :param query_moment_conf: List of query_moment declarated in ConfigFile
    :param query_moment_reference: Datetime will be use in order to get diff between the timestamp and reference date
    :return: DataFrame =  Dataframe will query moment converting query_moment_fixed list in total of hours
        Example: String List relative to a reference moment of an entity. As an example: if a model makes a 
                            prediction 2 hours and 4 hours before a flight departs, its query moments are '2h' and 4h'. 
        """
    reg_extract = r'([0-9]+)'
    column_name = 'value'
    df_query_moment = spark_session.createDataFrame(query_moment_conf, StringType())
    return df_query_moment \
                        .withColumn('qt_hours',(when(regexp_extract(column_name, reg_extract, 1)  == 'd', (regexp_extract(column_name, reg_extract, 1)).cast(IntegerType())*24))
                        .otherwise((regexp_extract(column_name, reg_extract, 1)).cast(IntegerType())))\
                        .withColumn('qt_hours', col('qt_hours').cast(DoubleType()))\
                        .withColumnRenamed('value', 'query_moment_value')
                            
def add_query_moment(spark_session:SparkSession,
                    query_moment_conf:list,
                    input_dataframe:DataFrame)-> DataFrame:
    """
        Method provide a cross join between  df_query_fixe and df_format . 
        :param spark_session: SparkSession process.
        :param query_moment_conf: List of query_moment declarated in ConfigFile
        :param query_moment_reference: Datetime will be use in order to get diff between the timestamp and reference date
        :param input_dataframe: Dataset 
        :return:Dataset with the last data informed by column.
    """
    list_schema_column_names = input_dataframe.schema.names
    w = Window.partitionBy('row_key:full').orderBy(col("etl:event_timestamp").desc())
    
    list_schema_column_names_targets = filter_list_by_find_word(list_schema_column_names,['target','row_key:full','row_key:departure_date','row_key:carrier_name','row_key:flight_number','row_key:flight_number','row_key:departure_airport','rowKey'])
    for column_name in list_schema_column_names_targets:
        input_dataframe = input_dataframe.withColumn(column_name,last(col(column_name),True).over(w))
        
    df_query_fixed = get_query_moment(spark_session=spark_session,query_moment_conf=query_moment_conf)    
    return   df_query_fixed.crossJoin(input_dataframe.select(list_schema_column_names_targets).distinct())