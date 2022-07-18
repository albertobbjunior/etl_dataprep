from lib.dataprep_files_utils import read_config_file
from lib.dataprep_utils import slice_rowkey
from lib.dataprep_target_utils import add_targets_columns
from lib.dataprep_target_utils import apply_target_filters
from lib.dataprep_target_utils import drop_target_columns
from lib.dataprep_features_utils import apply_features_filters
from lib.dataprep_features_utils import add_features_columns
from lib.dataprep_query_moment_utils  import add_query_moment
from lib.dataprep_query_moment_utils import add_diff_hours_column
from lib.dataprep_utils import genarate_end_result
from lib.dataprep_files_utils import save_file
from lib.dataprep_files_utils import read_parquet_file
from lib.dataprep_utils import generate_model_dataset
from lib.dataprep_spark_utils import getlogger
from lib.dataprep_spark_utils import start_spark_with_retries
from lib.dataprep_files_utils import validade_path_project_name
from lib.dataprep_files_utils import validade_config_file_parameters
import os



def run_dataprep(args:list):
    

    project_name =  args.project
    
    file_path = validade_path_project_name(project_name)
    model_dict = read_config_file(file_path)
    path_project=os.getcwd()
    validade_config_file_parameters(path_project=path_project,dataprep=model_dict)
    logging = getlogger(name=f"Project:{model_dict.data.get('project_name')} - Version:{model_dict.data.get('mode')}",project_name=project_name)
    

    spark =  start_spark_with_retries(project_name)
    spark.sparkContext.setLogLevel("Error")
    
    
    if(model_dict.data.get('mode')=='cluster'):
        print('cluster---------')
        date_ini = args.date_start
        date_end = args.date_end
    else: 
        print('out-------------')
        date_ini = model_dict.data.get('date_window_start')
        date_end = model_dict.data.get('date_window_end')
    print(date_ini)
    print(date_end)

    logging.info(f"Processing data between {date_ini} - {date_end} ")
    logging.info("Reading parquet files")
    df_raw = read_parquet_file(spark_session=spark,
                                parquet_read_folder=model_dict.data.get('input_folder'),
                                date_window_start=date_ini,
                                date_window_end=date_end
                                )
    df_raw.count()  
    logging.info("Slicing row_key.")
    df_raw_slice = slice_rowkey(input_dataframe=df_raw,
                                row_key_name_columns=model_dict.row_key_split)


    # #Convert columns in target Columns
    logging.info("Providing target columns ")
    df_targets = add_targets_columns(input_dataframe=df_raw_slice,
                                     targets_columns=model_dict.targets_columns,
                                     partition_data='row_key:full',
                                     order_by_data='etl:event_timestamp')

    #Apply filters level target
    logging.info("Filtering special flights on target level")
    df_targets = apply_target_filters(input_dataframe=df_targets,
                                      config_file_filters=model_dict.target_filters)

    #Apply filters level features
    logging.info("Filtering special flights on Feature level")
    df_targets = apply_features_filters(input_dataframe=df_targets,
                                         features_filters=model_dict.features_filters)

    logging.info("Providing queries moment")
    df_query_moment = add_query_moment(spark_session=spark,
                                       query_moment_conf=model_dict.query_moment_conf,
                                       input_dataframe=df_targets)

    #Creates dataframe with all features
    #df_query_moment.count()
    logging.info("Providing features columns")
    df_features =  add_features_columns(input_dataframe=df_targets,
                                         columns_features=model_dict.features_column)

    #Creates dataframe with all features
    logging.info("Providing diff_hours_column")
    df_features = add_diff_hours_column(input_dataframe=df_features,
                                        row_key='rowkey:full',
                                        query_moment_reference=model_dict.query_moment_reference
                                        )
    logging.info("Dropping target columns in feature dataframe")
    df_features = drop_target_columns(df_features)
    
    logging.info("Providing Dataset join targets, query_moment and features")
    #df_feture.show(truncate=False,n=1,vertical=True)
    df_end_result = genarate_end_result(input_dataframe=df_features,
                                        query_moment_dataframe=df_query_moment)
    

    
    df_generate_model_dataset = generate_model_dataset(input_dataframe=df_end_result,
                                                       project_name=project_name,
                                                       start_date=date_ini,
                                                       end_date=date_end,
                                                       spark=spark
                                                       )
    #df_generate_model_dataset.show(n=3,truncate=False,vertical=True)
    logging.info("Converting dataframe in parquet_files")
    save_file(output_dataframe=df_generate_model_dataset,
              dataprep=model_dict,
              spark_session=spark,
              file_path=file_path
              
              )
    logging.info("Dataprep finished.")