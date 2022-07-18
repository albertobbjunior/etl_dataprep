from typing import Type


class Dataprep():

   def __init__(self,
                 data_parameters,
                 row_key_split,
                 features_column,
                 targets_columns,
                 target_filters,
                 features_filters,
                 query_moment_conf,
                 query_moment_reference
                 ):
        self.data  = data_parameters
        self.row_key_split    = row_key_split
        self.features_column  = features_column
        self.targets_columns  = targets_columns
        self.target_filters   = target_filters
        self.features_filters = features_filters
        self.query_moment_conf = query_moment_conf
        self.query_moment_reference  = query_moment_reference