from pyspark.sql import DataFrame, SparkSession
from settings.preprocessing import HUBDB_PORT,HUBDB_SERVER,SPARK_ODBC_DRIVER



def fetch_and_save_targets(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    hubdb_username: str,
    hubdb_password: str,
    hubdb_database:str
) -> DataFrame:
    """
    Query the targets from HubDBMartBO, process the data and save.

    :param spark: SparkSession
    :param dataset_version: str with the dataset version. Example 0.0.1
    :param start_date: str with start date. Format: %Y-%m-%d
    :param end_date: str with end date. Format: %Y-%m-%d
    :param hubdb_username: str with the hubdb username
    :param hubdb_password: str with the hubdb password
    :return:
    """
    target_query = return_target_query(start_date, end_date)

    query_as_table = "(" + target_query + ") AS t"

    df_targets = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:sqlserver://{HUBDB_SERVER}:{HUBDB_PORT};database={hubdb_database}")
        .option("driver", SPARK_ODBC_DRIVER)
        .option("dbtable", query_as_table)
        .option("user", hubdb_username)
        .option("password", hubdb_password)
        .load()
    )

    return df_targets


def return_target_query(start_date: str, end_date: str) -> str:
    """
    Return a query string to obtain the targets.

    The targets are fetched from a view that Burak made for this use case.

    :param start_date: str with start date. Format: %Y-%m-%d
    :param end_date: str with end date. Format: %Y-%m-%d
    :return: query str
    """
    target_query = f"""
SELECT
    f720_flight_legs_rowkey AS 'row_key:full',
    pax_arriving_local AS target_pax_arriving_local,
    pax_arriving_transfer AS target_pax_arriving_transfer,
    number_bags_arriving_local AS target_number_bags_arriving_local,
    number_bags_arriving_transfer AS target_number_bags_arriving_transfer
FROM dbo.vwGenerateInboundPartnerModelData
WHERE head_station_departure_date_utc BETWEEN '{start_date}' AND '{end_date}'
    """
    return target_query