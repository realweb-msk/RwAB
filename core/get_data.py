from google.cloud import bigquery
from google.oauth2 import service_account
from core.exceptions import InvalidDataType, InvalidInput
import re


def query(table_name, start_date, end_date, experiment_id, events=None, custom_dimensions=None, source='ga',
          additional_dimensions=None, additional_metrics_query=None):
    """
    Функция получения данных в дефолтных разрезах из данных стриминга GA в BQ.
    Подробнее про схему читайте в https://github.com/realweb-msk/RwAB

    :param source:
    :param table_name: (str), Название таблицы BQ в формате: projectID.datasetID
    :param start_date: (str), Начальная дата в формате YYYYmmdd
    :param end_date:(str), Конечная дата в формате YYYYmmdd
    :param experiment_id: (str), ID эксперимента
    :param events: (dict, optional, default=None), Словарь с дополнительными событиями. Словарь формата:
        {'field_name': ['eventAction', 'eventCategory']}, например: {'pep': ['Ecom', 'Click']}
    :param custom_dimensions: (dict, optional, default=None), Словарь с пользовательскими параметрами. Словарь формата:
        {'field_name': [customDimensionIndex(int), level ('hits', 'session', 'user')}
    :param additional_dimensions: (list, optional, default=None), Список с одним из следующий параметров из :
            visitNumber
            trafficSource.*
            trafficSource.adwordsClickInfo.*
            device.*
            geoNetwork.*
    :param additional_metrics_query: (str, optional, default=None), Строка с частью SQL запроса, в которой
        агрегируются метрики
    :return: query - строка с запросом для BQ
    """

    additional_metrics_query = '' if additional_metrics_query is None else additional_metrics_query

    event_string = ''''''
    if events is not None:
        for event, list_ in events.items():
            event_action = list_[0]
            event_category = list_[1]
            event_string += f"""COUNTIF(hits.eventInfo.eventCategory = '{event_category}' 
            AND hits.eventinfo.eventAction = '{event_action}') AS {event},"""

    cd_query = ''''''
    if custom_dimensions is not None:
        cd_string = ''''''
        for cd, list_ in custom_dimensions.items():
            cd_index = list_[0]
            cd_lvl = list_[1]
            if cd_lvl == 'hits':
                cd_string += f'''MAX((SELECT MAX(value) AS rrr FROM UNNEST(hits.customDimensions) 
                WHERE index={cd_index})) AS {cd},'''

            elif cd_lvl in ('session', 'user'):
                cd_string += f'''MAX((SELECT MAX(value) AS rrr FROM UNNEST(ga.customDimensions) 
                WHERE index={cd_index})) AS {cd},'''

        cd_query = f'''
        ,custom_dim AS(
        SELECT
        
        CONCAT(fullvisitorid, CAST(visitstarttime AS STRING)) AS session_id,
        
        {cd_string}
        
        FROM `{table_name}.ga_sessions_*` ga, UNNEST(hits) AS hits
        WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
        GROUP BY 1
    )'''

    if custom_dimensions is None:
        total_query = '''SELECT * FROM main'''
    else:
        total_query = '''
            SELECT 
            main.*,
            custom_dim.* EXCEPT(session_id)
            FROM main
            LEFT JOIN custom_dim
            ON main.session_id = custom_dim.session_id
            '''

    dim_string = ''''''
    groupby_string = ''''''
    if additional_dimensions is not None:
        for dim in additional_dimensions:
            if bool(re.match(r'visitNumber|trafficSource.|trafficSource.adwordsClickInfo.|device.|geoNetwork.', dim)):
                dim_string += f'{dim}, '
                groupby_string += f', {dim}'

            else:
                raise InvalidDataType(f"""Dimensions must be one of the following: 
                visitNumber, trafficSource.*, trafficSource.adwordsClickInfo.*, device.*, geoNetwork.*, got {dim}""")


    ########### total query ###########
    query_string = f"""
    DECLARE start_date, end_date, experiment_id STRING;
    SET start_date = '{start_date}'; SET end_date = '{end_date}'; SET experiment_id = '{experiment_id}';
    
    WITH main AS(
    SELECT 
    -- Измерения
    TIMESTAMP(PARSE_DATE('%Y%m%d', date)) AS date,
    clientid AS client_id,
    device.deviceCategory AS device,
    geoNetwork.region AS region,
    {dim_string}
    
    -- Метрики
    MAX(experimentVariant) AS experimentVariant,
    IF(totals.newVisits = 1, 'new_visitor', 'returning_visitor') AS visitor_type,
    CONCAT(fullvisitorid, CAST(visitstarttime AS STRING)) AS session_id,
    COUNT(DISTINCT(transaction.transactionId)) AS transactions,
    SUM(transaction.transactionRevenue / 1000000) AS transactionRevenue,
    MAX(hits.time) / 1e3 AS duration,
    COUNTIF(hits.type = 'PAGE') AS pageviews,
    {additional_metrics_query}
    {event_string}
    
    FROM `{table_name}.ga_sessions_*` ga, UNNEST(hits) AS hits, UNNEST(hits.experiment)
    WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
    AND experimentId = experiment_id
    GROUP BY date, client_id, device, visitor_type, session_id, geoNetwork.region{groupby_string}
    )
    
    {cd_query}
    
    {total_query}
    """

    return query_string


def to_bq_type(type):
    """
    Функция для специализации BQ типов
    :param type: (string), data type of column
    :return: object of bigqueru.enum.SqlTypeNames - special BQ datatype
    """

    if type == 'str':
        return bigquery.enums.SqlTypeNames.STRING
    if type == 'float' or type == 'double':
        return bigquery.enums.SqlTypeNames.FLOAT64
    if type == 'int':
        return bigquery.enums.SqlTypeNames.INT64
    if type == 'bool':
        return bigquery.enums.SqlTypeNames.BOOL
    if type == 'bytes':
        return bigquery.enums.SqlTypeNames.BYTES
    if type == 'date':
        return bigquery.enums.SqlTypeNames.DATE
    if type == 'datetime':
        return bigquery.enums.SqlTypeNames.DATETIME
    if type == 'timestamp':
        return bigquery.enums.SqlTypeNames.TIMESTAMP

    # Если ничего не подошло, кидаем ошибку
    raise InvalidDataType


def to_bq(df, path_to_json_creds, project_id, bq_dataset_name, bq_table_name, create_dataset=False, schema=None,
          write_disposition="WRITE_TRUNCATE"):
    """
    Функция для загрузки данных в BQ
    :param df: (pandas.DataFrame), dataframe to write to BQ
    :param path_to_json_creds: (str), path to json file with credentials
    :param project_id: (str), project-id from BQ
    :param bq_dataset_name: (str), dataset name from BQ
    :param bq_table_name: (str), table name from BQ
    :param create_dataset: (bool, optional), whether to create new dataset
    :param schema: (dict, optional), python dict where keys are column names and values are dtypes
    :param write_disposition: (str, optional), type of write disposition in BQ
    https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition

    :return: (str)
    """

    credentials = service_account.Credentials.from_service_account_file(
            path_to_json_creds)

    project_id = project_id
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Создаем датасет
    dataset_ref = client.dataset(bq_dataset_name)
    dataset = bigquery.Dataset(dataset_ref)
    if create_dataset == True:
        dataset = client.create_dataset(dataset)

    table_ref = dataset_ref.table(bq_table_name)

    if schema is not None:
        try:
            partial_schema = []
            for k, v in schema.items():
                partial_schema.append(bigquery.SchemaField(k, to_bq_type(v)))

            job_config = bigquery.LoadJobConfig(
                # Specify a (partial) schema. All columns are always written to the
                # table. The schema is used to assist in data type definitions.
                schema=partial_schema,
                # Optionally, set the write disposition. BigQuery appends loaded rows
                # to an existing table by default, but with WRITE_TRUNCATE write
                # disposition it replaces the table with the loaded data.
                write_disposition=write_disposition
            )

        except AttributeError as e:
            print("Schema must be python dict where keys are column names and values are dtypes")
            raise e
        except InvalidDataType as er:
            print("Passed invalid data type."
                  " Data type should be one of the following: str, float, int, bool, bytes, data, dateime, timestamp")
            raise er
        except:
            raise

    else:
        job_config = None

    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()

    return f"Add {bq_table_name} to {project_id}.{bq_dataset_name} with {write_disposition}"


def get_from_bq(path_to_json_creds, project_id, sql_query):
    """
    Функция для получения данных с указанным запросом
    :param path_to_json_creds: (str), Путь до JSON ключа из GCS
    :param project_id: (str), Имя проекта BQ
    :param sql_query: (str), SQL запрос в стандартном диалекте

    :return: results (pandas.DataFrame), Таблица с результатом запроса
    """
    creds = service_account.Credentials.from_service_account_file(path_to_json_creds)
    client = bigquery.Client(credentials=creds,project=project_id)
    query_job = client.query(sql_query)
    results = query_job.result().to_dataframe()

    return results


