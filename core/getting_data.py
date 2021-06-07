from google.cloud import bigquery # импорт сервисов google cloud
from google.oauth2 import service_account # импорт сервисов по аунтефикации
import pandas as pd


def query(source, table_name, start_date, end_date, experiment_id, events=None, custom_dimensions=None):

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

    query_string = f"""
    DECLARE start_date, end_date, experiment_id STRING;
    SET start_date = '{start_date}'; SET end_date = '{end_date}'; SET experiment_id = '{experiment_id}';
    
    WITH main AS(
    SELECT 
    -- Измерения
    TIMESTAMP(PARSE_DATE('%Y%m%d', date)) AS date,
    clientid AS client_id,
    device.deviceCategory AS device,
    geoNetwork.region AS reggion,
    
    -- Метрики
    MAX(experimentVariant) AS experimentVariant,
    IF(totals.newVisits = 1, 'new_visitor', 'returning_visitor') AS visitor_type,
    CONCAT(fullvisitorid, CAST(visitstarttime AS STRING)) AS session_id,
    COUNT(DISTINCT(transaction.transactionId)) AS transactions,
    SUM(transaction.transactionRevenue / 1000000) AS transactionRevenue,
    MAX(hits.time) / 1e3 AS duration,
    COUNTIF(hits.type = 'PAGE') AS pageviews,
    {event_string}
    
    FROM `{table_name}.ga_sessions_*` ga, UNNEST(hits) AS hits, UNNEST(hits.experiment)
    WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
    AND experimentId = experiment_id
    GROUP BY date, client_id, device, visitor_type, session_id, geoNetwork.region
    )
    
    {cd_query}
    
    {total_query}
    """

    return query_string


def get_from_bq(path_to_json_creds, project_id, sql_query):
    creds = service_account.Credentials.from_service_account_file(path_to_json_creds)
    client = bigquery.Client(credentials=creds,project=project_id)
    query_job = client.query(sql_query)
    results = query_job.result().to_dataframe()

    return results


#df- табличка с данными; fn - метрика для группера; col_name - колонка для метрики; *groups - перечисление столбцов для group by
def grouper(df, *groups, **funs):
    a=[*groups]
    out = {}
    for k, v in funs.items():
        if isinstance(v, list):
            for x in v:
                out[x] = k
        else:
            out[v] = k
    data_total = df.groupby(a, as_index=False).agg(out)
    print(data_total)
