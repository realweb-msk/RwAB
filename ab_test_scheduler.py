import pandas as pd
from ab_test_pipeline import Pipeline
import datetime

def parse_exp_start_from_expId(expId_df, expId_column_name='experimentID', split_char='_', 
        date_position_in_expId=2, date_format_in_expId='%Y%m%d', final_date_format='%Y-%m-%d'):
    """
    Зачем: добавляет столбец с датами начала тестов, распарсенными из expId. 
    Используется в get_current_tests().
    Input:
        :param expId_df: (pandas DataFrame), датафрейм с id экспериментов
        :param expId_column_name: (str), Название столбца датафрейма expId_df с id экспериментов
        :param split_char: (str), символ, по которому нужно разделить строку id эксперимента
        :param date_position_in_expId: (int), позиция даты в expId после разделения строки по split_char
        :param date_format_in_expId: (str), формат, в котором дата записана в expId
        :param final_date_format: (str), формат, в котором дату нужно записать в итоговый столбец exp_start_date
    Output: 
        expId_df с добавленным столбцом exp_start_date с датами начала тестов, распарсенными из expId.
    """
    expId_df['exp_start_date'] = pd.to_datetime(
            expId_df[expId_column_name].str.split(split_char).str[date_position_in_expId], 
            format=date_format_in_expId).dt.strftime( final_date_format)
    return expId_df

def get_current_tests(clickhouse_client, table_name, expId_column_name='experimentID', 
    date_column_name='date', date='yesterday()'):
    """
    Зачем: делает запрос в clickhouse по уникальным id экспериментов за выбранный день, 
    добавляет в полученный датафрейм столбец с датами начала тестов, распарсенными из expId.
    Нужно, чтобы определеять "активные" эксперименты, т.е. эксперименты, данные о которых
    есть за вчерашний день.
    Используется в ab_test_scheduler().
    Input:
        :param clickhouse_client: (object), авторизованный клиент clickhouse из библиотеки clickhouse_driver
        :param table_name: (str), название таблицы в клике, к которой будет делаться запрос,
            включая датасет, например, 'rw.WEB_ab_test_in'
        :param expId_column_name: (str), Название столбца запрашиваемой таблицы table_name с id экспериментов
        :param date_column_name: (str), Название столбца запрашиваемой таблицы table_name с датой
        :param date: (str), дата, за которую нужно получить уникальные expId
            принимает дату в формате 'YYYY-MM-DD' или кликовские функции 'yesterday()' и т.п.
    Output: 
        pandas DataFrame со столбцами {expId_column_name} и 'exp_start_date'.
    """
    try:
        datetime.datetime.strptime(date, "%Y-%m-%d")
        current_tests = clickhouse_client.query_dataframe(f'''
        SELECT
            DISTINCT {expId_column_name}
        FROM {table_name}
        WHERE {date_column_name}='{date}'
        ''')
        return parse_exp_start_from_expId(current_tests)
    except ValueError:
        current_tests = clickhouse_client.query_dataframe(f'''
        SELECT
            DISTINCT {expId_column_name}
        FROM {table_name}
        WHERE {date_column_name}={date}
        ''')
        return parse_exp_start_from_expId(current_tests)

def query_from_ab_test_stats_clickhouse(clickhouse_client, table_name, expId, exp_start_date,
    expId_column_name='experimentID', date_column_name='date', date='yesterday()'):
    """
    Зачем: делает запрос в clickhouse к таблице по статистике тестов для заданного теста
    за период от указанного начала теста до выбранного дня (по умолчанию вчера).
    Используется в ab_test_scheduler().
    Input:
        :param clickhouse_client: (object), авторизованный клиент clickhouse из библиотеки clickhouse_driver
        :param table_name: (str), название таблицы в клике, к которой будет делаться запрос,
            включая датасет, например, 'rw.WEB_ab_test_in'
        :param expId: (str), id эксперимента, для которого нужно запросить статистику
        :param exp_start_date: (str, 'YYYY-MM-DD'), дата начала теста
        :param expId_column_name: (str), Название столбца запрашиваемой таблицы table_name с id экспериментов
        :param date_column_name: (str), Название столбца запрашиваемой таблицы table_name с датой
        :param date: (str), дата, до которой нужно получить статистику.
            Задумано так, что в текущей день, нужно выбирать значение "вчера" и выгружать статистику
            от начала теста до вчера.
            Принимает дату в формате 'YYYY-MM-DD' или кликовские функции 'yesterday()' и т.п.
    Output: 
        pandas DataFrame с таблицей table_name, отфильтрованной по expId и дате
    """
    try:
        datetime.datetime.strptime(date, "%Y-%m-%d")
        return clickhouse_client.query_dataframe(f'''
        SELECT 
            *
        FROM {table_name}
        WHERE {date_column_name}>='{exp_start_date}' AND {date_column_name}<='{date}'
            AND {expId_column_name}='{expId}'
        ''')
    except ValueError:    
        return clickhouse_client.query_dataframe(f'''
        SELECT 
            *
        FROM {table_name}
        WHERE {date_column_name}>='{exp_start_date}' AND {date_column_name}<={date}
            AND {expId_column_name}='{expId}'
        ''')

def ab_test_scheduler(clickhouse_client, stats_table_name, results_table_name, date='yesterday()',
    groups=['device', 'visitor_type', 'channelGroupB'],
    conversions_column_name='goal_visits', sessions_column_name='visits',
    expId_column_name='experimentID', expVar_column_name='experimentVariant', 
    date_column_name='date'):
    """
    Зачем: для расчета результатов всех активных тестов за выбранный день (по умолчанию вчера).
    Алгоритм работы:
    1. Запрашивает активные тесты с помощью get_current_tests()
    2. Для каждого expId в списке активных тестов:
        2.1. С помощью query_from_ab_test_stats_clickhouse() запрашивает статистику по тесту
        2.2. Расчитывает результаты тестов с помощью ab_test_pipeline.Pipeline
        2.3. Добавляет в таблицу результатов столбцы, общие для всего теста:
            ['experimentID', 'ab_product', 'ab_name', 'ab_start_date', 'ab_goal_id', 'date']
        2.4. Записывает таблицу результатов в clickhouse
    Input:
        :param clickhouse_client: (object), авторизованный клиент clickhouse из библиотеки clickhouse_driver
        :param stats_table_name: (str), название таблицы в клике со статистикой теста, к которой будет делаться запрос,
            включая датасет, например, 'rw.WEB_ab_test_in'
        :param results_table_name: (str), название таблицы в клике с результатами тестов,
            куда будут записываться результаты,
            включая датасет, например, 'rw.WEB_ab_test_in'
        :param date: (str), дата, до которой нужно получить статистику.
            Задумано так, что в текущей день, нужно выбирать значение "вчера" и выгружать статистику
            от начала теста до вчера.
            Принимает дату в формате 'YYYY-MM-DD' или кликовские функции 'yesterday()' и т.п.
        :param groups: (list(str)), список столбцов stats_table_name с разрезами, например, 
            с типом устройств, типом пользователей и т.д.
        :param conversions_column_name: (str), Название столбца запрашиваемой таблицы 
            stats_table_name с числом конверсий
        :param sessions_column_name: (str), Название столбца запрашиваемой таблицы 
            stats_table_name с числом сеансов
        :param expId_column_name: (str), Название столбца запрашиваемой таблицы 
            stats_table_name с id экспериментов
        :param expVar_column_name: (str), Название столбца запрашиваемой таблицы 
            stats_table_name с вариантами экспериментов
        :param date_column_name: (str), Название столбца запрашиваемой таблицы stats_table_name с датой
        
    Output: нет вывода, результат - записанные в таблицу в clickhouse результаты тестов
    """
    current_tests = get_current_tests(clickhouse_client, stats_table_name, expId_column_name, date=date)

    for i in range(len(current_tests)):
        test_stats = query_from_ab_test_stats_clickhouse(clickhouse_client, stats_table_name, 
            expId=current_tests['experimentID'][i], exp_start_date=current_tests['exp_start_date'][i],
            expId_column_name=expId_column_name, date_column_name=date_column_name, date=date)
        
        p = Pipeline(test_stats)
        res = p.pipeline(date_column_name, experiment_var_col=expVar_column_name, 
                        groups = groups,
                        metrics_for_binary={conversions_column_name: sessions_column_name})[0]
        
        res[['experimentID', 'ab_product', 'ab_name', 
            'ab_start_date', 'ab_goal_id', 'date']] = test_stats.loc[0, 
                    ['experimentID', 'ab_product', 'ab_name', 'ab_start_date', 'ab_goal_id', 'date']]
        res.drop('metric', axis='columns', inplace=True)
        
        clickhouse_client.execute(f'INSERT INTO {results_table_name} VALUES', res.values.tolist())
