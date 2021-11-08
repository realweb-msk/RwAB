import argparse
import googleapiclient.discovery as discovery
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import time
import re
import pandas as pd
import urllib.parse


class GetGAData:
    """
    Класс для сбора данных по API Google Analytics

    :param client_secrets_path: (str), Путь до json файла, который выдается на странице GA API в GoogleCloud
    :param profile_id: (str), Номер представления ga
    :param start_date: (str), Начальная дата периода. Передается в формате YYYYMMDD
    :param end_date: (str), Конечная дата периода. Передается в формате YYYYMMDD
    :param metrics: (list), Список с показателями, которые вы хотите получить. Корректный нейминг брать отсюда
    https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/
    :param dimensions: (list), Список с параметрами, которые вы хотите получить. Корректный нейминг брать отсюда
    https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/ Максимум можно указать 7 параметров
    :param start_index: (int, optional, default=1), С какой строки собирать результаты
    """

    def __init__(self, client_secrets_path, profile_id, start_date, end_date, metrics, dimensions, start_index=1):
        self.client_secrets_path = client_secrets_path
        self.profile_id = profile_id
        self.start_date = start_date
        self.end_date = end_date
        self.metrics = metrics
        self.dimensions = dimensions
        self.start_index = start_index

    @staticmethod
    def get_service(client_secrets_path, api_name='analytics', api_version='v3',
                    scope='https://www.googleapis.com/auth/analytics.readonly'):

        """
        Метод создает коннект с API
        :param client_secrets_path: См. описание класса
        :param api_name:(str, optional, default='analytics') Название применяемого API, сейчас реализовано для analytics
        :param api_version:(str, optional, default='v3') Версия используемого API
        :param scope: (str, optional, default='https://www.googleapis.com/auth/analytics.readonly'), С каким уровнем
        доступа подключается API. По умолчанию - только на чтение
        :return: service object
        """
        # Parse command-line arguments.
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            parents=[tools.argparser])
        flags = parser.parse_args([])

        # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(
            client_secrets_path, scope=scope,
            message=tools.message_if_missing(client_secrets_path))

        # Prepare credentials, and authorize HTTP object with them.
        # If the credentials don't exist or are invalid run through the native client
        # flow. The Storage object will ensure that if successful the good
        # credentials will get written back to a file.
        storage = file.Storage(re.match(r'.+/(.+).json$|^(.+).json$', client_secrets_path)[1] + '_.dat')
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)
        http = credentials.authorize(http=httplib2.Http())

        # Build the service object.
        service = discovery.build(api_name, api_version, http=http)

        return service

    def get_full_results(self, service, profile_id, start_date_, end_date_, metrics_, dimensions_, start_index_,
                         filters_string=None):
        """
        Метод получает ответ от API и складывает результаты в список
        :param service: (GetGAData.get_service object)
        :param profile_id: См. описание класса
        :param start_date_: См. описание класса
        :param end_date_: См. описание класса
        :param metrics_: См. описание класса
        :param dimensions_: См. описание класса
        :param start_index_: См. описание класса
        :param filters_string: Строка фильтров запроса, результат работы метода GetGAData.build_filters_string

        :return: (list) Список с данными, полученными от API
        """
        response = service.data().ga().get(
              ids=profile_id,
              start_date=start_date_,
              end_date=end_date_,
              metrics=metrics_,
              dimensions=dimensions_,
              start_index=start_index_,
              filters=filters_string,
              max_results=10000
        ).execute()

        print('got response')
        start_index = response['query']['start-index']
        total_results = response['totalResults']
        max_results = response['query']['max-results']
        if start_index == 1:
            full_rows = []
            print('total_results: '+str(total_results))
        headers_ = response['columnHeaders']
        headers_arr = []
        for head_ in headers_:
            headers_arr.append(head_.get('name'))
        rows_ = response.get('rows', [])
        if not full_rows:
            full_rows.insert(0, headers_arr)
        full_rows += rows_

        if start_index < total_results:
            start_index_ += max_results
            time.sleep(1)
            print(start_index)
            self.get_full_results(service, profile_id, start_date_, end_date_, metrics_, dimensions_, start_index_)
        else:
            print('thats all')
            return full_rows

    @staticmethod
    def list_response_to_df(lists_):
        """
        Метод переводит список с данными API в pandas.DataFrame
        :param lists_: (list), Список с данными от API
        :return: pandas.DataFrame
        """
        df_ = pd.DataFrame.from_records(lists_[1:])
        df_.columns = list(map(lambda x: re.match(r'ga:(.+)', str(x))[1], lists_[0]))
        if 'date' in df_.columns and len(df_['date']) > 0:
           df_['date'] = df_['date'].apply(lambda x:
                             re.match(r'(\d{4})\d{2}\d{2}', str(x))[1]+'-'+
                             re.match(r'\d{4}(\d{2})\d{2}', str(x))[1]+'-'+
                             re.match(r'\d{4}\d{2}(\d{2})', str(x))[1]
                            )

        print(df_.columns)
        return df_

    @staticmethod
    def build_filters_string(dimensions, operators, values, unions=None):
        """
        Компанует допустимую строку-фильр для API GA, полезные ссылки:
        https://developers.google.com/analytics/devguides/reporting/core/v3/reference?hl=ru#filters

        :param dimensions: См. описание метода ga_get_data
        :param operators: См. описание метода ga_get_data
        :param values: См. описание метода ga_get_data
        :param unions: См. описание метода ga_get_data
        :return: (str)
        """
        operator_to_url = {
            '==': "%3D%3D",
            "!=": "!%3D",
            ">": "%3E",
            "<": "%3C",
            ">=": "%3E%3D",
            "<=": "%3C%3D",
            "=@": "%3D@",
            "!@": "!@",
            "=~": "%3D~",
            "!~": "!~"
        }

        filter_string = ""
        cnt = 0
        for dim, op, val in zip(dimensions, operators, values):
            if op not in operator_to_url:
                raise ValueError("Operator must be one of:", list(operator_to_url.keys()))
            cur_str = f"{dim}{op}{urllib.parse.quote(val)}"

            filter_string += cur_str

            if unions:
                try:
                    filter_string += unions[cnt]
                    cnt += 1
                except Exception:
                    continue

        return filter_string

    def get_ga_data(self, filters=None):
        """
        Совершает запрос к GA и загружает его результат в pandas.DataFrame
        :param filters: (optional, dict default=None), словарь со следующей схемой:
        filters{'dimensions': list, 'operators': list, 'values': list, 'unions'(optional): None or list}
        - dimensions - упорядоченный список с показателями, по которым будет идти фильтрация
        доступные параметры можно найти здесь: https://ga-dev-tools.web.app/dimensions-metrics-explorer/
        - operators - упорядоченный список операторов, при помощи которых будет происходить фильтрация,
        допустимые операторы:
            == - равно (для чисел) или в точности совпадает (для строк)
            != - не равно (для чисел) или не совпадает (для строк)
            > - больше (только для чисел)
            < - меньше (только для чисел)
            >= - больше или равно (только для чисел)
            <= - меньше или равно (только для чисел)
            =@ - содержит подстроку
            !@ - не содержит подстроку
            =~ - подчиняется регулярному выражению
            !~ - не подчиняется регулярному выражению
        - values - упорядоченный список со значениями, по которым будет идти фильтрация
        - unions (optional) - должно быть не None, если в строке фильтрации есть более одного условия. Тогда в unions
        находятся один из двух допустимых операторов:
        - ";" - AND
        - "," - OR
        Где OR имеет приоритет над AND, подробнее:
        https://developers.google.com/analytics/devguides/reporting/core/v3/reference?hl=ru#filters

        Итоговая строка фильтра кодируется в URL и строится следующим образом:
        filters[dimension][0] + filters[operators][0] + filters[values][0] + filters[unions][0]
        + filters[dimensions][1] + ...

        :return: pd.DataFrame
        """
        service = self.get_service(self.client_secrets_path)

        if filters is not None:
            filter_string = self.build_filters_string(filters['dimensions'], filters['operators'],
                                                      filters['values'], unions=filters['unions'])
        else:
            filter_string = ""
        full_results = self.get_full_results(service, self.profile_id, self.start_date, self.end_date,
                                             self.metrics, self.dimensions, self.start_index, filter_string)
        df = self.list_response_to_df(full_results)

        return df
