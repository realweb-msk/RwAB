from clickhouse_driver import Client
import ab_test_scheduler

host='rc1b-9sj53dbwyy94t0mx.mdb.yandexcloud.net'
port='8443'
db='rw'
user='rw_user'
password='RWdata256'
ca_cert_path=r"C:\Users\user1676\Работа\Clickhouse SSL key\CA.pem"

ch_client = Client(host, user=user, password=password, database=db, secure=True, ca_certs=ca_cert_path)

# Обработка всех активных тестов за выбраннуб дату:
ab_test_scheduler.ab_test_scheduler(ch_client, # подключение к клику
                'rw.WEB_abtests_in', # таблица, откуда берется статистика по тестам
                'rw.WEB_ab_tests_results', # таблица, куда будут записаны результаты тестов
                groups=['device', 'visitor_type', 'channelGroupB'], # столбцы с разрезами
                date='2022-09-04') # дата, принимает дату в формате 'YYYY-MM-DD' или кликовские функции 'yesterday()' и т.п.