# RwAB
Realweb tools for AB-testing

В модуле реализован полный пайплайн для работы с результатами A/B теста. На 
основе данных стриминга Google Analytics в BQ.

## Модуль получения и обработки данных
```python
from core.get_data import query, get_from_bq

path = '../my_creds.json' # path to json creds 
table = 'project_id.dataset_name'
start = '20201229' # Start date
end = '20201230' # End date
exp = 'qwertyEQbOY6YCfNmOvvg' # experiment_id
events = {'kek': ['foo', 'bar']} # event dict
cds = {'pep': [15, 'hits']} # custom dimensions dict
query = query(1, table, start, end, exp, events=events, custom_dimensions=cds)

r = get_from_bq(path, 'project_id', query)
r.head()
```
Метод `query` делает запрос, который по-умолчанию возвращает таблицу следующей 
структуры:
- **date** (timestamp) - дата сеанса
- **client_id** (str) - GA clientID пользователя
- **device** (str) - Девайс, с которого был совершен сеанс
- **region** (str) - Регион пользователя
- **experimentVariant** (str) - Вариант эксперимента
- **visitor_type** (str) - Тип пользователя (новый или вернувшийся)
- **session_id** (str) - sessionID
- **transactions** (int) - Количество транзакций в сеансе
- **transactionRevenue** (float) - Суммарная выручка транзакций в сеансе
- **duration** (float) - Длительность сеанса в секундах
- **pageviews** (int) - Кол-во просмотров страниц в сеансе

Также при помощи параметра `events` возможно указать свои события,
для которых будет посчитано их кол-во за сеанс. Подробнее про формат см. 
документацию метода.

Аналогично при помощи параметра `custom_dimensions` возможно добавить статистику
по пользовательским переменным.