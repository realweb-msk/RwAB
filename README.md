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

## Пайплайн анализа результатов A/B теста
Для быстрой работы с результатами A/B тестов используется класс 
`ab_test.Pipeline`

```python
from ab_test import Pipeline

p = Pipeline(r)
res, tot = p.pipeline('client_id', ['device', 'visitor_type'], {'transactionRevenue': 'sum'}, 'experimentVariant')
res.head()
```
<img width="855" alt="Снимок экрана 2021-06-08 в 10 20 43" src="https://user-images.githubusercontent.com/60659176/121141022-30ade980-c843-11eb-9500-97df7ee921cd.png">

```python
tot.head()
```
<img width="589" alt="Снимок экрана 2021-06-08 в 10 21 30" src="https://user-images.githubusercontent.com/60659176/121141124-4c18f480-c843-11eb-8bb0-25e53cdaed9e.png">