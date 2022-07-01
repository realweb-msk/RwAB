# RwAB
Realweb tools for AB-testing

В модуле реализован полный пайплайн для работы с результатами A/B теста. На 
основе данных стриминга Google Analytics в Google BigQuery или выгрузки, полученной
при помощи API Google Analytics.

## Модуль получения и обработки данных
### get_data.py
```python
from core.get_data import query, get_from_bq

path = '../my_creds.json' # path to json creds 
table = 'project_id.dataset_name'
start = '20201229' # Start date
end = '20201230' # End date
exp = 'qwertyEQbOY6YCfNmOvvg' # experiment_id
events = {'kek': ['foo', 'bar']} # event dict
cds = {'pep': [15, 'hits']} # custom dimensions dict
query = query(table, start, end, exp, events=events, custom_dimensions=cds)

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

### ga.py
Модуль, в котором реализованно получение данных при помощи [API Google Analytics](https://developers.google.com/analytics/devguides/reporting/core/v3/reference?hl=en)
```python
from core.ga import GetGAData


path_to_creds = "../my_creds.json"
profile_id = "12345678" # ID представления GA
start_date, end_date = "2021-01-01", "2021-04-04" # Период сбора статистики
# Строка с метриками в нужном формате, подробнее читайте в описании класса
metrics = "ga:sessions, ga:users"
# Строка с параметрами (разрезами) данных, подробнее читайте в описании класса
# UPD: с недавнего времени появилось ограничение на максимальное кол-во параметров = 5 
dimensions = "ga:date, ga:userType"

ga_getter = GetGAData(path_to_creds, profile_id, start_date, end_date,
                      metrics, dimensions)
# Доступна поддержка фильтров, подробнее в описании метода
df_with_ga_data = ga_getter.get_ga_data()
```

## Пайплайн анализа результатов A/B теста
Для быстрой работы с результатами A/B тестов используется класс `ab_test.Pipeline`.

Класс ожидает таблицу со статистикой эксперимента. Пример такой таблицы приведен в 
`data/res.csv`. Для работы необходимо задать название столбца, по которому 
будет идти группировка данных, это может быть, к примеру `clientID`, если за единицу
измерения в эксперименте считается один пользователь, либо же по полю даты, тогда 
одним наблюдением в выборке будет считаться агрегированная статистика за день.

Для корректной агрегации данных, необходимо, чтобы в подаваемых данных была
хотя бы одна метрика, ее имя и тип группировки передаются в параметр `metric_aggregations`.
Более подробную документацию читайте в описании класса `ab_test.Pipeline`

Во время работы пайплайна происходит анализ и сравнение 
статистических характеристик выборок (мат. ожидание, дисперсия, медиана, мода,
скошенность), а также анализ и сравнение распределений. В зависимости от полученных 
результатов происходит выбор статистического критерия оценки уровня стат. значимости
для нулевой гипотезы H = "Между выборками по есть статистически значимая 
разница по определенному показателю".

Поддерживается возможность анализа определенных подгрупп выборки и их комбинаций, 
т.к. помимо общей выборки достаточно часто интересно посмотреть на изолированные 
результаты теста (например, только разница для пользователей заходивших с мобильных
устройств)

```python
from ab_test_pipeline import Pipeline

p = Pipeline(r)
ab_test_results, summary = p.pipeline("date", {"cr": 'mean'}, 'experimentVariant', 
                  groups = ['deviceCategory', 'userType'])
ab_test_results
```
Снимок экрана 2021-11-08 в 12.12.49<img width="817" alt="изображение" src="https://user-images.githubusercontent.com/60659176/140715576-3df16a9e-a624-4e7b-8629-0c3a0dc74fb8.png">

Также поддерживается аналитика показателей A/B теста как бинарных метрик 
(отношение успехи / попытки) при помощи стандартных статистических методов, а также Байесовой статистики.
```python
p = Pipeline(data)
continuous_res, summary, binary_res = (
    p.pipeline('date', {'sessions': 'sum', 'addToCart': 'sum'}, "experimentVariant", 
               groups = ['deviceCategory', 'userType'], 
               metrics_for_binary={"addToCart": "sessions"})
)

binary_res
```
<img width="657" alt="изображение" src="https://user-images.githubusercontent.com/60659176/140715629-8cd43534-fe1f-401f-a333-ff11c6e501f9.png">


[comment]: <> (Больше примеров можно найти в [этом ноутбуке]&#40;https://colab.research.google.com/drive/1wFDoR-4F3lxXb8bO3SOcXlUO3w1yGvWM?usp=sharing&#41;)
