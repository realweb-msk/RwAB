from itertools import combinations
import pandas as pd
from core.stat_test import *
import numpy as np
from core.bayes import BayesTest


class Pipeline:
    def __init__(self, df, verbose=0):
        """
        Класс для анализа результатов АБ-теста
        :param df: (pandas.DataFrame), Датафрейм с данными по A/B тесту. По дефолту ожидаются данные, полученные при
        помощи core.get_data.query, но возможно работать со своими данными похожей конфигурации
        :param verbose:
        """
        self.df = df

    @staticmethod
    def compute_combinations(values_, max_len):
        """
        Зачем:
        Input:
            values - [] значения для комбинирования
            max_len - максимальная длина () комбинаций
        Output: 
            _res - [] с ()-ами всех комбинаций, где включены комбинации длины от 1 до max_len (зачем???)
        """
        _res = []
        _res.append(tuple())
        for i in range(1, max_len + 1):
            _res.extend([g for g in combinations(values_, i)])
        return _res

    @staticmethod
    def get_unique_values(df, columns):
        """
        Зачем:
        Input: 
            df, 
            columns
        Output: 
            _unique_vals - уникальные значения из всех указанных columns в одном []
            _dict - словарь формата {уникальное значение: название столбца (из columns)}
        """
        _unique_vals = []
        _dict = {}
        for col in columns:
            uniques = df[col].unique()
            for val in uniques:
                _unique_vals.append(val)
                _dict[val] = col

        return _unique_vals, _dict

    def grouper(self, groupby_col, experiment_variant_col, group_comb, metric_aggregations):
        """
        Зачем:
        Input:
            :param groupby_col: (str), Название столбца по которму будет идти группировка данных, например по дате
            :param experiment_var_col: (str), Название столбца с параметром варианта эксперимента
            group_comb - идет из комбинаций по :param groups: (iterable, optional, default=None), 
                Список с названиями столбцов, по которым будет идти срез
            :param metric_aggregations: (dict), Словарь следующего формата: {'metric_name': ['agg_func']},
                например: {'transactions': 'sum'}
        Output: 
            comb - генератор [] комбинаций уникальных значений group_comb в df
            temp - генератор [] датафреймов - сгруппированных df по комбинациям значений в comb
        """
        _unique_vals, _group_val_dict = self.get_unique_values(self.df, group_comb)
        val_combinations = self.compute_combinations(_unique_vals, len(group_comb))

        for comb in val_combinations:
            query_string = ""
            for i, val in enumerate(comb):
                if i != 0:
                    query_string += " and "
                query_string += f"{_group_val_dict[val]} == '{val}'"

            if query_string == "":
                temp = self.df
            else:
                temp = self.df.query(query_string)
            temp = temp.groupby([groupby_col, experiment_variant_col], as_index=False).agg(metric_aggregations)

            yield comb, temp, _group_val_dict

    @staticmethod
    def compute_results_binary(grouped_data, experiment_var_col, metrics, alpha=0.05):
        """
        Функция для расчета резульатов A/B теста как бинарных показателей через вычисление CR для групп
        :param grouped_data: (pandas.DataFrame), Сгруппированный датафрейм
        :param experiment_var_col: (str), Имя столбца с вариантом эксперимента
        :param metrics: (dict), Словарь с названиями столбцов датафрейма, для расчета CR следующей структуры:
            {"success_col_1": "tries_col_1", "success_col_2": "tries_col_2", ...}
            где success и tries столбцы по которым будет считаться отношение успехи/попытки
        :param alpha: (float, optional, default=0.05), alpha-value для статистических тестов
        :return:
        """

        variants = grouped_data[experiment_var_col].unique()
        dfs_vars = {}

        for var in variants:
            dfs_vars[f'df_{var}'] = grouped_data.query(f"{experiment_var_col} == @var")

        res = pd.DataFrame(columns=['cnt', 'first', 'second', 'metric'])
        res = res.set_index(['cnt', 'first', 'second', 'metric'])

        keys_comb = combinations(dfs_vars, 2)
        vals_comb = combinations(dfs_vars.values(), 2)

        for names, values in zip(keys_comb, vals_comb):
            df_1 = values[0].copy()
            df_2 = values[1].copy()

            for succ, trial in metrics.items():
                cur_metric = f"{succ}_{trial}_CR"

                df_1[cur_metric] = df_1[succ] / df_1[trial]
                df_2[cur_metric] = df_2[succ] / df_2[trial]

                # Дополнительный индекс, для того, чтобы разные тесты записывались в разные строки DF
                _cnt = 0
                col_name = tuple([_cnt]) + names + tuple([cur_metric])
                res.loc[col_name, 'mean_lift'] = lift(df_1[cur_metric], df_2[cur_metric])
                res=res.sort_index()

                # Z-test
                res.loc[col_name, "test_type"] = "z-test"
                res.loc[col_name, "p_value"] = z_test_ratio(df_2[succ].sum(), df_1[succ].sum(),
                                                            df_2[trial].sum(), df_1[trial].sum())

                _cnt += 1
                col_name = tuple([_cnt]) + names + tuple([cur_metric])

                # Bayes A/B statistics
                bayes_res = BayesTest(df_1[succ].sum(), df_2[succ].sum(),
                                                            df_1[trial].sum(), df_2[trial].sum())
                bayes_prob, bayes_lift = bayes_res.bayes_prob()
                res.loc[col_name, "test_type"] = "bayes_test"
                res.loc[col_name, "mean_lift"] = bayes_lift
                res.loc[col_name, "p_value"] = 1 - bayes_prob

        return res

    @staticmethod
    def compute_results_continuous(grouped_data, experiment_var_col, metrics, alpha=0.05,
                                   show_total=True, show_plots=False):
        """
        Функция для расчета попарных результатов A/B теста для групп

        :param grouped_data: (pandas.DataFrame), Сгруппированный датафрейм
        :param experiment_var_col: (str), Имя столбца с вариантом эксперимента
        :param metrics: (iterable), Названия столбцов с метриками, которые будут сравниваться
        :param alpha: (float, optional, default=0.05), alpha-value для статистических тестов
        :param show_total: (bool, optional, default=True), Выводить ли датафрейм с общей статистикой
        :param show_plots: (bool, optional, default=False), Выводить ли QQ plot для нормального распределения

        :return: При show_total=True, возвращает tuple из двух датафреймов: res и tot, res - с результатами
        эксперимента, при show_total=False возвращает только res
        """
        variants = grouped_data[experiment_var_col].unique()
        dfs_vars = {}

        for i, var in enumerate(variants):
            dfs_vars[f'df_{var}'] = grouped_data.query(f"{experiment_var_col} == @var")

        res = pd.DataFrame(columns=['first', 'second', 'metric'])
        res = res.set_index(['first', 'second', 'metric'])

        keys_comb = combinations(dfs_vars, 2)
        vals_comb = combinations(dfs_vars.values(), 2)

        for df in dfs_vars.values():
            for metric in metrics:
                if show_plots:
                    qq_plot(df[metric], metric)
                df[metric+'_normal'] = normality_test(df[metric])

        for names, values in zip(keys_comb, vals_comb):
            df_1 = values[0]
            df_2 = values[1]

            for metric in metrics:
                col_name = names + tuple([metric])
                res.loc[col_name, 'mean_lift'] = lift(df_1[metric], df_2[metric])
                # Если оба распределения нормальные
                if df_1[metric+'_normal'].min() and df_2[metric+'_normal'].min():
                    equal_var = levene_var(df_1[metric], df_2[metric], alpha=alpha)
                    equal_median = kruskal_wallis(df_1[metric], df_2[metric])
                    res.loc[col_name, 'equal_variance'] = equal_var
                    res.loc[col_name, 'equal_median'] = equal_median

                    # Если дисперсии равны
                    if equal_var:
                        res.loc[col_name, "test_type"] = "independent ttest"
                        res.loc[col_name, "p_value"] = independent_ttest(df_1[metric], df_2[metric], alpha=alpha)

                    else:
                        res.loc[col_name, "test_type"] = "mann_whitneyu"
                        res.loc[col_name, 'p_value'] = mann_whitneyu_test(df_1[metric], df_2[metric], alpha=alpha)

                else:
                    equal_var = mood_var(df_1[metric], df_2[metric], alpha=alpha)
                    res.loc[col_name, 'equal_variance'] = equal_var
                    equal_median = kruskal_wallis(df_1[metric], df_2[metric])
                    res.loc[col_name, 'equal_median'] = equal_median

                    res.loc[col_name, "test_type"] = "mann_whitneyu"
                    res.loc[col_name, 'p_value'] = mann_whitneyu_test(df_1[metric], df_2[metric], alpha=alpha)

        if show_total:
            total_sum = grouped_data.groupby(experiment_var_col, as_index=False)[metrics].sum()\
                .rename(columns={metric: metric+'_sum' for metric in metrics})
            total_mean = grouped_data.groupby(experiment_var_col, as_index=False)[metrics].mean()\
                .rename(columns={metric: metric+'_mean' for metric in metrics})
            total = total_sum.merge(total_mean, left_on=experiment_var_col, right_on=experiment_var_col)
            return res, total

        return res

    def pipeline(self, groupby_col,  experiment_var_col, groups=None, show_total=True,
                 experiment_id=None, metric_aggregations=None, metrics_for_binary=None):
        """
        Метод с пайплайном анализа результатов всего A/B теста. Выполняет предобработку и группировку данных.
        Возможно посмотреть результаты A/B теста в определенных разрезах (например отдельно по новым пользователям)
        :param groupby_col: (str), Название столбца по которму будет идти группировка данных, например по дате
        Кажется, что этот параметр не должен быть
        :param metric_aggregations: (dict), Словарь следующего формата: {'metric_name': ['agg_func']},
            например: {'transactions': 'sum'}
        :param experiment_var_col: (str), Название столбца с параметром варианта эксперимента
        :param groups: (iterable, optional, default=None), Список с названиями столбцов, по которым будет идти срез
        :param show_total: (bool, optional, default=True), См. описание метода compute_results
        :param experiment_id: (str, optional, default=None), Возможность добавить в итоговые результаты столбец с
        ID эксперимента
        :param metrics_for_binary: (dict, optional, default=None), Если определен, в дополнение результаты будут
        рассчитаны в бинарной парадигме успехи/попытки
            Словарь с названиями столбцов датафрейма, для расчета CR следующей структуры:
            {"success_col_1": "tries_col_1", "success_col_2": "tries_col_2", ...}
            где success и tries столбцы по которым будет считаться отношение успехи/попытки

        :return: При groups = None возвращаются общие результаты для групп.
        При show_total = True к результатам будет добавлен датафрейм с общими показателями теста
        При непустом metrics_for_binary к результатам будет добавлен датафрейм с анализом показателей как CR
        """

        totals = None
        results = None
        bin_results = None
        res = []

        # TO DO: убрать эту дичь
        # костыль, чтобы пока не менять функцию grouper
        # т.к. когда непрерывные показатели задавать необязательно, в нее нечего передать
        # в качестве агрегаций (4 аргумент)
        # Сначала переводим словарь с бинарными переменными в лист
        metrics_for_binary_flatten=[] 
        for key, value in metrics_for_binary.items():
            metrics_for_binary_flatten.append(key)
            metrics_for_binary_flatten.append(value)
        # В полученном листе находим уникальные значения
        metrics_for_binary_flatten = list(set(metrics_for_binary_flatten))

        binary_aggregations = {}
        # Задаем всем метрикам для бинарных величин агрегацию суммой
        for x in metrics_for_binary_flatten:
            binary_aggregations.update({x: 'sum'})
        
        if (metric_aggregations is None) & (metrics_for_binary is not None):
            all_aggregations = binary_aggregations
        elif (metric_aggregations is not None) & (metrics_for_binary is not None):
            #all_aggregations = dict(**metric_aggregations, **binary_aggregations)
            all_aggregations = binary_aggregations.copy()
            for k,v in metric_aggregations.items():
                all_aggregations.update({k:v})
        else: 
            all_aggregations = metric_aggregations

        

        # если нет группировок:
        if groups is None:
            if metric_aggregations is not None:
                _df_gr = self.df.groupby([groupby_col, experiment_var_col], as_index=False).agg(metric_aggregations)
                _res, total = self.compute_results_continuous(_df_gr, experiment_var_col, list(metric_aggregations.keys()))

                if experiment_id is not None:
                    _res['experiment_id'] = experiment_id
                    total['experiment_id'] = experiment_id

                # Приводим нейминг таблиц с groups и без groups к единому виду
                _res['group'] = "No group"
                _res = _res.reset_index()
                total['group'] = "No group"
                total = total.reset_index()

                res.extend([_res, total])

            if metrics_for_binary is not None:
                _df_gr = self.df.groupby([groupby_col, experiment_var_col], as_index=False).sum()
                bin_res = self.compute_results_binary(_df_gr, experiment_var_col, metrics_for_binary)
                if experiment_id is not None:
                    bin_res['experiment_id'] = experiment_id

                res.append(bin_res)

            return res

        # Кол-во срезов в комбинации не может быть больше чем кол-во групп
        max_comb_len = len(groups)

        # Считаем разультаты для каждой возможной комбинации срезов по длинам от 1 до len(groups)
        # Это сделано для того, чтобы итоговые результаты анализа тестов было возможно посмотреть
        # Во всех возможных комбинациях разрезов
        for group_comb in self.compute_combinations(groups, max_len=max_comb_len):
            for val_comb, df_gr, group_val_dict in self.grouper(groupby_col, experiment_var_col, group_comb, all_aggregations):
                # Continuous metrics
                if metric_aggregations is not None:
                    _res, total = self.compute_results_continuous(df_gr, experiment_var_col, list(metric_aggregations.keys()))
                    _res = _res.reset_index()

                # Binary metrics
                if metrics_for_binary is not None:
                    bin_res = self.compute_results_binary(df_gr, experiment_var_col, metrics_for_binary)
                    bin_res = bin_res.reset_index().drop("cnt", axis=1)

                # Если никаких результатов пока не считали,
                # то создаем датафрейм, куда будем затем их складывать
                if results is None:
                    # Зададим в индексы максимально возможное кол-во срезов в одной группе
                    _indx = ['cnt'] + groups
                    #_indx = ['cnt'] + [f'group_{i}' for i in range(max_comb_len)]
                    if metric_aggregations is not None:
                        results = pd.DataFrame(columns=np.append(_indx, _res.columns))
                        results = results.set_index(_indx)
                        totals = pd.DataFrame(columns=np.append(_indx, total.columns))
                        totals = totals.set_index(_indx)
                if (bin_results is None) & (metrics_for_binary is not None):
                        bin_results = pd.DataFrame(columns=np.append(_indx, bin_res.columns))
                        bin_results = bin_results.set_index(_indx)

                # Заполняем датафрейм результатов для непрерывных показателей результатами
                if metric_aggregations is not None:
                    for i, _r in enumerate(_res.values):
                        try:
                            # Дополним пустые индексы групп как "No group"
                            # Таких max_comb_len - len(val_com)
                            _new_index = [str(i)] + ["No group"] * len(groups)
                            for x in val_comb:
                                _new_index[groups.index(group_val_dict[x])+1] = x
                            _new_index = tuple(_new_index)
                            #_new_index = tuple([str(i)] + [name for name in val_comb] +
                            #                ["No group"] * (max_comb_len - len(val_comb)))
                            results.loc[_new_index, :] = _r
                        except:
                            print(results, val_comb, _r)
                            raise
                
                    # Заполняем датафрейм результатов для непрерывных показателей результатами
                    for i, _t in enumerate(total.values):
                        # Дополним пустые индексы групп как "No group"
                        _new_index = [str(i)] + ["No group"] * len(groups)
                        for x in val_comb:
                            _new_index[groups.index(group_val_dict[x])+1] = x
                        _new_index = tuple(_new_index)
                        #_new_index = tuple([str(i)] + [name for name in val_comb] +
                        #                    ["No group"] * (max_comb_len - len(val_comb)))
                        totals.loc[_new_index, :] = _t

                # Заполняем датафрейм результатов для бинарных показателей результатами
                if metrics_for_binary is not None:
                    for i, _bin in enumerate(bin_res.values):
                        _new_index = [str(i)] + ["No group"] * len(groups)
                        for x in val_comb:
                            _new_index[groups.index(group_val_dict[x])+1] = x
                        _new_index = tuple(_new_index)
                        # Дополним пустые индексы групп как "No group"
                        #_new_index = tuple([str(i)] + [name for name in val_comb] +
                        #                     ["No group"] * (max_comb_len - len(val_comb)))
                        bin_results.loc[_new_index, :] = _bin

        # Добавляем exp_id в таблицу результатов, если это задано в вызове функции
        # Зачем явно прописывать exp_id, если он есть в исходном датасете?????? 
        # Почему с этой настройкой функция не работает???
        if experiment_id is not None:
            if results is not None:
                results['experiment_id'] = experiment_id
            if totals is not None:
                totals['experiment_id'] = experiment_id
            if bin_results is not None:
                bin_results['experiment_id'] = experiment_id

        # Приводим нейминг таблиц с groups и без groups к единому виду            
        if metric_aggregations is not None:
            results = results.reset_index()
            results = results.drop(['cnt'], axis=1)
            res.append(results)
            if show_total:
                totals = totals.reset_index()
                totals = totals.drop(['cnt'], axis=1)
                res.append(totals)
        if metrics_for_binary is not None:
            bin_results = bin_results.reset_index().drop("cnt", axis=1)
            res.append(bin_results)
        
        return res

        """# Если нужен тотал, то выводим результаты для непрерывных показателей и тотал
        if show_total:
            if totals:
                totals = totals.reset_index()
                totals = totals.drop(['cnt'], axis=1)
            if metrics_for_binary is not None:
                bin_results = bin_results.reset_index().drop("cnt", axis=1)
                return results, totals, bin_results

            return results, totals

        # Если нужны бинарные показатели, то выводим результаты для непрерывных и бинарных покателей и тотал
        if metrics_for_binary is not None:
            bin_results = bin_results.reset_index().drop("cnt", axis=1)
            return results, totals, bin_results

        # Если ни тотал, ни бинарные показатели не нужны, то выводим просто результат
        return results"""
