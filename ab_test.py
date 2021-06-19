from itertools import combinations
import pandas as pd
from core.stat_test import *
import numpy as np


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
    def grouper(df, groupby_col, experiment_variant_col, group, metric_aggregations):

        for var in df[group].unique():
            temp = df.query(f"{group} == '{var}'")
            temp = temp.groupby([groupby_col, experiment_variant_col], as_index=False).agg(metric_aggregations)
            yield var, temp


    @staticmethod
    def compute_results(grouped_data, experiment_var_col, metrics, alpha=0.05, show_total=True):
        """
        Функция для расчета попарных результатов A/B теста для групп

        :param grouped_data: (pandas.DataFrame), Сгруппированный датафрейм
        :param experiment_var_col: (str), Имя столбца с вариантом эксперимента
        :param metrics: (iterable), Названия столбцов с метриками, которые будут сравниваться
        :param alpha: (float, optional, default=0.05), alpha-value для статистических тестов
        :param show_total: (bool, optional, default=True), Выводить ли датафрейм с общей статистикой
        :return: При show_total=True, возвращает tuple из двух датафреймов: res и tot, res - с результатами
        эксперимента, при show_total=False возвращает только res
        """
        variants = grouped_data[experiment_var_col].unique()
        dfs_vars = {}

        for i, var in enumerate(variants):
            dfs_vars[f'df_{i}'] = grouped_data.query(f"{experiment_var_col} == @var")

        res = pd.DataFrame(columns=['first', 'second', 'metric'])
        res = res.set_index(['first', 'second', 'metric'])

        keys_comb = combinations(dfs_vars, 2)
        vals_comb = combinations(dfs_vars.values(), 2)

        for df in dfs_vars.values():
            for metric in metrics:
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
                        res.loc[col_name, 'ttest'] = independent_ttest(df_1[metric], df_2[metric], alpha=alpha)

                    else:
                        res.loc[col_name, 'mann_whitneyu'] = mann_whitneyu_test(df_1[metric], df_2[metric], alpha=alpha)

                else:
                    equal_var = mood_var(df_1[metric], df_2[metric], alpha=alpha)
                    res.loc[col_name, 'equal_variance'] = equal_var
                    equal_median = kruskal_wallis(df_1[metric], df_2[metric])
                    res.loc[col_name, 'equal_median'] = equal_median

                    res.loc[col_name, 'mann_whitneyu'] = mann_whitneyu_test(df_1[metric], df_2[metric], alpha=alpha)
                try:
                    res.loc[col_name, 'mann_whitneyu_log'] = mann_whitneyu_test(np.log(df_1[metric]),
                                                                            np.log(df_2[metric]), alpha=alpha)
                except:
                    raise

        if show_total:
            total_sum = grouped_data.groupby(experiment_var_col, as_index=False)[metrics].sum()\
                .rename(columns={metric: metric+'_sum' for metric in metrics})
            total_mean = grouped_data.groupby(experiment_var_col, as_index=False)[metrics].mean()\
                .rename(columns={metric: metric+'_mean' for metric in metrics})
            total = total_sum.merge(total_mean, left_on=experiment_var_col, right_on=experiment_var_col)
            return res, total

        return res


    def pipeline(self, groupby_col, metric_aggregations, experiment_var_col, groups=None, show_total=True,
                 experiment_id=None):
        """
        Метод с пайплайном анализа результатов всего A/B теста. Выполняет предобработку и группировку данных.
        Возможно посмотреть результаты A/B теста в определенных разрезах (например отдельно по новым пользователям)
        :param groupby_col: (str), Название столбца по которму будет идти группировка данных, например по дате
        :param metric_aggregations: (dict), Словарь следующего формата: {'metric_name': ['agg_func']},
            например: {'transactions': 'sum'}
        :param experiment_var_col: (str), Название столбца с параметром варианта эксперимента
        :param groups: (iterable, optional, default=None), Список с названиями столбцов, по которым будет идти срез
        :param show_total: (bool, optional, default=True), См. описание метода compute_results
        :return: При groups = None возвращаются общие результаты для групп
        """

        totals = None
        results = None


        if groups is None:
            _ = self.df.groupby([groupby_col, experiment_var_col], as_index=False).agg(metric_aggregations)
            res, total = self.compute_results(_, experiment_var_col, list(metric_aggregations.keys()))

            if experiment_id is not None:
                res['experiment_id'] = experiment_id
                total['experiment_id'] = experiment_id

            return res, total

        for group in groups:

            for _ in self.grouper(self.df, groupby_col, experiment_var_col, group, metric_aggregations):
                name = _[0]
                df_gr = _[1]
                res, total = self.compute_results(df_gr, experiment_var_col, list(metric_aggregations.keys()))
                res = res.reset_index()

                if results is None:
                    results = pd.DataFrame(columns=np.append(['cnt', 'group'], res.columns))
                    results = results.set_index(['cnt', 'group'])
                    totals = pd.DataFrame(columns=np.append(['cnt', 'group'], total.columns))
                    totals = totals.set_index(['cnt', 'group'])

                for i, _r in enumerate(res.values):
                    results.loc[(i, name), :] = _r

                for i, _t in enumerate(total.values):
                    totals.loc[(i, name), :] = _t

        if experiment_id is not None:
            results['experiment_id'] = experiment_id
            totals['experiment_id'] = experiment_id

        if show_total:
            return results, totals

        return results
