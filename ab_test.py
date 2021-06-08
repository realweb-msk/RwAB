from itertools import combinations
import pandas as pd
from core.stat_test import *
import numpy as np


class Pipeline:
    def __init__(self, df, verbose=0):
        self.df = df

    @staticmethod
    def grouper(df, groupby_col, experiment_variant_col, group, metric_aggregations):
        for var in df[group].unique():
            temp = df.query(f"{group} == '{var}'")
            temp = temp.groupby([groupby_col, experiment_variant_col], as_index=False).agg(metric_aggregations)
            yield var, temp


    @staticmethod
    def compute_results(grouped_data, experiment_var_col, metrics, alpha=0.05, show_total=True):
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


    def pipeline(self, groupby_col, groups, metric_aggregations, experiment_var_col, show_total=True):
        totals = None
        results = None
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

        return results, totals
