from itertools import combinations
import pandas as pd
from stat_test import *


def compute_results(grouped_data, id_col, experiment_var_col, metrics, alpha=0.05):
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

    return res

