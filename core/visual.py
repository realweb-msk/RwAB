from itertools import combinations
import plotly.graph_objs as go
import plotly.express as px
import plotly.figure_factory as ff
import logging


class ShowPlots:

    def __init__(self, df, date_col, variant_col, metrics):
        self.df = df
        self.date_col = date_col
        self.variant_col = variant_col
        self.metrics = metrics


    @staticmethod
    def cumulative_lift(df, date_col, variant_col, metric):

        df = df.sort_values(by=date_col)
        _vars = df[variant_col].unique()
        vars_comb = combinations(df[variant_col].unique(), 2)

        figures = []

        for comb in vars_comb:
            df_1 = df.query(f"{variant_col} == {comb[0]}").reset_index(drop=True)
            df_2 = df.query(f"{variant_col} == {comb[1]}").reset_index(drop=True)
            plt_data = df_1[metric].cumsum() - df_2[metric].cumsum()
            figures.append(go.Scatter(y=plt_data, x=df_1[date_col],
                                      name=f"{metric} lift variant {comb[1]} vs {comb[0]}"))

        fig = go.Figure(figures)
        fig.update_layout(title=f"Lift of cumulative sum of {metric} between groups")
        fig.show()

        return

    @staticmethod
    def time_series(df, date_col, variant_col, metric, rolling_period=None, width=None, height=None):

        _vars = df[variant_col].unique()
        dfs = [df.query(f"{variant_col} == {var}") for var in _vars]
        rolling_period = 7 if rolling_period is None else rolling_period

        figures = [
            go.Scatter(y=d.sort_values(by=date_col)[metric],
                   name=f'Experiment variant {var}',
                   x=d.sort_values(by=date_col)[date_col]
                   )
            for d, var in zip(dfs, _vars)
        ]

        means = []
        for d, var in zip(dfs, _vars):
            means.append(go.Scatter(
                y=[d[metric].mean()] * len(d),
                name=f'Mean of variant {var}',
                x=d.sort_values(by=date_col)[date_col],
                line={'dash': 'dash'}))

            means.append(go.Scatter(
                y=d[metric].rolling(rolling_period).mean(),
                x=d.sort_values(by=date_col)[date_col],
                line={'dash': 'dot'},
                name=f'rolling mean {rolling_period} variant {var}'))

        figures += means
        fig = go.Figure(figures)

        if width is None and height is None:
            fig.update_layout(autosize=True, title='Variants comparison')

        else:
            fig.update_layout(autosize=False, width=width, height=height, title='Variants comparison')

        fig.show()
        return

    @staticmethod
    def boxplots(df, variant_col, metric, autosize=False, **kwargs):

        fig = px.box(df, x=variant_col, y=metric, **kwargs)
        if autosize:
            fig.update_layout(autosize=autosize, title=f"{metric}'s boxplot per experiment variant")
        else:
            fig.update_layout(autosize=autosize, height=600, title=f"{metric}'s boxplot per experiment variant")

        fig.show()
        return

    @staticmethod
    def distplots(df, variant_col, metric, **kwargs):

        group_labels = df[variant_col].unique().astype(str)
        hist_data = [df.query(f'{variant_col} == {var}')[metric] for var in group_labels]

        fig = ff.create_distplot(hist_data, group_labels, bin_size=hist_data[0].mean() * 0.05, **kwargs)
        fig.update_layout(title='Distribution plot for experiment variants')
        fig.show()
        return

    def create_plots(self):
        for metric in self.metrics:
            logging.warning(f"PLOTS FOR {metric}")
            self.time_series(self.df, self.date_col, self.variant_col, metric)
            self.distplots(self.df, self.variant_col, metric)
            self.boxplots(self.df, self.variant_col, metric)
            self.cumulative_lift(self.df, self.date_col, self.variant_col, metric)

        return

