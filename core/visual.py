from itertools import combinations
import plotly.graph_objs as go
import plotly.express as px
import plotly.figure_factory as ff
import scipy.stats as st
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
            plt_data = (df_2[metric].cumsum() - df_1[metric].cumsum()) / df_1[metric].cumsum() * 100
            figures.append(go.Scatter(y=plt_data, x=df_1[date_col],
                                      name=f"{metric} lift variant {comb[1]} vs {comb[0]}"))

        fig = go.Figure(figures)
        fig.update_layout(title=f"Relative lift of cumulative sum of {metric} between groups")
        fig.show()

        return

    @staticmethod
    def time_series(df, date_col, variant_col, metric, rolling_period=None, width=None, height=None, alpha=.95):

        _vars = df[variant_col].unique()
        dfs = [df.query(f"{variant_col} == {var}") for var in _vars]
        rolling_period = 7 if rolling_period is None else rolling_period

        figures = []
        for d, var in zip(dfs, _vars):
            # Mean line
            figures.append(go.Scatter(
                y=[d[metric].mean()] * len(d),
                name=f'Mean of variant {var}',
                x=d.sort_values(by=date_col)[date_col],
                line={'dash': 'dash'}))

            rolling_mean = d[metric].rolling(rolling_period).mean()
            # Rolling mean
            figures.append(go.Scatter(
                y=rolling_mean,
                x=d.sort_values(by=date_col)[date_col],
                line={'dash': 'solid'},
                name=f'rolling mean {rolling_period} variant {var}')
            )
            # Conf. intervals
            figures.append(go.Scatter(
                y=rolling_mean * 1.05,
                x=d.sort_values(by=date_col)[date_col],
                line=dict(color='rgba(0,0,0,0)'),
                name=f'Conf. interval lower bound {rolling_period} variant {var}')
            )

            figures.append(go.Scatter(
                y=rolling_mean * .95,
                x=d.sort_values(by=date_col)[date_col],
                line=dict(color='rgba(0,0,0,0)'),
                fill='tonexty',
                fillcolor='rgba(176, 228, 255, 0.56)',
                name=f'Conf. interval upper bound {rolling_period} variant {var}')
            )

        fig = go.Figure(figures)

        if width is None and height is None:
            fig.update_layout(autosize=True, title='Variants comparison')

        else:
            fig.update_layout(autosize=False, width=width, height=height, title='Variants comparison')

        fig.show()
        return

    @staticmethod
    def p_value_dynamic(df, date_col, variant_col, metric, alpha=0.05, **kwargs):
        df = df.sort_values(by=date_col)
        _vars = df[variant_col].unique()
        vars_comb = combinations(df[variant_col].unique(), 2)

        figures = []

        for comb in vars_comb:
            df_1 = df.query(f"{variant_col} == {comb[0]}")
            df_2 = df.query(f"{variant_col} == {comb[1]}")
            p_values = []

            for i in range(1, len(df_1)):
                p_value = st.mannwhitneyu(df_1[metric][:i], df_2[metric][:i])[1]
                p_values.append(p_value)

            figures.append(go.Scatter(
                x=df_1[date_col],
                y=p_values,
                name=f'P-values for variants {comb}'
            ))

        fig = go.Figure(figures, **kwargs)
        fig.add_trace(go.Scatter(
            x=df[date_col].unique(),
            y=[alpha]*len(df[date_col].unique()),
            name=f'Alpha-value = {alpha}'
        ))
        fig.update_layout(title=f'p_value dynamic for {metric}')
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
            self.p_value_dynamic(self.df, self.date_col, self.variant_col, metric)
        return

