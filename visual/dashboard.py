import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, callback_context
from dash.dependencies import Input, Output
import tab_layout
import data_processing
import numpy as np

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    dcc.Tabs(id="dashboard_tabs", value="tab_1", children=[
        dcc.Tab(label="Test description", value="tab_1"),
        dcc.Tab(label="Summary", value="tab_2"),
        dcc.Tab(label="A/B test", value="tab_3"),
        dcc.Tab(label="Test", value="tab_4")
    ]),
    html.Div(id="tab_content")
])


@app.callback(
    Output("tab_content", "children"),
    Input("dashboard_tabs", "value")
)
def render_tabs(tab):
    if tab == "tab_1":
        return tab_layout.first_tab_layout(backlog_df)

    elif tab == "tab_2":
        return tab_layout.second_tab_layout(res_df)

    elif tab == "tab_3":
        return tab_layout.third_tab_layout(stat_res_df)

    return html.Div([
        html.H3("SMTH WENT WRONG!")
    ])


# TODO: Rewrite function with cashing, or State
@app.callback(
    Output("test_name", "children"),
    Output("test_status", "children"),
    Output("test_description", "children"),
    Output("test_region", "children"),
    Output("test_audience", "children"),
    Output("test_result", "children"),

    Input("tab_1_dropdown_selector", "value")
)
def update_tab_1_content(filter_value, dropdown_col="name"):
    _plt_data = data_processing.process_tbl_for_dropdown(backlog_df, dropdown_col, filter_value)

    _status = _plt_data["status"].values[0]
    _name = _plt_data["name"].values[0]
    _description = _plt_data["description"].values[0]
    _region = _plt_data["region"].values[0]
    _audience = _plt_data["auditory"].values[0]
    _result = _plt_data["result"].values[0]

    return (
        html.H3(f"{_name}"),
        html.H3(f"Статус: {_status}"),
        html.H3(f"{_description}"),
        html.H3(f"{_region}"),
        html.H3(f"{_audience}"),
        html.H3(f"{_result}")
    )

# TODO: Write single method to hande data filtering and use chained callbacks

@app.callback(
    Output("tab_2_sessions_sum", "children"),
    Output("tab_2_transactions_sum", "children"),
    Output("tab_2_revenue_sum", "children"),
    Output("tab_2_add_to_cart_sum", "children"),
    Output("tab_2_transaction_cr", "children"),
    Output("tab_2_to_cart_cr", "children"),
    Output("tab_2_control_var_funnel", "figure"),
    Output("tab_2_exp_var_funnel", "figure"),


    Input("tab_2_test_name_dropdown", "value"),
    Input("tab_2_date_picker", "start_date"),
    Input("tab_2_date_picker", "end_date"),
    Input("tab_2_device_type_dropdown", "value"),
    Input("tab_2_user_type_dropdown", "value"),
)
def update_tab_2_content(_test_name, _start_date, _end_date, _device_type, _user_type,
                         _experiment_var_col="experimentVariant"):

    _plt_data = data_processing.filter_tbl_tab_2(res_df, _test_name, _start_date,
                                                 _end_date, _device_type, _user_type)

    _df_sums = data_processing.get_df_sums(
        _plt_data,
        ["sessions", "users", "transactionRevenue", "transactions", "addToCart"]
    )

    _sessions_sum = _df_sums["sessions"]
    _transactions_sum = _df_sums["transactions"]
    _revenue_sum = _df_sums["transactionRevenue"]
    _add_to_cart_sum = _df_sums["addToCart"]
    _to_cart_cr = _df_sums["addToCart"] / _df_sums["sessions"]
    _to_transaction_cr = _df_sums["transactions"] / _df_sums["sessions"]

    # Funnels
    _exp_data = _plt_data[_plt_data[_experiment_var_col] == 0]
    _exp_data_sums = data_processing.get_df_sums(
        _exp_data,
        ["sessions", "users", "transactionRevenue", "transactions", "addToCart"]
    )

    _ctrl_data = _plt_data[_plt_data[_experiment_var_col] == 1]
    _ctrl_data_sums = data_processing.get_df_sums(
        _ctrl_data,
        ["sessions", "users", "transactionRevenue", "transactions", "addToCart"]
    )

    stages = ["Сеансы", "Добавления в корзину", "Покупки"]
    funnel_control = px.funnel(
        x=[_ctrl_data_sums["sessions"], _ctrl_data_sums["addToCart"], _ctrl_data_sums["transactions"]],
        y=stages, title="Исходный вариант"
    )
    funnel_control.update_layout(transition_duration=500)

    funnel_exp = px.funnel(
        _exp_data,
        x=[_exp_data_sums["sessions"], _exp_data_sums["addToCart"], _exp_data_sums["transactions"]],
        y=stages, title="Тестовый вариант"
    )
    funnel_exp.update_layout(transition_duration=500)

    return (
        f"Сеансы: {_sessions_sum}",
        f"Транзакции: {_transactions_sum}",
        f"Доход: {_revenue_sum}",
        f"Добавлений в корзину: {_add_to_cart_sum}",
        f"CR в транзакцию: {round(_to_transaction_cr * 100, 2)}%",
        f"CR в корзину: {round(_to_cart_cr * 100, 2)}%",
        funnel_control,
        funnel_exp
    )


@app.callback(
    Output("tab_2_avg_revenue_hor_bar", "figure"),

    Input("tab_2_test_name_dropdown", "value"),
    Input("tab_2_date_picker", "start_date"),
    Input("tab_2_date_picker", "end_date"),
    Input("tab_2_user_type_dropdown", "value")
)
def update_hor_bar_tab_2(_test_name, _start_date, _end_date, _user_type, device_col="deviceCategory",
                         experiment_var_col="experimentVariant", revenue_col="transactionRevenue"):
    # Filter date range and test name
    _plt_data = data_processing.process_tbl_for_dropdown(
        data_processing.process_table_date_range(res_df, _start_date, _end_date),
        dropdown_col="name",
        filter_value=_test_name
    )

    # Filter user_type
    _plt_data = data_processing.process_tbl_for_dropdown(
        _plt_data,
        dropdown_col="userType",
        filter_value=_user_type
    )

    _plt_data = _plt_data.groupby([device_col, experiment_var_col], as_index=False).agg({
        revenue_col: np.mean
    })
    _plt_data[experiment_var_col] = _plt_data[experiment_var_col].astype(str)

    fig = px.bar(_plt_data, x=revenue_col, y=device_col, orientation="h", barmode="group",
                 color=experiment_var_col, title="Средний чек по вариантам")
    fig.update_layout(transition_duration=500)

    return fig


@app.callback(
    Output("tab_2_timeline", "figure"),

    Input("tab_2_test_name_dropdown", "value"),
    Input("tab_2_date_picker", "start_date"),
    Input("tab_2_date_picker", "end_date"),
    Input("tab_2_device_type_dropdown", "value"),
    Input("tab_2_user_type_dropdown", "value"),
    Input("tab_2_aov_button", "n_clicks"),
    Input("tab_2_cr_button", "n_clicks"),
    Input("tab_2_revenue_button", "n_clicks"),
    Input("tab_2_users_button", "n_clicks"),
    Input("tab_2_sessions_button", "n_clicks"),
    Input("tab_2_transactions_button", "n_clicks")

)
def update_timeline_tab_2(_test_name, _start_date, _end_date, _device_type, _user_type, *args):
    changed_id = [p['prop_id'] for p in callback_context.triggered][0]

    plt_data = data_processing.filter_tbl_tab_2(res_df, _test_name, _start_date, _end_date, _device_type, _user_type)
    plt_data = plt_data.sort_values(by="date", ascending=True)

    if 'tab_2_aov_button' in changed_id:
        plt_col = "transactionRevenue"
    elif 'tab_2_cr_button' in changed_id:
        plt_col = "cr"
        plt_data[plt_col] = plt_data["transactions"] / plt_data["sessions"]
    elif 'tab_2_revenue_button' in changed_id:
        plt_col = "transactionRevenue"
    elif 'tab_2_users_button' in changed_id:
        plt_col = "users"
    elif 'tab_2_sessions_button' in changed_id:
        plt_col = "sessions"
    elif 'tab_2_transactions_button' in changed_id:
        plt_col = "transactions"
    else:
        plt_col = "transactionRevenue"
        # msg = 'None of the buttons have been clicked yet'

    fig = px.line(plt_data, x="date", y=plt_col, title=f"Изменение {plt_col}")

    return fig


@app.callback(
    Output("tab_3_other_filters", "children"),

    Input("tab_3_test_name_dropdown", "value")
)
def update_filters_tab_3(test_name):
    _df = stat_res_df[stat_res_df["name"] == test_name]
    _columns_and_uniques = data_processing.get_df_columns_and_uniques(_df)

    _test_type = _columns_and_uniques["test_type"]
    _segment_0 = _columns_and_uniques["group_0"]
    _segment_1 = _columns_and_uniques["group_1"]
    _metric = _columns_and_uniques["metric"]

    return [
        dcc.Dropdown(
            _test_type,
            value=_test_type[0],
            id="tab_3_test_type_dropdown"
        ),

        dcc.Dropdown(
            _segment_0,
            value=_segment_0[0],
            id="tab_3_segment_0_dropdown"
        ),

        dcc.Dropdown(
            _segment_1,
            value=_segment_1[0],
            id="tab_3_segment_1_dropdown"
        ),

        dcc.Dropdown(
            _metric,
            value=_metric[0],
            id="tab_3_metric_dropdown"
        ),
    ]


@app.callback(
    Output("tab_3_information_blocks", "children"),
    Output("tab_3_exp_group_pie", "figure"),
    Output("tab_3_contr_group_pie", "figure"),
    Output("tab_3_bar", "figure"),

    Input("tab_3_test_name_dropdown", "value"),
    Input("tab_3_test_type_dropdown", "value"),
    Input("tab_3_segment_0_dropdown", "value"),
    Input("tab_3_segment_1_dropdown", "value"),
    Input("tab_3_metric_dropdown", "value"),
    Input("tab_3_sessions_button", "n_clicks"),
    Input("tab_3_transactions_button", "n_clicks"),
    Input("tab_3_revenue_button", "n_clicks"),

)
def update_tab_3_content(_test_name, _test_type, _test_segment_0, _test_segment_1, _test_metric, *args):
    changed_id = [p['prop_id'] for p in callback_context.triggered][0]
    plt_data = data_processing.filter_tbl_tab_3(stat_res_df, _test_name, _test_type, _test_segment_0,
                                                _test_segment_1, _test_metric)

    plt_pie = res_df[res_df["name"] == _test_name].groupby(by=["deviceCategory", "experimentVariant"],
                                                           as_index=False).agg({
        "transactionRevenue": "sum",
        "sessions": "sum",
        "transactions": "sum"
    })

    plt_bar = res_df[res_df["name"] == _test_name].groupby(by=["userType", "experimentVariant"],
                                                           as_index=False).agg({
        "transactionRevenue": "sum",
        "sessions": "sum",
        "transactions": "sum"
    })

    if 'tab_3_revenue_button' in changed_id:
        plt_col = "transactionRevenue"
    elif 'tab_2_sessions_button' in changed_id:
        plt_col = "sessions"
    elif 'tab_2_transactions_button' in changed_id:
        plt_col = "transactions"

    else:
        plt_col = "transactions"

    pie_exp = px.pie(
        plt_pie[plt_pie["experimentVariant"] == 0],
        names="deviceCategory",
        values=plt_col,
        title="Контрольный вариант"
    )

    pie_control = px.pie(
        plt_pie[plt_pie["experimentVariant"] == 1],
        names="deviceCategory",
        values=plt_col,
        title="Тестовый вариант"
    )

    bar = px.bar(
        plt_bar,
        x="experimentVariant",
        y=plt_col,
        color="userType",
        title="Тип пользователя по вариантам"
    )

    return (
        html.Div([
            html.H2(f"p-value: {plt_data['p_value'].values[0]:.5}")
        ], id="tab_3_p_value"),
        pie_exp,
        pie_control,
        bar
    )


if __name__ == '__main__':
    backlog_path = "./data/_test_backlog.csv"
    res_path = "./data/_res.csv"
    stat_res_path = "./data/_stat_results.csv"

    backlog_df = pd.read_csv(backlog_path, sep=',')
    res_df = pd.read_csv(res_path, sep=',')
    stat_res_df = pd.read_csv(stat_res_path, sep=',')

    # We'll use backlog_df to add test names to other tables
    res_df = res_df.merge(backlog_df[["name", "experimentId"]], how="inner",
                          left_on="experimentId", right_on="experimentId")
    stat_res_df = data_processing.process_stat_res_table(res_df, stat_res_df, backlog_df)

    app.run_server(debug=True)
