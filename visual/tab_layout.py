from dash import dcc, html
import data_processing


def first_tab_layout(backlog_df):
    """Generates layout for the first tab of A/B test Dashboard"""
    _columns_and_uniques = data_processing.get_df_columns_and_uniques(backlog_df)
    # _region = backlog_df[""]
    layout = html.Div([
        html.Div([
            dcc.Dropdown(
                _columns_and_uniques["name"],
                value=_columns_and_uniques["name"][0],
                id="tab_1_dropdown_selector",
                style={"align": "left", "width": "50%", "height": "30px"}
            ),
            html.Hr(id="horizontal_line", style={"textAlign": "center", "borderTop": "3px solid red",
                                                 "overflow": "visible"})
        ]),

        # Test description
        html.Div([
            html.H2(id="test_name", style={"textAlign": "center"})
        ], style={"margin": "auto", "backgroundColor": "pink", "width": "80%", "height": "70px",
                  "verticalAlign": "middle", "marginBottom": "20px"}),

        # Create two columns with shit
        html.Div([
            html.Div([
                html.H1("Описание", id="left_col_header"),
                html.Br(),
                html.Div([], id="test_status", style={"margin": "auto", "backgroundColor": "pink", "width": "50%",
                                            "height": "50px", "fontStyle": "oblique", "verticalAlign": "textBottom"}),
                html.Br(),
                html.Div([
                    html.Div([], id="test_description"),
                    html.Br(),
                    html.Div([], id="test_region"),
                    html.Br(),
                    html.Div([], id="test_audience")
                ], id="tab_1_right_column", style={"backgroundColor": "#ffcc66"})
            ], className="leftCol", style={"float": "left", "width": "50%", "textAlign": "center",
                                           "margin": "left"}),

            html.Div([
                html.H1("Результаты", id="left_col_header", style={"textAlign": "center"}),
                html.Br(),
                html.Div([html.H3(id="test_result")], style={"backgroundColor": "pink",
                                                             "width": "50%", "height": "50px", "fontStyle": "oblique",
                                                             "verticalAlign": "textBottom"})
            ], className="rightCol", style={"textAlign": "center"}),

        ])
    ])

    return layout


def second_tab_layout(res_df):
    """Generates layout for the second tab of A/B test Dashboard"""
    _columns_and_uniques = data_processing.get_df_columns_and_uniques(res_df)

    layout = html.Div([
        html.Div([
            # Test variant name
            dcc.Dropdown(
                _columns_and_uniques["name"],
                value=_columns_and_uniques["name"][0],
                id="tab_2_test_name_dropdown"
            ),

            # Date picker range
            dcc.DatePickerRange(
                start_date=min(_columns_and_uniques["date"]),
                end_date=max(_columns_and_uniques["date"]),
                id="tab_2_date_picker"
            ),

            # Device type
            dcc.Dropdown(
                _columns_and_uniques["deviceCategory"],
                value=_columns_and_uniques["deviceCategory"][0],
                id="tab_2_device_type_dropdown"
            ),

            # User type
            dcc.Dropdown(
                _columns_and_uniques["userType"],
                value=_columns_and_uniques["userType"][0],
                id="tab_2_user_type_dropdown"
            )
        ]),

        html.Br(),

        html.Div([
            html.Div([html.H2(id="tab_2_sessions_sum")], style={"float": "left", "textAlign": "center"}),
            html.Div([html.H2(id="tab_2_transactions_sum")], style={"textAlign": "center"}),
            html.Div([html.H2(id="tab_2_revenue_sum")]),
            html.Div([html.H2(id="tab_2_add_to_cart_sum")]),
            html.Div([html.H2(id="tab_2_transaction_cr")]),
            html.Div([html.H2(id="tab_2_to_cart_cr")])
        ]),

        html.Br(),

        # Avg revenue horizontal hist
        html.Div([
            dcc.Graph(id="tab_2_avg_revenue_hor_bar")
        ]),

        html.Br(),

        # Two funnel bars
        html.Div([
            dcc.Graph(id="tab_2_exp_var_funnel"),
            dcc.Graph(id="tab_2_control_var_funnel"),
        ]),

        html.Br(),

        # Three buttons
        html.Div([
            html.Button("AOV", id="tab_2_aov_button", n_clicks=0),
            html.Button("CR", id="tab_2_cr_button", n_clicks=0),
            html.Button("Доход", id="tab_2_revenue_button", n_clicks=0),
            html.Button("Пользователи", id="tab_2_users_button", n_clicks=0),
            html.Button("Сеансы", id="tab_2_sessions_button", n_clicks=0),
            html.Button("Транзакции", id="tab_2_transactions_button", n_clicks=0)
        ]),

        html.Div([
            dcc.Graph(id="tab_2_timeline")
        ])
    ])

    return layout


def third_tab_layout(stat_res_df):
    """Generates layout for the third tab of A/B test Dashboard"""
    _columns_and_uniques = data_processing.get_df_columns_and_uniques(stat_res_df)

    layout = html.Div([
        html.Div([
            # Date picker range
            # dcc.DatePickerRange(
            #     start_date=min(_columns_and_uniques["min_date"]),
            #     end_date=max(_columns_and_uniques["max_date"]),
            #     id="tab_3_date_picker"
            # ),

            # Test variant name
            dcc.Dropdown(
                _columns_and_uniques["name"],
                value=_columns_and_uniques["name"][0],
                id="tab_3_test_name_dropdown"
            ),

            # Other filters based on test name with
            # Segment pickers, Metric picker, Test type picker
            html.Div([], id="tab_3_other_filters"),

            html.Br(),

            # Information blocks
            html.Div([], id="tab_3_information_blocks")
        ]),

        html.Div([
            html.Button("Доход", id="tab_3_revenue_button", n_clicks=0),
            html.Button("Сеансы", id="tab_3_sessions_button", n_clicks=0),
            html.Button("Транзакции", id="tab_3_transactions_button", n_clicks=0)
        ]),

        html.Div([
            dcc.Graph(id="tab_3_exp_group_pie"),

            dcc.Graph(id="tab_3_contr_group_pie"),

            dcc.Graph(id="tab_3_bar")
        ])

    ])

    return layout
