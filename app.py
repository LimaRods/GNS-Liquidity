import pandas as pd
import numpy as np
import onchain_data
import query_data
import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc 
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import analytics.utils.dash as du
import dash_loading_spinners as dls
from dash.exceptions import PreventUpdate
from dateutil.relativedelta import relativedelta

app = dash.Dash(
    __name__,
    external_stylesheets=du.EXTERNAL_STYLESHEETS,
    title='Dev Funnel',
    suppress_callback_exceptions=True
)
server = app.server

app.layout = html.Div([
    html.Div(id='main-div', children=[
        du.generate_navbar("KeyRock Challenge"),

        # -------- SIMULATOR  ----------------------------------------------- #
        html.Div(children=[
       
            html.Div(html.H3('Simulator for GNS Pool Liquidity'), style={'marginBottom':20, 'marginTop':20}),
            dbc.Row([
                        dbc.Col([
                            dbc.Row([
                                html.B('Choose the Token to Buy GNS', style = {'paddingBottom':5,'paddingTop':5,'paddingLeft':10}),
                                dcc.Dropdown(
                                        id='token-list',
                                        value = 'USDC',
                                        multi= False,
                                        style = {'paddingLeft':10}
                                ),
                                html.Div(
                                id = 'exchange-rate'
                                ),
                                html.Div(
                                id = 'exchange-rate2'
                                )
                            ])
                        ]),
                        dbc.Col([
                            dbc.Row([
                                html.B('Number of GNS in the Pool'),
                                dbc.Col([
                                    dbc.Row(
                                        [dcc.Input(id = 'token-GNS', type = 'number', value= 1000, placeholder = 'GNS Amount')], style={'paddingRight':20}),
                                ]),
                                dbc.Col([
                                    dbc.Row([
                                        dcc.Input(id = 'token-from', type = 'number',value= 4000, placeholder = 'Token 1 Amount')
                                    ])
                                ])
                            ]),
                            html.Br(),
                            dbc.Row([
                                html.B('Token 1 Swapped', style = {'paddingBottom':5,'paddingTop':5,'paddingLeft':10}),
                                dbc.Col([
                                    dbc.Row(
                                        [dcc.Input(id = 'token-swapped', type = 'number', value= 200, placeholder = 'Token 1 Swapped')],  style={'paddingRight':20}),
                                ]),
                                dbc.Col([
                                    dbc.Row(
                                        html.Button('Submit', id='submit-button', n_clicks=0)
                                    )
                                ])
                            ]),

                        ])
                    ], justify='start', style={'paddingBottom':20}),
            
            html.Br(),
            du.generate_two_col(
                title1 = 'Token Reserves in the Pool',
                plotid1 = "token-reserves",
                definition1 = 'This refers to the quantity of tokens present in the primary token-pair pool.',
                analysis1= 'Evaluate the distribution of each token within the pool and observe how it fluctuates with the number of swaps between specific tokens and GNS.',
                title2 = 'USD Volume IN and OUT of the Pool',
                plotid2= "token-flow",
                definition2 = "The inflow, outflow, and slippage of swaps in USD while trading a specific token for GNS. It's important to note that the quantity of tokens leaving the pool does not equal the quantity entering the pool due to adjustments made by the K = xy formula.",
                analysis2 ="evaluate the behavior of slippage and token flow based on the pool's initial reserves and the number of tokens swapped."
            ),
             du.generate_two_col(
                title1 = 'Price Impact on GNS (Slippage)',
                plotid1 = "price-impact",
                definition1 = 'Slippage refers to the discrepancy between the anticipated price of a trade and the price at which the trade is actually executed.',
                analysis1= 'Evaluate the trend of slippage on GNS price in relation to the number of tokens being swapped.',
                title2 = 'Price Change Over Deposits',
                plotid2= "price-change",
                definition2 = 'The variation in token prices in relation to the quantity of tokens swapped for GNS.',
                analysis2 ='Examine the price trends of each token.'
            ),
        ], style={'marginRight': '30px', 'marginLeft': '30px'}) # Close main dbc.Div()
    ]), # Close html.Div()
    html.Footer(style={'marginTop':60, 'height':100, 'padding':0, 'backgroundColor':du.LIGHT_GREY}),
    dcc.Store(id='token-prices', data=onchain_data.get_token_prices()),
])


# Callbacks ----------------------------------------------------------

# Dropdown
@app.callback(
    [Output("token-list", "options"),
     Input("token-prices", "data")]

)
def display_tokens(data):

    return [list(data.keys())]

# Exchange rates display
@app.callback(
    [Output("exchange-rate", "children"),
    Output("exchange-rate2", "children"),
    Input("token-list", "value"),
    Input("token-prices", "data")]
)
def token_prices(token1, data):
    token1_price_usd = data[token1]
    token0_price_usd = data['GNS']
    token1_price = round(token1_price_usd/ token0_price_usd,4) #Token 1 prices in GNS
    token0_price = round(token1_price**-1,4)

    price_display = html.B(f"{token1} = {token1_price_usd} USD | GNS = {token0_price_usd} USD")
    price_display2 =  html.B(f"{token1} = {token1_price} GNS | GNS = {token0_price} {token1}")
    return [price_display], [price_display2]


#Charts 
@app.callback(
    [Output("token-reserves", "children"),
    Output("token-flow", "children"),
    Output("price-impact", "children"),
    Output("price-change", "children")],
    [Input("submit-button", "n_clicks")],
    [State("token-list", "value"),
    State("token-prices", "data"),
    State("token-GNS", "value"),
    State("token-from", "value"),
    State("token-swapped", "value")]
)
def generate_charts(n_clicks,token1, data, GNS_amount, token1_amount, token_swapped):
    token1_price_usd = data[token1]
    token0_price_usd = data['GNS']

    df = query_data.AMM_contract(
                in_amount = token1_amount,
                in_price_usd = token1_price_usd,
                out_amount = GNS_amount,
                out_price_usd = token0_price_usd,
                deposit_limit= token_swapped,
                token_from =token1
    )
    # Generate Token Reserve Chart
    df_melt = df.melt(id_vars=["token_deposit"], value_vars=[token1, 'GNS'], 
                    var_name="Token", value_name="Reserve")


    fig_reserve = px.bar(df_melt, x="Token", y="Reserve", color="Token",
                animation_frame="token_deposit", range_y=[0, df[[token1, 'GNS']].max().max() + 5],
                height=500)  # Set the height of the chart explicitly
    
    fig_reserve.update_layout(
    autosize=True,
    margin=dict(l=10, r=10, b=10, t=10, pad=0),
    showlegend=False
    )
    graph_reserve = [dcc.Graph(id='bar-chart',config = {'displayModeBar': False}, figure=fig_reserve)]


    # Generate token flow chart
    df_melt_flow = df.melt(id_vars=["token_deposit"], value_vars=["Amount_IN_USD", "Amount_OUT_USD", "Slippage_USD"], 
                            var_name="Token Flow", value_name="Volume [USD]")

    fig_flow = px.bar(df_melt_flow, x="Token Flow", y="Volume [USD]", color="Token Flow",
                        animation_frame="token_deposit", 
                        range_y=[0, df[["Amount_IN_USD", "Amount_OUT_USD", "Slippage_USD"]].max().max() + 5],
                        height=500)  # Set the height of the chart explicitly

    fig_flow.update_layout(
        autosize=True,
        margin=dict(l=10, r=10, b=10, t=10, pad=0),
        showlegend=False
    )
    graph_flow = [dcc.Graph(id='bar-chart',config = {'displayModeBar': False}, figure=fig_flow)]


    # Generating GNS Slippage chart
    df_line = df.copy()
    df_line.rename(columns = {"token_deposit": f"{token1} Deposit"}, inplace = True)
    fig_slippage = px.line(df_line, x = f"{token1} Deposit", y = 'GNS Slippage percent', height=450)
    fig_slippage.update_layout(
        autosize=True,
        margin=dict(l=10, r=10, b=10, t=10, pad=0),
        showlegend=False
    )
    graph_slippage = [dcc.Graph(id='bar-chart',config = {'displayModeBar': False}, figure=fig_slippage)]

 #Generate Price charts
    df_price = df.copy()
    df_price.rename(columns = {"token_deposit": f"{token1} Deposit"}, inplace = True)
    fig = make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Scatter(x=df_price[f"{token1} Deposit"], y= df_price[f'Price of GNS in {token1}'], mode='lines', name= "GNS"),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=df_price[f"{token1} Deposit"], y=df_price[f'Price of {token1} in GNSD'], mode='lines', name= f"{token1}"),
        secondary_y=True,
    )

    # Set x-axis title
    fig.update_xaxes(title_text=f"{token1} Deposit")
    # Set y-axes titles
    fig.update_yaxes(title_text=f"Price of GNS in {token1}", secondary_y=False)
    fig.update_yaxes(title_text=f"Price of {token1} in GNS", secondary_y=True)

    fig.update_layout(
        autosize=True,
        margin=dict(l=10, r=10, b=10, t=10, pad=0)
    )
    graph_price = [dcc.Graph(id='line-chart',config = {'displayModeBar': False}, figure=fig)]
   

    return  graph_reserve, graph_flow, graph_slippage, graph_price



if __name__ == '__main__':
    app.run_server(debug=True, port = 8051)

