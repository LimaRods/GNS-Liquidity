from dash import dcc, html
import dash_bootstrap_components as dbc 
import dash as du




TEMPLATE_TWO_COL = \
dbc.Row([
    dbc.Col([
        dbc.Row([
            # title
            dbc.Col(html.H4('TITLE 1', style ={"text-align":"left"}), width='auto'),
            # big number current status
            dbc.Col(dcc.Loading(id="big-number-1"), width='auto')
        ], justify='between', align='end', style={'paddingBottom':10}),
        # plot 
        html.Div(dcc.Loading(id='plot-1', type='circle'), style={'paddingBottom':10}),
        # definitions
        dbc.Row([
            du.graph_subheader('Definition: ', 'Definition 1'),
            du.graph_subheader('Analysis:' ,'Analysis 1'),
        ], style={'paddingBottom':10})
    ], lg=5),
    dbc.Col([
        dbc.Row([
            dbc.Col(html.H4('TITLE 2', style={"text-align":"left"}), width='auto'),
            dbc.Col(dcc.Loading(id="big-number-2"), width = 'auto')
        ], justify='between', align='end', style={'paddingBottom':10}),
        html.Div(dcc.Loading(id='plot-2', type='circle'), style={'paddingBottom':10}),
        dbc.Row([
            du.graph_subheader("Definition: ", "Definition 2"),
            du.graph_subheader("Analysis:", "Analysis 2"),
        ], style={'paddingBottom':10}),
    ], lg={'size':5, 'offset':1}),
], justify='start', style={'paddingBottom':20})

TEMPLATE_ONE_COL = \
dbc.Row([
    dbc.Col([
        dbc.Row([
            # title
            dbc.Col(html.H4('TITLE', style ={"text-align":"left"}), width='auto'),
            # big number current status
            dbc.Col(dcc.Loading(id="big-number"), width='auto')
        ], justify='between', align='end', style={'paddingBottom':10}),
        # plot 
        html.Div(dcc.Loading(id ='plot', type='circle'), style={'paddingBottom':10}),
        # definitions
        dbc.Row([
            du.graph_subheader("Definition: ", "definition"),
            du.graph_subheader("Analysis:", 'analysis guide'),
        ], style={'paddingBottom':10})
    ], lg=7),
], justify='center'),