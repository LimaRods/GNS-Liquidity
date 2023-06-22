import plotly.express as px
from datetime import datetime, timedelta
from dash import dcc, html
import dash_bootstrap_components as dbc 
from . import plotting as plt
import pandas as pd
import string
import random
import plotly.graph_objects as go
EXTERNAL_STYLESHEETS=[dbc.themes.FLATLY, dbc.icons.FONT_AWESOME, "https://fonts.googleapis.com/css?family=Sofia", "assets/custom.css"]

#colors
LIGHT_GREY = "#F5F5F5"
PLOT_BACKGROUND = "#F5F5F5"
TRENDLINE = "grey"
SECONDARY = "#1f77b4"
SECONDARY_GREEN = "#179942"

# function for number formating
def human_format(num):
        num = float('{:.3g}'.format(num))
        magnitude = 0
        while abs(num) >= 1000:
            magnitude += 1
            num /= 1000.0
        return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])

def generate_split_button(id, value1, value2):
    return html.Div([
    dbc.RadioItems(
        id=id,
        className="btn-group",
        inputClassName="btn-check",
        labelClassName="btn btn-outline-primary",
        labelCheckedClassName="active",
        options=[
                {"label": value1, "value": value1.lower()},
                {"label": value2, "value": value2.lower()}
        ],
        value='overall'
    ),
    html.Div(id=id +"-output"),
], className="radio-group",)

def common_date_dropdown(id):
    today = datetime.today()
    year = datetime.today().year
    begin_year = f"{year}-01-01"
    end_year = f"{year}-12-31"
    prev_begin_year = f"{year-1}-01-01"
    prev_end_year = f"{year-1}-12-31"
    last_90 = (today - timedelta(days=90)).strftime("%Y-%m-%d")
    last_180 = (today - timedelta(days=180)).strftime("%Y-%m-%d")
    last_year = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    today = today.strftime("%Y-%m-%d")

    options=[
        {'label': 'Current Year', 'value': f"['{begin_year}', '{end_year}']"},
        {'label': 'Previous Year', 'value':f"['{prev_begin_year}', '{prev_end_year}']" },
        {'label': 'Last 90 Days', 'value': f"['{last_90}', '{today}']"},
        {'label': 'Last 180 Days', 'value': f"['{last_180}', '{today}']"},
        {'label': 'Last Year (365 Days)', 'value': f"['{last_year}', '{today}']"},
        # {'label': 'Last Quarter', 'value': f"[{}, {}]"},
        # {'label': 'Last 90 Days', 'value': f"[{}, {}]"},
    ]

    # quarters are dynamic based on todays date, e.g. q4 will not be visible in may
    quarter_starts = pd.date_range(begin_year, today, freq="Q").tolist()
    quarter_starts.insert(0, begin_year)
    quarter_starts.append(end_year)

    for i in range(len(quarter_starts)-1):
        start = pd.to_datetime(quarter_starts[i]).strftime("%Y-%m-%d")
        end = pd.to_datetime(quarter_starts[i+1]).strftime("%Y-%m-%d")
        options.append({'label':f"Q{i+1} {year}", 'value':f"['{start}', '{end}']"})
    
    return dcc.Dropdown(
        id=id,
        options=options,
        value=f"['{begin_year}', '{end_year}']"
    )

def format_common_date_dropdown_value(value):
    value = eval(value)
    val1 = pd.to_datetime(value[0])
    val2 = pd.to_datetime(value[1])
    return value[0], value[1]

# function for number formating
def human_format(num):
        num = float('{:.3g}'.format(num))
        magnitude = 0
        while abs(num) >= 1000:
            magnitude += 1
            num /= 1000.0
        return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])

def graph_subheader(type, text):
    padding = 11 if type=='Analysis:' else 5
    return html.Div([html.B(type, style={'paddingRight':padding}), html.Div(text)], style={'display':'inline-flex'})

def generate_navbar(app_name):
    navbar = dbc.NavbarSimple(
        brand=app_name,
        brand_href="#",
        color="primary",
        dark=True,
        style={"marginBottom":20}
    )
    return navbar

def generate_big_number_card_line(title:str, num1:float, num2:float, plot_df, x, y, as_of=None, info_text:str=None):
    def generate_card_line_plot(df, x, y):
        fig = px.area(df, x=x, y=y, markers=False, height=100)
        fig.update_xaxes(visible=False)
        fig.update_yaxes(visible=False)
        fig.update_layout(
            paper_bgcolor=LIGHT_GREY,
            plot_bgcolor=LIGHT_GREY,
            margin=dict(l=0, r=0, t=0, b=0))
        return fig

    if num2 >= 0:
        arrow_src = "/assets/green-up-arrow.svg"
        color = 'green'
    else:
        arrow_src = "/assets/red-down-arrow.svg"
        color = 'red'

    if as_of is not None:
        as_of_div = html.Div(" as of " + str(as_of), style={'padding':4, 'display':'inline'})
    else:
        as_of_div = html.Div()

    rand_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    info_tooltip = html.Div([
        html.I(className="fa-sharp fa-solid fa-circle-info", id=rand_id),
        dbc.Popover(
            info_text,
            target=rand_id,
            trigger='hover',
            body=True
        )
    ], style={'display':'block' if info_text is not None else 'none'})
    
    config = {'displayModeBar': False}
    card = dbc.Card([
        dbc.Row([
            html.Div([
                html.H4(title, style={"paddingRight":8, "margin":0, "marginLeft":12, "textAlign":"left"}),
                info_tooltip,
            ], style={'padding':0, 'display':'inline-flex', 'align-items':'center'}),
        ], align='center', justify='start'),
        dbc.Row([
            html.Div([
                html.H2(human_format(num1), style={"paddingRight":12, "margin":0, "marginLeft":12, "textAlign":"left"}),
                html.Div(html.Img(src=arrow_src), style={"padding":4, "paddingLeft":0}),
                html.H4(str(num2)+'%', style={'color':color, "margin":0, "padding":4})
            ], style={'padding':0, 'display':'inline-flex', 'align-items':'center'})
        ], justify="end", align="end", style={'padding':0, 'display':'inline-flex', 'align-items':'center'}),
        as_of_div,
        dcc.Graph(id=title, figure=generate_card_line_plot(df=plot_df, x=x, y=y), config=config)
    ], color=LIGHT_GREY, style={"padding":20})
    return card

def generate_big_number_card_donut(title:str, plot_df, values, names, as_of=None):
    def generate_card_donut_plot(df, values, names):
        fig = px.pie(df, values=values, names=names, hole=.7, height=150)
        fig.update_layout(
            annotations=[dict(text=str(df[values].values[0])+"%", x=0.5, y=0.5, font_size=16, showarrow=False)],
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=.5,
                font_size=12
            ),
            paper_bgcolor=LIGHT_GREY,
            plot_bgcolor=LIGHT_GREY,
            margin=dict(l=0, r=0, t=0, b=0)
        )
        fig.update_traces(textinfo='none')
        return fig

    if as_of is not None:
        as_of_div = html.Div(" as of " + str(as_of), style={'padding':4, 'display':'inline'})
    else:
        as_of_div = html.Div()

    card = dbc.Card([
        dbc.Row(dbc.Col(html.H4(title), width={"offset":0}), align='center', justify='start'),
        as_of_div,
        dcc.Graph(id=title, figure = generate_card_donut_plot(df=plot_df, values=values, names=names))
    ], color=LIGHT_GREY, style={"padding":20})
    return card

def generate_line_plot(id, plot_df, x, y, grouping_col=None, **kwargs):
    fig = plt.generate_line_plot(plot_df, x, y, grouping_col, **kwargs)

    config = {'displayModeBar': False}
    return dcc.Graph(
        id=id,
        figure=fig,
        config=config
    )

def generate_n_day_plot(id, plot_df, day_col, grouping_col, counting_col, growth_or_decay='decay', **kwargs):
    fig = plt.generate_n_day_plot(plot_df, day_col, grouping_col, counting_col, growth_or_decay, **kwargs)
    config = {'displayModeBar': False}
    return dcc.Graph(
        id=id,
        figure=fig,
        config=config
    )
def generate_map_scatter(id, plot_df, color_col, **kwargs):
    """Generate map scatter plot
    Parameters
    ----------
    plot_df: must contain lat/long

    """
    fig = plt.generate_map_scatter(plot_df, color_col, **kwargs)
    return dcc.Graph(
        id=id,
        figure=fig
    )

def generate_bar_chart(id, plot_df, x, y, **kwargs):
    fig = plt.generate_bar_chart(plot_df, x, y, **kwargs)
    config = {'displayModeBar': False}
    return dcc.Graph(
        id=id,
        figure=fig,
        config=config
    )

def generate_multiaxis_line_bar_plot(
    id, plot_df, x, y_line, y_bar, grouping_col=None, trendline=None, **kwargs):
    fig = plt.generate_multiaxis_line_bar_plot( plot_df, x, y_line, y_bar, grouping_col, trendline, **kwargs)
    
    config = {'displayModeBar': False}
    return dcc.Graph(
        id=id,
        figure=fig,
        config=config
    )

def generate_donut_chart(id, plot_df, values, names, **kwar):
    fig = plt.generate_donut_chart(plot_df, values, names, **kwar)
    config = {'displayModeBar': False}

    return dcc.Graph(
            id=id,
            figure=fig,
            config=config
    )

def generate_split_button(id, values):
    return html.Div([
    dbc.RadioItems(
        id=id,
        className="btn-group",
        inputClassName="btn-check",
        labelClassName="btn btn-outline-primary",
        labelCheckedClassName="active",
        options=[
                {"label": value, "value": value.lower()}
                for value in values
        ],
        value='overall'
    ),
    html.Div(id=id +"-output"),
], className="radio-group",)

def generate_two_col(
    title1:str = None,
    bignumid1:str = None,
    buttonid1:str = None,
    buttonvalues1:str = None,
    plotid1:str = "",
    maindiv1:html.Div=None,
    definition1:str = None,
    analysis1:str = None,
    title2:str = None,
    bignumid2:str = None,
    buttonid2:str = None,
    buttonvalues2:str = None,
    plotid2:str = "",
    maindiv2:html.Div=None,
    definition2:str = None,
    analysis2:str = None
):

    return dbc.Row([
        dbc.Col([
            dbc.Row([
                # title
                dbc.Col(html.H4(title1, style ={"text-align":"left"}), width='auto'),
                # big number current status
                dbc.Col(dcc.Loading(id=bignumid1), width='auto') if bignumid1 is not None else None,
                dbc.Col(generate_split_button(buttonid1, buttonvalues1), width='auto') if buttonid1 is not None else None,
            ], justify='between', align='end', style={'paddingBottom':10}),
            # plot 
            html.Div(dcc.Loading(id=plotid1, type='circle'), style={'paddingBottom':10}) if maindiv1 is None else maindiv1,
            # definitions
            dbc.Row([
                graph_subheader('Definition: ', definition1),
                graph_subheader('Analysis:' , analysis1),
            ], style={'paddingBottom':10})
        ], lg=5),
        dbc.Col([
            dbc.Row([
                dbc.Col(html.H4(title2, style={"text-align":"left"}), width='auto'),
                dbc.Col(dcc.Loading(id=bignumid2), width='auto') if bignumid2 is not None else None,
                dbc.Col(generate_split_button(buttonid2, buttonvalues2), width='auto') if buttonid2 is not None else None,
            ], justify='between', align='end', style={'paddingBottom':10}),
            html.Div(dcc.Loading(id=plotid2, type='circle'), style={'paddingBottom':10}) if maindiv2 is None else maindiv2,
            dbc.Row([
                graph_subheader("Definition: ", definition2),
                graph_subheader("Analysis:", analysis2),
            ], style={'paddingBottom':10}),
        ], lg={'size':5, 'offset':1}),
    ], justify='start', style={'paddingBottom':20})

def generate_one_col(
    title:str = None,
    bignumid:str = None,
    buttonid:str = None,
    buttonvalues:str = None,
    plotid:str = "",
    maindiv:html.Div=None,
    definition:str = None,
    analysis:str = None
):
    return dbc.Row([
        dbc.Col([
            dbc.Row([
                # title
                dbc.Col(html.H4(title, style ={"text-align":"left"}), width='auto'),
                # big number current status
                dbc.Col(dcc.Loading(id=bignumid), width='auto') if bignumid is not None else None,
                dbc.Col(generate_split_button(buttonid, buttonvalues), width='auto') if buttonid is not None else None,
            ], justify='between', align='end', style={'paddingBottom':10}),
            # plot 
            html.Div(dcc.Loading(id=plotid, type='circle'), style={'paddingBottom':10}) if maindiv is None else maindiv,
            # definitions
            dbc.Row([
                graph_subheader("Definition: ", definition),
                graph_subheader("Analysis:", analysis),
            ], style={'paddingBottom':10})
        ], lg=7),
    ], justify='center')
