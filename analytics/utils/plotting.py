import plotly.express as px
from datetime import datetime, timedelta
from dash import dcc, html
import dash_bootstrap_components as dbc 
import pandas as pd
import plotly.graph_objects as go
#colors
LIGHT_GREY = "#F5F5F5"
MEDIUM_GREY = "#D0D0D0"
PRIMARY_BLUE = 'rgb(25,33,96)'
PLOT_BACKGROUND = 'rgba(0,0,0,0)'
SECONDARY = "#1f77b4"
SECONDARY_GREEN = "#179942"
def apply_default_layout(fig, **kwargs):
    fig.update_layout(
        legend=dict(
            orientation="v",
            y=kwargs.get('leg_y', 1),
            x=kwargs.get('leg_x', .8),
            font_size=12,
            title=None,
            bgcolor='rgba(0,0,0,0)'
        ),
        height=320,
        legend_traceorder='normal',
        showlegend=kwargs.get('showlegend', True),
        #paper_bgcolor=LIGHT_GREY,
        plot_bgcolor=PLOT_BACKGROUND,
        margin=dict(l=0, r=0, t=0, b=0),
        hovermode=kwargs.get('hovermode', 'closest')
    )
    fig.update_yaxes(
        title=None, 
        showgrid=True,
        zeroline=True,
        gridcolor=LIGHT_GREY,
        ticklabelposition="inside top",  
        ticksuffix=kwargs.get('ticksuffix', None), 
        tickprefix=kwargs.get('tickprefix', None)
    )
    fig.update_xaxes(title=None)

    return fig

def generate_line_plot(plot_df, x, y, grouping_col=None, trendline=None, **kwargs):
    fig = px.scatter(
        plot_df, x=x, y=y, color=grouping_col ,# height=300, 
        trendline=trendline,
        hover_data=kwargs.get('hover_data', None),
        trendline_options=kwargs.get('trendline_options', None)
    )

    fig.update_traces(
        mode=kwargs.get('mode', 'lines+markers'),
        fill=kwargs.get('fill', None),
        marker_color = kwargs.get('marker_color', PRIMARY_BLUE) if grouping_col is None else None
    )
    if grouping_col is None:
        fig.update_traces(
             marker_color = kwargs.get('marker_color', PRIMARY_BLUE)
        )
    
    if trendline is not None: fig.data[-1].line.color = kwargs.get('trend_color', SECONDARY_GREEN)

    fig = apply_default_layout(fig, **kwargs)
    
    fig.update_layout(
        yaxis_range = kwargs.get('yaxis_range', None),
        yaxis_tickformat = kwargs.get('yaxis_tickformat', None)
    )
    fig.update_yaxes(
        title_text = kwargs.get('title_text_y', None)
    )

    fig.update_xaxes(
        title_text = kwargs.get('title_text_x', None)
    )
    
    return fig 

def generate_n_day_plot(plot_df, day_col, grouping_col, counting_col, growth_or_decay='decay', **kwargs):
    full_counts = plot_df.groupby(grouping_col)[counting_col].nunique().rename('total_count').reset_index()
    # pivot to count users above threshold fby day
    full = []
    for i in range(1, int(max(plot_df[day_col]))):
        if growth_or_decay == 'growth':
            sub = plot_df[plot_df[day_col] <= i].groupby(grouping_col)[counting_col].nunique().rename('count').reset_index()
        else:
            sub = plot_df[plot_df[day_col] >= i].groupby(grouping_col)[counting_col].nunique().rename('count').reset_index()
        sub[day_col] = i
        full.append(sub)
    full_df = pd.concat(full)

    full_df = full_counts.merge(full_df, on=grouping_col, how='right')
    full_df['perc'] = full_df['count'] / full_df['total_count'] * 100
    
    fig = px.line(full_df, x=day_col, y='perc', color=grouping_col) #, height=300)
    fig = apply_default_layout(fig, **kwargs)

    return fig


def generate_map_scatter(plot_df, color_col, **kwargs):
    """Generate map scatter plot
    Parameters
    ----------
    plot_df: must contain lat/long

    """
    fig = px.scatter_geo(
        plot_df, 
        lat='lat', lon='lon', 
        color=color_col, 
        hover_name=kwargs.get('hover_name', None), 
        size=kwargs.get('marker_size_col', None), 
        color_continuous_scale=kwargs.get('color_continuous_scale', None), 
        projection=kwargs.get('projection', "natural earth")
    )

    return fig

def generate_bar_chart(plot_df, x, y, **kwargs):

    fig = px.bar(plot_df, x=x, y=y, 
        color=kwargs.get('color', None),
        hover_data=kwargs.get('hover_data', None),
    )
    
    fig.update_layout(
        yaxis_range = kwargs.get('yaxis_range', None),
        yaxis_tickformat = kwargs.get('yaxis_tickformat', None)            
    )

    fig = apply_default_layout(fig, **kwargs)
    
    fig.update_yaxes(
        title_text = kwargs.get('title_text_y', None)
    )

    fig.update_xaxes(
        title_text = kwargs.get('title_text_x', None)
    )
    return fig

def generate_multiaxis_line_bar_plot(
    plot_df, x, y_line, y_bar, grouping_col=None, trendline=None, **kwargs):
    fig = px.scatter(plot_df, x=x, y=y_line, 
        color=grouping_col,
        trendline=trendline,
        hover_data=kwargs.get('hover_data', None)
    )

    fig.update_traces(
        mode=kwargs.get('mode', 'lines+markers'),
    )
    if grouping_col is None:
        fig.update_traces(marker_color = kwargs.get('color_line', PRIMARY_BLUE))

    if trendline is not None: 
        fig.data[-1].line.color = MEDIUM_GREY
        fig.data[-1].marker.color = MEDIUM_GREY
        

    fig.add_trace(
        go.Bar(x=plot_df[x],
            y=plot_df[y_bar],
            name = y_bar,
            yaxis='y2',
            marker=dict(
                color=kwargs.get('color_bar', SECONDARY_GREEN),
                opacity=kwargs.get('opacity', .4)
            ),
            text=plot_df[grouping_col] if grouping_col is not None else None, 
            textposition='inside',
        )
    )
    fig = apply_default_layout(fig, **kwargs)

    fig.update_layout(
        yaxis=dict(
            title=y_line,
             titlefont=dict(
                 color=kwargs.get('color_line', PRIMARY_BLUE)
            ),
            tickfont=dict(
                color= kwargs.get('color_line', PRIMARY_BLUE)
            ),
            ticklabelposition="inside top", 
            ticksuffix=kwargs.get('line_ticksuffix', None), 
            tickprefix=kwargs.get('line_tickprefix', None)
        ),
        yaxis2=dict(
            title=y_bar,
            range = kwargs.get('range', None),
            titlefont=dict(color=kwargs.get('color_bar', SECONDARY_GREEN)),
            tickfont=dict(color=kwargs.get('color_bar', SECONDARY_GREEN)),
            # anchor="free",
            overlaying="y",
            side="right",
            showgrid=False,
            ticklabelposition="inside top", 
            ticksuffix=kwargs.get('bar_ticksuffix', None), 
            tickprefix=kwargs.get('line_tickprefix', None)
        )
    )

    return fig

def generate_donut_chart(plot_df, values, names, **kwargs):
    fig = px.pie(
        data_frame= plot_df,
        values = values,
        names = names,
        hole = .6
    )

    fig.update_layout(
        annotations = [dict(
            text = kwargs.get('text', None),
            x = .5, 
            y = .5,
            font_size = 30,
            showarrow = False)],         
    )
  
    fig = apply_default_layout(fig)
    fig.update_layout(
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-.1,
                xanchor="center",
                x=.5
            ),
            showlegend = kwargs.get('showlegend',True)
    )
    return fig