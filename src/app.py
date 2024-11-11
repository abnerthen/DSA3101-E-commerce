import pandas as pd
from dash import Dash, dcc, html, Output, Input
import plotly.express as px
import plotly.graph_objects as go
import webbrowser
import threading
import os

conversion_funnel = pd.read_parquet('conversion_funnel.parquet')
channel_conversion_rate = pd.read_parquet('channel_conversion_rate.parquet')

def func():
    online_retail = pd.read_csv('online_retail.csv')
    online_retail['InvoiceDate'] = pd.to_datetime(online_retail['InvoiceDate'])
    online_retail['Country'] = online_retail['Country'].fillna('Unknown')

    # Sort by customer and invoice date for churn calculations
    online_retail = online_retail.sort_values(by=['CustomerID', 'InvoiceDate'])
    online_retail['NextPurchaseDate'] = online_retail.groupby('CustomerID')['InvoiceDate'].shift(-1)
    online_retail['DaysSinceLastPurchase'] = (online_retail['NextPurchaseDate'] - online_retail['InvoiceDate']).dt.days

    # Define churned customers (no purchase within 30 days)
    churned = online_retail[online_retail['DaysSinceLastPurchase'] > 30]
    online_retail['YearMonth'] = online_retail['InvoiceDate'].dt.to_period('M')

    # Calculate churn rate by month
    monthly_churn = churned.groupby('YearMonth').size() / online_retail.groupby('YearMonth').size()
    monthly_churn = monthly_churn.reset_index().rename(columns={0: 'ChurnRate'})
    monthly_churn['YearMonth'] = monthly_churn['YearMonth'].dt.to_timestamp()
    
    return monthly_churn_country

country_churn = func()

external_stylesheets = [
    {
        "href": (
            "https://fonts.googleapis.com/css2?"
            "family=Lato:wght@400;700&display=swap"
        ),
        "rel": "stylesheet",
    },
]
app = Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "E-Commerce Analytics"

'''
header = html.Div(
            children = [html.H1(children = "E-Commerce Optimisation Dashboard",className = "header-title"),
                html.P(children = (
                        "An all-in-one dashboard for e-commerce transactions" 
                        "based on different categories"
                    ),
                    className = 'header-description',
                ),
            ],
            className = 'header',
        )
'''

# Layout with tabs
app.layout = html.Div([
    html.Div([
    dcc.Tabs(id="tabs", value="tab-1",
             vertical = True, children=[
        dcc.Tab(label="Conversion Funnel", value="tab-1"),
        dcc.Tab(label = 'Conversion Rate by Channel', value = 'tab-2'),
        dcc.Tab(label = 'Monthly Churn Rate by Country', value = 'tab-3')
    ],
    style={
                "display": "flex",
                "flexDirection": "column",
                "width": "15%",
                "height": "100vh",  # Full viewport height
            },)
], style = {"float": "left"}),
html.Div(id='content', style = {'marginLeft': '15%', 'padding': '20px'})
])

# Define callback to update content based on selected tab
@app.callback(
    Output("content", "children"),
    [Input("tabs", "value")]
)

def render_content(tab):
    if tab == "tab-1":
        # Sales trend line chart
        unique_categories = conversion_funnel['category'].unique()
        color_map = {category: f'rgba({(i * 50) % 255}, {(i * 100) % 255}, {(i * 150) % 255}, 0.8)' for i, category in enumerate(unique_categories)}

        # Create a list of colors for each stage based on the category
        colors = [color_map[category] for category in conversion_funnel['category']]
        funnel_graph = go.Figure(go.Funnel(
            y=conversion_funnel['action'],
            x=conversion_funnel['users'],
            text=conversion_funnel['category'],
            marker_color=colors,
            textposition='inside',
            textinfo='text+value+percent initial'
        ))
        funnel_graph.update_layout(
            title_text='Google Merchandise Store Conversion Path',
            height=700,
            width=1200
        )
        return html.Div([dcc.Graph(figure = funnel_graph)])
    
    elif tab == 'tab-2':
        df = channel_conversion_rate
        fig = px.sunburst(df,
                          path = ['channel', 'main_category'],
                          values = 'total_conversions',
                          color = 'channel', 
                          title = 'Conversions by Channel & Category')
        return html.Div([dcc.Graph(figure = fig)])
    
    elif tab == 'tab-3':
        fig = px.choropleth(
            country_churn,
            locations="Country",
            locationmode="country names",
            color="ChurnRate",
            title="Churn Rate by Country",
            color_continuous_scale="Reds"
        )
        return html.Div([dcc.Graph(figure=fig)])

    

def open_browser():
    webbrowser.open_new("http://127.0.0.1:8050")

if __name__ == "__main__":
    threading.Timer(1, open_browser).run()
    app.run_server(host = '0.0.0.0')
