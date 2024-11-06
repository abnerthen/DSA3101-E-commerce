import pandas as pd
from dash import Dash, dcc, html, Output, Input
import plotly.express as px

total_revenue = pd.read_parquet('total_revenue.parquet')
agg_sales_by_cat = pd.read_parquet('agg_sales_by_cat.parquet')
# purchases_per_user = pd.read_parquet('purchases_per_user.parquet')
# purchase_averages = pd.read_parquet('purchase_averages.parquet')
# top_customers = pd.read_parquet('top_customers.parquet')
# numerical_correlation = pd.read_parquet('numerical_correlation.parquet')
# nested_correlation = pd.read_parquet('nested_correlation.parquet')
# price_sensitive = pd.read_parquet('price_sensitive.parquet')
# by_region = pd.read_parquet('by_region.parquet')
# by_category = pd.read_parquet('by_category.parquet')

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
        dcc.Tab(label="Total Revenue", value="tab-1"),
        dcc.Tab(label="Aggregate Sales by Category", value="tab-2"),
    ],
    style={
                "display": "flex",
                "flexDirection": "column",
                "width": "20%",
                "height": "100vh",  # Full viewport height
            },)
], style = {"float": "left"}),
html.Div(id='content', style = {'marginLeft': '25%', 'padding': '20px'})
])

# Define callback to update content based on selected tab
@app.callback(
    Output("content", "children"),
    [Input("tabs", "value")]
)

def render_content(tab):
    if tab == "tab-1":
        # Sales trend line chart
        fig = px.line(total_revenue, x="date", y="total_revenue", title="Total Revenue by Date")
        return html.Div([
            dcc.Graph(figure=fig)
        ])

    elif tab == "tab-2":
        # Monthly sales by category bar chart
        df = agg_sales_by_cat[agg_sales_by_cat['main_category'] != 'Unavailable']
        fig = px.area(df, x="date", y="revenue", color="main_category", title="Monthly Sales by Category")
        return html.Div([
            dcc.Graph(figure=fig)
        ])

if __name__ == "__main__":
    app.run_server(debug = True)
