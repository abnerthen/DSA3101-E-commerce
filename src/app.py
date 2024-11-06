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
conversion_funnel = pd.read_parquet('conversion_funnel.parquet')

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
            height=800,
            width=1400
        )
        return html.Div([dcc.Graph(figure = funnel_graph)])


if __name__ == "__main__":
    app.run_server(debug = True)
