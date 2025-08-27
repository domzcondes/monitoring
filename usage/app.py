from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import usage
import mdm_jobs
import pc_jobs

app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "Monitoring Dashboard"

# Set up routing
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

def index_page():
    return html.Div([
        html.H1("Dashboard Home", style={'textAlign': 'center'}),
        html.Div([
            html.A("🔍 Usage Dashboard", href="/usage", style={'marginRight': '20px'}),
            html.A("🧩 MDM Jobs Dashboard", href="/mdm", style={'marginRight': '20px'}),
            html.A("⚙️ PC Jobs Dashboard", href="/pc")
        ], style={'textAlign': 'center', 'marginTop': '40px'})
    ])

@app.callback(Output('page-content', 'children'), Input('url', 'pathname'))
def display_page(pathname):
    if pathname == "/usage":
        return usage.layout
    elif pathname == "/mdm":
        return mdm_jobs.layout()
    elif pathname == "/pc":
        return pc_jobs.layout()
    else:
        return index_page()

# Register callbacks
usage.register_callbacks(app)
pc_jobs.register_callbacks(app)

if __name__ == '__main__':
    app.run(debug=True, port=8050)
