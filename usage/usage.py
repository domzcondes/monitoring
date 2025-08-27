from dash import dcc, html, Output, Input
import plotly.express as px
import pandas as pd
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Paths to usage CSV files
csv_file_paths = {
    os.getenv("MDM_DEV"): "MDM Dev",
    os.getenv("SQL_DEV"): "SQL Dev",
    os.getenv("PWC_DEV"): "PWC Dev",
    os.getenv("MDM_SIT"): "MDM Sit",
    os.getenv("SQL_SIT"): "SQL Sit",
    os.getenv("PWC_SIT"): "PWC Sit",
    os.getenv("MDM_PRD"): "MDM Prd",
    os.getenv("SQL_PRD"): "SQL Prd",
    os.getenv("PWC_PRD"): "PWC Prd"
}

# Load and process data
def load_data(file_path):
    df = pd.read_csv(file_path, sep='|')
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Value'] = df['Value'].astype(float)
    df['Threshold'] = df['Threshold'].astype(float)

    gb_metrics = ['Memory Usage'] + [m for m in df['Metric'].unique() if 'Free Space' in m]
    df.loc[df['Metric'].isin(gb_metrics), ['Value', 'Threshold']] /= 1024 ** 3
    return df

# Time range options
TIME_OPTIONS = {
    "All": None,
    "Last 15 minutes": datetime.timedelta(minutes=15),
    "Last 1 hour": datetime.timedelta(hours=1),
    "Last 6 hours": datetime.timedelta(hours=6)
}

# Layout for the /usage route
layout = html.Div([
    html.H1("System Metrics Dashboard", style={"textAlign": "center"}),
    
    html.Div([
        dcc.Link("MDM Jobs →", href="/mdm", style={
            "fontSize": "16px", "padding": "10px", "display": "inline-block", "textDecoration": "none"
        }),
        dcc.Link("PC Jobs →", href="/pc", style={
            "fontSize": "16px", "padding": "10px", "display": "inline-block", "textDecoration": "none"
        })
    ], style={    "position": "absolute",
    "top": "10px",
    "left": "10px",
    "display": "flex",
    "flexDirection": "column",
    "alignItems": "flex-start"}),
    
    html.Div([
        html.Label("Select Environment:"),
        dcc.Dropdown(
            id='file-selector',
            options=[{"label": alias, "value": path} for path, alias in csv_file_paths.items()],
            value=list(csv_file_paths.keys())[0],
            clearable=False,
            style={"width": "300px", "margin": "0 auto"}
        )
    ], style={"textAlign": "center", "marginTop": "20px"}),

    html.Div([
        html.Label("Select Time Range:"),
        dcc.Dropdown(
            id='time-range-dropdown',
            options=[{"label": k, "value": k} for k in TIME_OPTIONS.keys()],
            value="All",
            clearable=False,
            style={"width": "250px", "margin": "0 auto"}
        )
    ], style={"textAlign": "center", "marginTop": "20px", "marginBottom": "30px"}),

    html.Div(id='latest-values', style={
        "display": "flex",
        "flexWrap": "wrap",
        "justifyContent": "center",
        "marginTop": "20px"
    }),

    html.Div(id='metrics-grid-container', className="grid-container", style={"padding": "10px"}),

    dcc.Interval(
        id='refresh-interval',
        interval=300000,  # 5 minutes
        n_intervals=0
    ),
])

def register_callbacks(app):
    @app.callback(
        Output('metrics-grid-container', 'children'),
        Output('latest-values', 'children'),
        Input('refresh-interval', 'n_intervals'),
        Input('time-range-dropdown', 'value'),
        Input('file-selector', 'value')
    )
    def update_dashboard(n, selected_range, selected_file):
        df = load_data(selected_file)

        if TIME_OPTIONS[selected_range]:
            time_cutoff = df['Timestamp'].max() - TIME_OPTIONS[selected_range]
            df = df[df['Timestamp'] >= time_cutoff]

        latest_time = df['Timestamp'].max()
        latest = df[df['Timestamp'] == latest_time]

        metric_cards = []

        # CPU
        cpu_df = df[df['Metric'] == 'CPU Usage']
        cpu_fig = px.line(cpu_df, x="Timestamp", y="Value", title="CPU Usage")
        cpu_fig.update_layout(yaxis=dict(range=[0, 100]))
        if not cpu_df.empty:
            cpu_fig.add_hline(y=85, line_dash="dot", annotation_text="CPU Threshold", line_color="red")
        metric_cards.append(html.Div([
            html.H2("CPU Usage", style={"textAlign": "center"}),
            dcc.Graph(figure=cpu_fig, config={'displayModeBar': False}, style={"height": "300px"})
        ], className="metric-card"))

        # Memory
        mem_df = df[df['Metric'] == 'Memory Usage']
        mem_fig = px.line(mem_df, x="Timestamp", y="Value", title="Memory Usage (GB)")
        if not mem_df.empty:
            max_mem = max(mem_df['Threshold'].max() * 1.1, mem_df['Value'].max() * 1.1)
            mem_fig.update_layout(yaxis=dict(range=[0, max_mem]))
            mem_fig.add_hline(y=mem_df['Threshold'].iloc[-1], line_dash="dot", annotation_text="Memory Threshold", line_color="red")
        metric_cards.append(html.Div([
            html.H2("Memory Usage", style={"textAlign": "center"}),
            dcc.Graph(figure=mem_fig, config={'displayModeBar': False}, style={"height": "300px"})
        ], className="metric-card"))

        # Disks
        disk_df = df[df['Metric'].str.contains('Free Space')]
        max_disk_threshold = disk_df['Threshold'].max() if not disk_df.empty else 1

        for disk_metric in sorted(disk_df['Metric'].unique()):
            sub_df = disk_df[disk_df['Metric'] == disk_metric]
            if sub_df.empty:
                continue
            fig = px.line(sub_df, x="Timestamp", y="Value", title=f"{disk_metric}")
            fig.update_traces(line=dict(color='blue'))
            fig.add_hline(y=sub_df['Threshold'].iloc[-1], line_dash="dash", line_color="red", annotation_text="Threshold")
            fig.update_layout(yaxis=dict(range=[0, max_disk_threshold * 1.1]))
            metric_cards.append(html.Div([
                html.H2(disk_metric, style={"textAlign": "center"}),
                dcc.Graph(figure=fig, config={'displayModeBar': False}, style={"height": "300px"})
            ], className="metric-card"))

        # Latest metric status cards
        latest_cards = []
        for _, row in latest.iterrows():
            metric = row['Metric']
            is_good = True
            if 'CPU' in metric:
                is_good = row['Value'] <= 85
            elif 'Memory' in metric:
                is_good = (row['Value'] / row['Threshold']) <= 0.85
            elif 'Free Space' in metric:
                is_good = (row['Value'] / row['Threshold']) >= 0.15

            bar_color = "green" if is_good else "red"

            latest_cards.append(
                html.Div([
                    html.H4(metric, style={"textAlign": "center"}),
                    html.P(f"Value: {row['Value']:.2f} GB" if 'Memory' in metric or 'Free Space' in metric else f"Value: {row['Value']:.2f}%", style={"textAlign": "center"}),
                    html.P(f"Threshold: {row['Threshold']:.2f} GB" if 'Memory' in metric or 'Free Space' in metric else f"Threshold: {row['Threshold']:.2f}%", style={"textAlign": "center"}),
                    html.Div(style={
                        "height": "10px",
                        "width": "100%",
                        "backgroundColor": bar_color,
                        "marginTop": "10px",
                        "borderRadius": "5px"
                    })
                ], style={
                    "padding": "20px",
                    "border": "1px solid #ddd",
                    "borderRadius": "10px",
                    "margin": "10px",
                    "minWidth": "200px",
                    "textAlign": "center",
                    "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"
                })
            )

        return metric_cards, latest_cards
