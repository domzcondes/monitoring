from dash import html, dcc
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import pyodbc
import warnings
from datetime import datetime, timedelta, time
from dotenv import load_dotenv
import os

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)

status_colors = {
    'Succeeded': 'green',
    'Disabled': 'grey',
    'Failed': 'red',
    'Stopped': 'red',
    'Aborted': 'red',
    'Terminated': 'red',
    'Running': 'yellow'
}

status_to_num = {
    'Failed': 0.0,
    'Stopped': 0.0,
    'Aborted': 0.0,
    'Terminated': 0.0,
    'Running': 0.5,
    'Disabled': 0.75,
    'Succeeded': 1.0
}

colorscale = [
    [0.0, 'red'],        # Failed
    [0.5, 'yellow'],     # Running
    [0.75, 'grey'],      # Disabled
    [1.0, 'green']       # Succeeded
]

def load_pc_data(folder=None):
    server = os.getenv('PC_SERVER')
    database = os.getenv('PC_DATABASE')
    username = os.getenv('PC_USERNAME')
    password = os.getenv('PC_PASSWORD')

    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )

    # Workflow query
    wf_query = """
    SELECT
      run.SUBJECT_AREA        AS Folder,
      run.WORKFLOW_NAME      AS Workflow,
      run.WORKFLOW_RUN_ID    AS RunID,
      run.START_TIME,
      run.END_TIME,
      DATEDIFF(MINUTE, run.START_TIME, run.END_TIME) AS Duration,
      CASE run.RUN_STATUS_CODE
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Disabled'
        WHEN 3 THEN 'Failed'
        WHEN 4 THEN 'Stopped'
        WHEN 5 THEN 'Aborted'
        WHEN 6 THEN 'Running'
        WHEN 15 THEN 'Terminated'
        ELSE 'Unknown'
      END AS Status,
      run.RUN_ERR_CODE      AS ErrCode,
      run.RUN_ERR_MSG       AS ErrMsg,
      run.USER_NAME         AS UserName
    FROM REP_WFLOW_RUN run
    """

    if folder:
        wf_query += f"\nWHERE run.SUBJECT_AREA NOT IN ('Shared', 'Monitoring') AND run.SUBJECT_AREA = '{folder}'"
    else:
        wf_query += "\nWHERE run.SUBJECT_AREA NOT IN ('Shared', 'Monitoring')"

    wf_query += "\nORDER BY run.START_TIME DESC"

    conn = pyodbc.connect(conn_str)
    df_wf = pd.read_sql(wf_query, conn)

    # Session query
    sess_query = """
    SELECT 
        SUBJECT_AREA AS Folder,
        WORKFLOW_NAME AS Workflow,
        WORKFLOW_RUN_ID AS RunID,
        SESSION_NAME as SessionName,
        CASE RUN_STATUS_CODE
            WHEN 1 THEN 'Succeeded'
            WHEN 2 THEN 'Disabled'
            WHEN 3 THEN 'Failed'
            WHEN 4 THEN 'Stopped'
            WHEN 5 THEN 'Aborted'
            WHEN 6 THEN 'Running'
            WHEN 15 THEN 'Terminated'
            ELSE 'Unknown'
        END AS Status,
        ACTUAL_START AS ActualStart,
        SUCCESSFUL_ROWS AS SuccessfulRows
    FROM REP_SESS_LOG
    """

    if folder:
        sess_query += f"\nWHERE SUBJECT_AREA NOT IN ('Shared', 'Monitoring') AND SUBJECT_AREA = '{folder}'"
    else:
        sess_query += "\nWHERE SUBJECT_AREA NOT IN ('Shared', 'Monitoring')"

    df_sess = pd.read_sql(sess_query, conn)
    conn.close()

     # Process dates
    df_wf['START_TIME'] = pd.to_datetime(df_wf['START_TIME'])
    df_wf['END_TIME'] = pd.to_datetime(df_wf['END_TIME'])
    df_sess['ActualStart'] = pd.to_datetime(df_sess['ActualStart'])

    # Filter today’s timeframe
    now = datetime.now()
    start_time = datetime.combine(now.date() - timedelta(days=1), time(22, 0))
    end_time = datetime.combine(now.date(), time(10, 0))

    df_today = df_wf[(df_wf['START_TIME'] >= start_time) & (df_wf['START_TIME'] <= end_time)]
    df_sess_today = df_sess[df_sess['ActualStart'].between(start_time, end_time)]

    # Metrics
    total_runs = len(df_today)
    successes = df_today[df_today.Status == 'Succeeded'].shape[0]
    failures = df_today[df_today.Status == 'Failed'].shape[0]
    avg_duration = df_today['Duration'].mean() or 0

    total_sessions = len(df_sess_today)
    failed_sessions = df_sess_today[df_sess_today.Status == 'Failed'].shape[0]

    # Join session and workflow data for full view
    df_merged = pd.merge(
        df_sess_today,
        df_today[['RunID', 'START_TIME', 'END_TIME']],
        how='left',
        on='RunID'
    )
    df_merged['Duration'] = (df_merged['END_TIME'] - df_merged['START_TIME']).dt.total_seconds() / 60

    # Bar chart (workflow-level durations)
    duration_df = (
        df_today
        .groupby(['Workflow', 'Status'], as_index=False)
        .agg(total_duration=('Duration', 'sum'))
    )

    bar_fig = px.bar(duration_df, 
                     x='Workflow', 
                     y='total_duration', 
                     color='Status', 
                     color_discrete_map=status_colors,
                     title='Workflow Status',
                     labels={'total_duration': 'duration (min)'}
                    )
    bar_fig.update_layout(legend=dict(orientation='h', y=1.15, x=0.5, xanchor='center'),
        legend_title_text='', xaxis_title=None)

    status_counts = df_today.Status.value_counts().reset_index()
    status_counts.columns = ['Status', 'Count']
    pie_fig = px.pie(status_counts,
                     names='Status',
                     values='Count',
                     color='Status',
                     color_discrete_map=status_colors,
                     hole=0.4,
                     title='Status Distribution')
    pie_fig.update_layout(legend=dict(orientation='h', y=1.15, x=0.5, xanchor='center'))

    # Gantt chart from session data
    gantt_fig = px.timeline(
        df_merged,
        x_start='START_TIME',
        x_end='END_TIME',
        y='Workflow',
        color='Status',
        color_discrete_map=status_colors,
        title='Workflow Durations'
    )
    gantt_fig.update_yaxes(autorange='reversed')
    gantt_fig.update_layout(yaxis_title=None, xaxis_title=None, legend=dict(orientation='h', y=1.15, x=0.5, xanchor='center'), legend_title_text='')

    # Trend chart (6 months)
    trend = (
        df_wf[df_wf['START_TIME'] >= (datetime.now() - pd.Timedelta(days=180))]
        .groupby(df_wf['START_TIME'].dt.date)
        .agg(total=('RunID', 'count'), avg_dur=('Duration', 'mean'))
        .reset_index()
        .rename(columns={'START_TIME': 'Date'})
    )
    line_fig = px.line(trend, x='Date', y=['total', 'avg_dur'], markers=True, title='Job Trends')
    
    # Pivot-style chart (Workflow > Session > Status)
    pivot_data = df_merged[['Workflow', 'SessionName', 'Status']]
    pivot_data = pivot_data.sort_values(by=['Workflow', 'SessionName'])
    #workflows = sorted(pivot_data['Workflow'].unique())
    #sessions = sorted(pivot_data['SessionName'].unique())
    workflows = pivot_data['Workflow'].unique().tolist()
    sessions = pivot_data['SessionName'].unique().tolist()

    z = []
    text = []

    for session in sessions:
        row = []
        row_text = []
        for wf in workflows:
            match = pivot_data[
                (pivot_data['Workflow'] == wf) & 
                (pivot_data['SessionName'] == session)
            ]
            if not match.empty:
                status = match.iloc[0]['Status']
                row.append(status_to_num.get(status, np.nan))
                row_text.append(status)
            else:
                row.append(np.nan)
                row_text.append('N/A')
        z.append(row)
        text.append(row_text)

    # Create the heatmap
    pivot_fig = go.Figure(data=go.Heatmap(
        z=z,
        x=workflows,
        y=sessions,
        text=text,
        hoverinfo='text',
        hovertemplate='Workflow: %{x}<br>Session: %{y}<br>Status: %{text}<extra></extra>',
        colorscale=colorscale,
        zmin=0.0,
        zmax=1.0,
        showscale=False
    ))

    # Update layout
    height = max(300, len(sessions) * 30 + 200)
    
    pivot_fig.update_layout(
        title='',
        xaxis_title='',
        yaxis_title='',
        xaxis=dict(side='top'),
        yaxis=dict(autorange='reversed'),
        margin=dict(t=100),
        height=height
    )

    return (
        df_today,
        total_runs,
        successes,
        failures,
        avg_duration,
        bar_fig,
        pie_fig,
        gantt_fig,
        line_fig,
        df_wf,
        total_sessions,
        failed_sessions,
        pivot_fig
    )

    #return df_today, total_runs, successes, failures, avg_duration, bar_fig, pie_fig, gantt_fig, line_fig, df_wf, total_sessions, failed_sessions


def layout():
    _, _, _, _, _, _, _, _, _, df_wf, _, _, _ = load_pc_data()
    folder_options = [{"label": f, "value": f} for f in sorted(df_wf['Folder'].dropna().unique()) if f not in ['Monitoring', 'Shared']]

    return html.Div([
        html.H1("PC Jobs Summary", style={'textAlign': 'center'}),

        html.Div([
            dcc.Link("Usage Dashboard →", href="/usage", style={"fontSize": "16px", "padding": "10px"}),
            dcc.Link("MDM Jobs →", href="/mdm", style={"fontSize": "16px", "padding": "10px"})
        ], style={"position": "absolute", "top": "10px", "left": "10px", "display": "flex", "flexDirection": "column"}),

        html.Div([
            html.Label("Filter by Folder:"),
            dcc.Dropdown(
                id='pc-folder-dropdown',
                options=folder_options,
                value=None,
                placeholder="Select a folder",
                style={'width': '300px', 'margin': '0 auto'}
            )
        ], style={'textAlign': 'center', 'marginTop': '60px', 'marginBottom': '20px'}),

        html.Div(id='pc-summary-cards', className='metric-container'),
        html.Div(id='pc-graph-row', className='graph-row'),
        html.Div(id='pc-gantt-chart', className='graph-full'),
        html.Div(id='pc-pivot-chart', className='graph-tall', style={'overflowX': 'auto'}),
        html.Div(id='pc-line-chart', className='graph-full'),

        dcc.Interval(id='pc-refresh', interval=24*60*60*1000, n_intervals=0)
    ])

def register_callbacks(app):
    @app.callback(
        Output('pc-summary-cards', 'children'),
        Output('pc-graph-row', 'children'),
        Output('pc-gantt-chart', 'children'),
        Output('pc-line-chart', 'children'),
        Output('pc-pivot-chart', 'children'),
        Input('pc-folder-dropdown', 'value'),
        Input('pc-refresh', 'n_intervals')
    )
    def update_pc_dashboard(selected_folder, _):
        df_today, total_runs, successes, failures, avg_duration, bar_fig, pie_fig, gantt_fig, line_fig, _, total_sessions, failed_sessions, pivot_fig = load_pc_data(selected_folder)

        cards = [
            html.Div([html.H3("Total Runs"), html.P(str(total_runs))], className='card'),
            html.Div([html.H3("✅ Succeeded"), html.P(str(successes))], className='card'),
            html.Div([html.H3("⚠️ Failed"), html.P(str(failures))], className='card'),
            html.Div([html.H3("⏱️ Avg. Duration (m)"), html.P(f"{avg_duration:.2f}")], className='card'),
            html.Div([html.H3("🧩 Total Sessions"), html.P(str(total_sessions))], className='card'),
            html.Div([html.H3("❌ Failed Sessions"), html.P(str(failed_sessions))], className='card'),
        ]

        graphs = [
            html.Div([dcc.Graph(figure=bar_fig)], className='graph-half'),
            html.Div([dcc.Graph(figure=pie_fig)], className='graph-half'),
        ]

        return cards, graphs, dcc.Graph(figure=gantt_fig), dcc.Graph(figure=line_fig), dcc.Graph(figure=pivot_fig)
