from dash import html, dcc
import plotly.express as px
import pandas as pd
import pyodbc
import warnings
from datetime import datetime, timedelta, time
from dotenv import load_dotenv
import os

load_dotenv()

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)

def load_mdm_data():
    server = os.getenv('MDM_SERVER')
    database = os.getenv('MDM_DATABASE')
    username = os.getenv('MDM_USERNAME')
    password = os.getenv('MDM_PASSWORD')

    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};DATABASE={database};UID={username};PWD={password}'
    )

    query = """
    WITH jgc AS (
        SELECT ROWID_JOB_GROUP_CONTROL, ROWID_JOB_GROUP
        FROM C_REPOS_JOB_GROUP_CONTROL
    ),
    jc AS (
        SELECT 
            ROWID_JOB_GROUP_CONTROL,
            TABLE_DISPLAY_NAME,
            START_RUN_DATE,
            END_RUN_DATE,
            b.JOB_STATUS_DESC AS STATUS,
            STATUS_MESSAGE,
            CASE 
                WHEN STATUS_MESSAGE LIKE '%rejected records%' THEN CAST(SUBSTRING(
                    STATUS_MESSAGE,
                    CHARINDEX('with ', STATUS_MESSAGE) + 5,
                    CHARINDEX(' rejected records', STATUS_MESSAGE) - (CHARINDEX('with ', STATUS_MESSAGE) + 5)
                ) AS INT)
                ELSE 0 
            END AS REJECTS
        FROM C_REPOS_JOB_CONTROL a
        LEFT JOIN C_REPOS_JOB_STATUS_TYPE b ON a.RUN_STATUS = b.JOB_STATUS_CODE
    )
    SELECT 
        jg.JOB_GROUP_NAME AS GroupName,
        jc.TABLE_DISPLAY_NAME AS Display,
        jc.START_RUN_DATE AS Start,
        jc.END_RUN_DATE AS [End],
        SUBSTRING(jc.STATUS, CHARINDEX('|', jc.STATUS)+1, LEN(jc.STATUS) - CHARINDEX('|', jc.STATUS)) AS Status,
        jc.STATUS_MESSAGE AS Message,
        jc.REJECTS AS Rejects
    FROM C_REPOS_JOB_GROUP jg
    LEFT JOIN jgc ON jg.ROWID_JOB_GROUP = jgc.ROWID_JOB_GROUP
    LEFT JOIN jc ON jgc.ROWID_JOB_GROUP_CONTROL = jc.ROWID_JOB_GROUP_CONTROL
    WHERE jg.JOB_GROUP_NAME IN (
        'StgBatchGroupSAP', 'BOBatchGroupAD', 'StgBatchGroupAD', 
        'BOBatchGroupSap', 'TokenMatchMergeGrp', 
        'BOBatchGroup_SRC_ID_SAPNO_FLAG_LDG_STG_BO', 
        'StgBatchGroupWorkday', 'BOBatchGroupWorkday'
    )
    AND jc.START_RUN_DATE >= '2025-01-01'
    """

    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    conn.close()

    df['Start'] = pd.to_datetime(df['Start'])
    df['End'] = pd.to_datetime(df['End'])
    df['Date'] = df['Start'].dt.date

    now = datetime.now()
    start_time = datetime.combine(now.date() - timedelta(days=1), time(22, 0))
    end_time = datetime.combine(now.date(), time(10, 0))

    df_today = df[(df['Start'] >= start_time) & (df['Start'] <= end_time)]

    total_jobs = len(df_today)
    total_rejects = df_today['Rejects'].sum()
    avg_duration = (df_today['End'] - df_today['Start']).mean()
    failed_jobs = df_today[df_today['Message'].str.contains("Error|401|Failed", case=False, na=False)].shape[0]
    
    custom_order = [
    "Party",
    "Party Relationship",
    "Party Source ID",
    "Party Status",
    "Party Postal Address",
    "Postal Address",
    "Party Electronic Address",
    "Party Phone Communication",
    "STG_PARTY",
    "STG_PARTY_REL",
    "Staging Party Source ID",
    "STG_PARTY_STATUS",
    "STG_PARTY_POSTAL_ADD",
    "STG_POSTAL_ADD",
    "STG_PARTY_ETRC_ADD",
    "STG_PARTY_PH_COMM",
    "STG_PARTY_WD",
    "STG_PARTY_REL_WD",
    "S_PARTY_SOURCE_ID_WD",
    "STG_PARTY_STATUS_WD",
    "STG_PARTY_PSTL_ADD_WD",
    "STG_PSTL_ADD_WD",
    "STG_PARTY_ERTC_ADD_WD",
    "STG_PARTY_PH_COMM_WD",
    "STG_PARTY_AD",
    "STG_PARTY_REL_AD",
    "S_PARTY_SOURCE_ID_AD",
    "STG_PARTY_STATUS_AD",
    "STG_PARTY_PSTL_ADD_AD",
    "STG_PSTL_ADD_AD",
    "STG_PARTY_ETRC_ADD_AD",
    "STG_PARTY_PH_COMM_AD"
]
    bar_fig = px.bar(df_today, x='Display', y='Rejects', color='Status', title="Rejects per Job", text='Rejects', category_orders={'Display': custom_order})
    bar_fig.update_layout(xaxis_title=None, 
        legend=dict(orientation="h", y=1.2, x=0.5, xanchor="center", yanchor="top"),
        legend_title_text=''
        )

    status_counts = df_today['Status'].value_counts().reset_index()
    status_counts.columns = ['Status', 'Count']
    pie_fig = px.pie(status_counts, names='Status', values='Count', title='Job Status Distribution', hole=0.4)
    pie_fig.update_layout(legend=dict(orientation="h", y=1.16, x=0.5, xanchor="center", yanchor="top"),
        )

    group_order = [
    'StgBatchGroupAD', 'BOBatchGroupAD',
    'StgBatchGroupSAP', 'BOBatchGroupSap',
    'StgBatchGroupWorkday', 'BOBatchGroupWorkday',
    'BOBatchGroup_SRC_ID_SAPNO_FLAG_LDG_STG_BO',
    'TokenMatchMergeGrp'
    ]
    gantt_fig = px.timeline(df_today, x_start='Start', x_end='End', y='GroupName', color='Status', hover_data=['Display', 'Message', 'Rejects'], title='Job Durations', category_orders={'GroupName': group_order})
    gantt_fig.update_yaxes(autorange="reversed")
    gantt_fig.update_layout(xaxis_title=None, 
        yaxis_title=None, 
        legend=dict(orientation="h", y=1.16, x=0.5, xanchor="center", yanchor="top"),
        legend_title_text=''
        )

    trend_df = df.groupby('Date').agg(
        total_jobs=('Display', 'count'),
        total_rejects=('Rejects', 'sum'),
        avg_duration=('Start', lambda x: (df.loc[x.index, 'End'] - x).mean().total_seconds() if not x.empty else 0)
    ).reset_index()

    line_fig = px.line(trend_df, x='Date', y=['total_jobs', 'total_rejects', 'avg_duration'], markers=True, title='Job Trends Over Time')
    line_fig.update_layout(legend=dict(orientation="h", y=1.16, x=0.5, xanchor="center", yanchor="top")
    )

    return df_today, total_jobs, total_rejects, avg_duration, failed_jobs, bar_fig, pie_fig, gantt_fig, line_fig

def layout():
    df_today, total_jobs, total_rejects, avg_duration, failed_jobs, bar_fig, pie_fig, gantt_fig, line_fig = load_mdm_data()

    return html.Div([
        html.H1("MDM Batch Summary", style={'textAlign': 'center'}),
        
        html.Div([
            dcc.Link("Usage Dashboard →", href="/usage", style={
                "fontSize": "16px", "padding": "10px", "display": "inline-block", "textDecoration": "none"
            }),
            dcc.Link("PC Jobs →", href="/pc", style={
                "fontSize": "16px", "padding": "10px", "display": "inline-block", "textDecoration": "none"
            })
        ], style={"position": "absolute",
            "top": "10px",
            "left": "10px",
            "display": "flex",
            "flexDirection": "column",
            "alignItems": "flex-start"}),
        
        html.Div([
            html.Div([html.H3("✅ Total Jobs Run"), html.P(str(int(total_jobs)))], className='card'),
            html.Div([html.H3("❌ Total Rejects"), html.P(str(int(total_rejects)))], className='card'),
            html.Div([html.H3("⏱️ Avg. Duration (sec)"), html.P(f"{avg_duration.total_seconds():.2f}" if pd.notnull(avg_duration) else "0.00")], className='card'),
            html.Div([html.H3("⚠️ Failed Jobs Count"), html.P(str(int(failed_jobs)))], className='card'),
        ], className='metric-container'),

        html.Div([
            html.Div([dcc.Graph(figure=bar_fig)], className='graph-half'),
            html.Div([dcc.Graph(figure=pie_fig)], className='graph-half'),
        ], className='graph-row'),

        html.Div([dcc.Graph(figure=gantt_fig)], className='graph-full graph-section'),
        html.Div([dcc.Graph(figure=line_fig)], className='graph-full graph-section'),

        dcc.Interval(id="daily-refresh", interval=24*60*60*1000, n_intervals=0)
    ])
