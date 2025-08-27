#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Monitoring Solution for Informatica PowerCenter and Informatica Master Data Management

- Monitors PowerCenter services, workflows, and sessions
- Monitors MDM applications and ORS batch jobs
- Sends results to Microsoft Teams via webhooks

Refactored into a single executable script (no Jupyter dependencies).
"""

import os
import json
import pyodbc
import requests
import subprocess
import datetime
import schedule
import time
import platform
import urllib3
from requests.auth import HTTPDigestAuth
from dotenv import load_dotenv

# ------------------ Config ------------------
load_dotenv()

REQUIRED_VARS = [
    # PowerCenter
    "PMCMD_PATH", "BAT_DEV", "BAT_SIT", "BAT_PRD",

    # Teams Webhooks
    "WEBHOOK_POST", "WEBHOOK_CHAT",

    # SQL Server
    "DB_SERVER", "DB_SCHEMA_PC", "DB_USER_PC", "DB_PASS_PC",
    "DB_SCHEMA_MDM", "DB_USER_MDM", "DB_PASS_MDM",

    # JBoss SIT
    "SIT_JBOSS_URL", "SIT_JBOSS_USER", "SIT_JBOSS_PASS",

    # JBoss PRD
    "PRD_JBOSS_URL", "PRD_JBOSS_USER", "PRD_JBOSS_PASS",
]


def validate_env():
    """Validate that all required environment variables are set."""
    missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(
            f"❌ Missing required environment variables: {', '.join(missing_vars)}\n"
            f"➡️ Please check your .env file."
        )
    print("✅ All required environment variables are loaded.")


# Run validation at startup
validate_env()

# Configurations
PMCMD_PATH = os.getenv("PMCMD_PATH")  # reserved for future use
BAT_FILES = {
    "DEV": os.getenv("BAT_DEV"),
    "SIT": os.getenv("BAT_SIT"),
    "PRD": os.getenv("BAT_PRD"),
}

WEBHOOK_POST = os.getenv("WEBHOOK_POST")
WEBHOOK_CHAT = os.getenv("WEBHOOK_CHAT")

DB_SERVER = os.getenv("DB_SERVER")
DB_SCHEMA_PC = os.getenv("DB_SCHEMA_PC")
DB_USER_PC = os.getenv("DB_USER_PC")
DB_PASS_PC = os.getenv("DB_PASS_PC")
DB_SCHEMA_MDM = os.getenv("DB_SCHEMA_MDM")
DB_USER_MDM = os.getenv("DB_USER_MDM")
DB_PASS_MDM = os.getenv("DB_PASS_MDM")

ENVIRONMENTS = {
    "SIT": {
        "JBOSS_URL": os.getenv("SIT_JBOSS_URL"),
        "JBOSS_USER": os.getenv("SIT_JBOSS_USER"),
        "JBOSS_PASS": os.getenv("SIT_JBOSS_PASS"),
    },
    "PRD": {
        "JBOSS_URL": os.getenv("PRD_JBOSS_URL"),
        "JBOSS_USER": os.getenv("PRD_JBOSS_USER"),
        "JBOSS_PASS": os.getenv("PRD_JBOSS_PASS"),
    },
}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Job order
CUSTOM_ORDER = [
    "Party", "Party Relationship", "Party Source ID", "Party Status", "Party Postal Address",
    "Postal Address", "Party Electronic Address", "Party Phone Communication", "STG_PARTY",
    "STG_PARTY_REL", "Staging Party Source ID", "STG_PARTY_STATUS", "STG_PARTY_POSTAL_ADD",
    "STG_POSTAL_ADD", "STG_PARTY_ETRC_ADD", "STG_PARTY_PH_COMM", "STG_PARTY_WD",
    "STG_PARTY_REL_WD", "S_PARTY_SOURCE_ID_WD", "STG_PARTY_STATUS_WD", "STG_PARTY_PSTL_ADD_WD",
    "STG_PSTL_ADD_WD", "STG_PARTY_ERTC_ADD_WD", "STG_PARTY_PH_COMM_WD", "STG_PARTY_AD",
    "STG_PARTY_REL_AD", "S_PARTY_SOURCE_ID_AD", "STG_PARTY_STATUS_AD", "STG_PARTY_PSTL_ADD_AD",
    "STG_PSTL_ADD_AD", "STG_PARTY_ETRC_ADD_AD", "STG_PARTY_PH_COMM_AD"
]

# ------------------ Database ------------------

def connect_to_db(connection_type: str):
    """Create a pyodbc connection to PC or MDM database."""
    if connection_type == "pc":
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={DB_SERVER};DATABASE={DB_SCHEMA_PC};"
            f"UID={DB_USER_PC};PWD={DB_PASS_PC}"
        )
    elif connection_type == "mdm":
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={DB_SERVER};DATABASE={DB_SCHEMA_MDM};"
            f"UID={DB_USER_MDM};PWD={DB_PASS_MDM}"
        )
    else:
        raise ValueError("Invalid connection_type. Use 'pc' or 'mdm'.")
    return pyodbc.connect(conn_str)

# ------------------ PowerCenter Service Check ------------------

def check_pc_service():
    """Run environment .bat files and detect if the Integration Service is alive."""
    print("Checking PowerCenter services")
    status = {}
    for env, bat_file in BAT_FILES.items():
        try:
            result = subprocess.run(
                ["cmd", "/c", bat_file],   # run .bat via cmd
                capture_output=True,
                text=True,
                shell=False,
                timeout=30
            )
            output = (result.stdout or "") + (result.stderr or "")
            status[env] = "Integration Service is alive" in output
        except subprocess.TimeoutExpired:
            print(f"{env} - Timeout")
            status[env] = False
        except Exception as e:
            print(f"{env} - Error: {e}")
            status[env] = False
    return status

# ------------------ JBoss / MDM App Check ------------------

def check_mdm_apps():
    """Query JBoss management API to list deployments and their runtime status."""
    print("Checking Master Data Management apps")
    list_payload = {"operation": "read-children-names", "child-type": "deployment"}
    data = {}

    for env, creds in ENVIRONMENTS.items():
        deployments = []
        try:
            auth = HTTPDigestAuth(creds["JBOSS_USER"], creds["JBOSS_PASS"])
            headers = {"Content-Type": "application/json"}
            verify_ssl = env.upper() != "DEV"

            resp = requests.post(
                creds["JBOSS_URL"], auth=auth, headers=headers,
                data=json.dumps(list_payload), verify=verify_ssl, timeout=15
            )

            if resp.status_code != 200:
                print(f"{env} returned HTTP {resp.status_code}")
                data[env] = [{"Deployment": "N/A", "Status": "❌", "Enabled": "Not Reachable"}]
                continue

            for dep in resp.json().get("result", []):
                status_resp = requests.post(
                    creds["JBOSS_URL"], auth=auth, headers=headers,
                    data=json.dumps({
                        "operation": "read-resource",
                        "address": [{"deployment": dep}],
                        "include-runtime": "true"
                    }),
                    verify=verify_ssl, timeout=15
                )
                if status_resp.status_code == 200:
                    result = status_resp.json().get("result", {})
                    deployments.append({
                        "Deployment": dep,
                        "Status": "✅" if result.get("status") == "OK" else "❌",
                        "Enabled": "✅" if result.get("enabled") else "❌"
                    })
            data[env] = deployments if deployments else [{"Deployment": "N/A", "Status": "❌", "Enabled": "Not Reachable"}]

        except Exception as e:
            print(f"{env} error: {e}")
            data[env] = [{"Deployment": "N/A", "Status": "❌", "Enabled": "Not Reachable"}]

    return data

# ------------------ Workflows & Sessions ------------------

def get_recent_workflows_and_sessions():
    """Fetch recent PC workflows and sessions in a fixed window (yesterday 10 PM to midnight)."""
    print('Fetching PC workflows and sessions')
    conn = connect_to_db("pc")
    cursor = conn.cursor()

    now = datetime.datetime.now()
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_10pm = today_midnight - datetime.timedelta(hours=2)

    # Workflow query
    wf_query = """
    SELECT
        run.SUBJECT_AREA,
        run.WORKFLOW_NAME,
        run.WORKFLOW_RUN_ID,
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
        END AS Status
    FROM REP_WFLOW_RUN run
    WHERE run.SUBJECT_AREA = 'GLENCORE_HR_PROD'
      AND run.START_TIME BETWEEN ? AND ?
    """

    # Session query
    sess_query = """
    SELECT 
        SUBJECT_AREA,
        WORKFLOW_NAME,
        WORKFLOW_RUN_ID,
        SESSION_NAME,
        CASE RUN_STATUS_CODE
            WHEN 1 THEN 'Succeeded'
            WHEN 2 THEN 'Disabled'
            WHEN 3 THEN 'Failed'
            WHEN 4 THEN 'Stopped'
            WHEN 5 THEN 'Aborted'
            WHEN 6 THEN 'Running'
            WHEN 15 THEN 'Terminated'
            ELSE 'Unknown'
        END AS Status
    FROM REP_SESS_LOG
    WHERE SUBJECT_AREA = '<modify this>'
      AND ACTUAL_START BETWEEN ? AND ?
    """

    cursor.execute(wf_query, yesterday_10pm, today_midnight)
    workflows = cursor.fetchall()

    cursor.execute(sess_query, yesterday_10pm, today_midnight)
    sessions = cursor.fetchall()

    cursor.close()
    conn.close()

    return workflows, sessions

# ------------------ MDM Batch Jobs ------------------

def get_recent_jobs():
    """Fetch recent MDM jobs for selected job groups in the same time window."""
    print('Fetching MDM jobs')
    conn = connect_to_db("mdm")
    cursor = conn.cursor()

    now = datetime.datetime.now()
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_10pm = today_midnight - datetime.timedelta(hours=2)

    jobs_query = """
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
    AND jc.START_RUN_DATE >= ? AND jc.START_RUN_DATE < ?
    """

    cursor.execute(jobs_query, yesterday_10pm, today_midnight)
    jobs = cursor.fetchall()

    cursor.close()
    conn.close()

    return jobs

# ------------------ Formatting Helpers ------------------

def get_date_str():
    fmt = "%B %#d, %Y" if platform.system() == "Windows" else "%B %-d, %Y"
    return datetime.datetime.now().strftime(fmt)


def format_pc_chat(service_status, workflows, sessions, detailed=False):
    failed_wf = [wf for wf in workflows if wf.Status != 'Succeeded']
    failed_sess = [s for s in sessions if s.Status != 'Succeeded']
    env_lines = "\n".join([f"{env} {'✅' if up else '❌'}" for env, up in service_status.items()])

    summary = (
        f"{get_date_str()}\n\n"
        f"**🔍 PowerCenter Monitoring Summary**\n\n"
        f"**Service Status:**\n{env_lines}\n\n"
        f"**📦 Workflows and Sessions**\n\n"
        f"**Workflows Failed:** {len(failed_wf)} / {len(workflows)}\n\n"
        f"**Sessions Failed:** {len(failed_sess)} / {len(sessions)}\n\n"
    )

    if detailed:
        wf_lines = [f"{row.WORKFLOW_NAME} | {'✅' if row.Status == 'Succeeded' else '❌'}" for row in workflows]
        sess_lines = [f"{row.SESSION_NAME} | {'✅' if row.Status == 'Succeeded' else '❌'}" for row in sessions]
        summary += (
            f"📊 **Workflow List:**\n```\nWorkflow Name | Status\n"
            f"{chr(10).join(wf_lines)}\n```\n\n"
            f"📊 **Session List:**\n```\nSession Name | Status\n"
            f"{chr(10).join(sess_lines)}\n```"
        )
    return summary


def format_pc_summary(service_status, workflows, sessions):
    print('Formatting PC summary')
    failed_wf = [wf for wf in workflows if wf.Status != 'Succeeded']
    failed_sess = [s for s in sessions if s.Status != 'Succeeded']

    env_status_icon = lambda status: '✅' if status else '❌'
    wf_status_icon = lambda s: '✅' if s == 'Succeeded' else '❌'

    wf_lines = [f"{row.WORKFLOW_NAME} | {wf_status_icon(row.Status)}" for row in workflows]
    sess_lines = [f"{row.SESSION_NAME} | {wf_status_icon(row.Status)}" for row in sessions]

    service_lines = "\n".join([f"{env} {env_status_icon(up)}" for env, up in service_status.items()])

    date_format = "%B %#d, %Y" if platform.system() == "Windows" else "%B %-d, %Y"
    current_date = datetime.datetime.now().strftime(date_format)

    summary = (
        f"{current_date}\n\n"
        f"**🔍 PowerCenter Monitoring Summary**\n\n"
        f"**Service Status:**\n{service_lines}\n\n"
        f"**📦 Workflows and Sessions**\n\n"
        f"**Workflows\nFailed:** {len(failed_wf)} / {len(workflows)}\n\n"
        f"**Sessions\nFailed:** {len(failed_sess)} / {len(sessions)}\n\n"
        f"📊 **Workflow List:**\n"
        "```\n"
        "Workflow Name | Status\n"
        "-----------------------\n"
        f"{chr(10).join(wf_lines)}\n"
        "```\n\n"
        f"📊 **Session List:**\n"
        "```\n"
        "Session Name | Status\n"
        "-----------------------\n"
        f"{chr(10).join(sess_lines)}\n"
        "```"
    )

    return summary


def format_mdm_chat(jboss_data, jobs, detailed=False):
    status_icon = lambda s: '✅' if 'completed' in s.lower() else '❌'
    status_dict = {row[1]: status_icon(row[4]) for row in jobs}
    ordered_results = [f"{job} | {status_dict.get(job, '❌')}" for job in CUSTOM_ORDER]

    total, failed = len(jobs), sum(1 for row in jobs if 'completed' not in row[4].lower())
    env_lines, env_tables = [], []

    for env, deployments in jboss_data.items():
        ok = sum(1 for d in deployments if d["Status"] == "✅" and d["Enabled"] == "✅")
        fail = len(deployments) - ok
        env_lines.append(f"{env} {ok} ✅ | {fail} ❌")

        if detailed:
            lines = [f"{d['Deployment']} | {d['Status']} | {d['Enabled']}" for d in deployments]
            env_tables.append(
                f"**{env} Applications**\n```\nDeployment | Status | Enabled\n"
                f"{chr(10).join(lines)}\n```"
            )

    summary = (
        f"{get_date_str()}\n\n"
        "**🔍 MDM Monitoring Summary**\n\n"
        "**Services Status**\n" + "\n".join(env_lines) +
        f"\n\n**📦 Batch Jobs**\n\nFailed: {failed} / {total}\n\n"
    )

    if detailed:
        summary += (
            "\n".join(env_tables) +
            "\n\n```\nJob Name | Status\n" +
            f"{chr(10).join(ordered_results)}\n```"
        )
    return summary


def format_mdm_summary(jboss_data, jobs):
    print('Formatting MDM summary')

    status_icon = lambda s: '✅' if 'completed' in s.lower() else '❌'

    status_dict = {row[1]: status_icon(row[4]) for row in jobs}
    ordered_results = []
    for job in CUSTOM_ORDER:
        emoji = status_dict.get(job, '❌')
        ordered_results.append(f"{job} | {emoji}")

    total = len(jobs)
    failed = sum(1 for row in jobs if 'completed' not in row[4].lower())

    env_summary_lines = []
    env_tables = []

    for env, deployments in jboss_data.items():
        ok_count = sum(1 for d in deployments if d["Status"] == "✅" and d["Enabled"] == "✅")
        fail_count = len(deployments) - ok_count
        env_summary_lines.append(f"{env} {ok_count} ✅ | {fail_count} ❌")

        table_lines = [
            f"{d['Deployment']} | {d['Status']} | {d['Enabled']}"
            for d in deployments
        ]
        env_table = (
            f"**{env} Applications**\n"
            "```\n"
            "Deployment | Status | Enabled\n"
            "-------------------------------\n"
            f"{chr(10).join(table_lines)}\n"
            "```"
        )
        env_tables.append(env_table)

    date_format = "%B %#d, %Y" if platform.system() == "Windows" else "%B %-d, %Y"
    current_date = datetime.datetime.now().strftime(date_format)

    summary = (
        f"{current_date}\n\n"
        "**🔍 MDM Monitoring Summary**\n\n"
        "**Services Status**\n"
        + "\n".join(env_summary_lines) + "\n\n"
        + "\n\n".join(env_tables) + "\n\n"
        "**📦 Batch Jobs**\n\n"
        f"Failed: {failed} / {total}\n\n"
        "```\n"
        "Job Name | Status\n"
        "----------------------\n"
        f"{chr(10).join(ordered_results)}\n"
        "```\n\n"
    )

    return summary

# ------------------ Teams Messaging ------------------

def send_to_teams(webhook_url, message: str):
    print("Posting summary to Teams")
    resp = requests.post(webhook_url, json={"text": message})
    if resp.status_code != 200:
        print(f"❌ Teams post failed: {resp.status_code} - {resp.text}")

# ------------------ Main Orchestration ------------------

def monitor():
    """End-to-end monitoring run and posting to Teams."""
    print(f"\n📅 Running Monitoring at {datetime.datetime.now()}")
    try:
        pc_service = check_pc_service()
        mdm_apps = check_mdm_apps()
        workflows, sessions = get_recent_workflows_and_sessions()
        jobs = get_recent_jobs()

        # Chat-friendly summaries
        pc_chat = format_pc_chat(pc_service, workflows, sessions)
        mdm_chat = format_mdm_chat(mdm_apps, jobs)
        send_to_teams(WEBHOOK_CHAT, pc_chat)
        send_to_teams(WEBHOOK_CHAT, mdm_chat)

        # Detailed posts
        pc_summary = format_pc_summary(pc_service, workflows, sessions)
        mdm_summary = format_mdm_summary(mdm_apps, jobs)
        send_to_teams(WEBHOOK_POST, pc_summary)
        send_to_teams(WEBHOOK_POST, mdm_summary)

        print("✅ Monitoring complete and sent to Teams.")
    except Exception as e:
        print(f"❌ Error during monitoring: {e}")


# Schedule at 06:00 daily
schedule.clear()
schedule.every().day.at("06:00").do(monitor)


def run_scheduler():
    print("⏳ Scheduler started... waiting for 6:00 AM daily run.")
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    # Make the script executable directly
    run_scheduler()