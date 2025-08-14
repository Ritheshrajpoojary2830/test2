#mkc
from __future__ import annotations
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials

# Your internal DB access module
import access as cr

# ------------------------------- CONFIG -------------------------------- #
SERVICE_ACCOUNT_FILE = r"C:\\Users\\rithe\\Downloads\\Field\\Field\\ringed-glass-460006-n9-c3952a96e429.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Source Google Sheet (for AgentDetails input)
AGENT_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1eaiF4Puux-aLB0m1zKAL4cM85rHHuriNghrp0evDvHA/edit?gid=0#gid=0"
)
AGENT_WORKSHEET_NAME = "AgentDetails"

# Destination Google Sheet (for outputs)
OUTPUT_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1eaiF4Puux-aLB0m1zKAL4cM85rHHuriNghrp0evDvHA/edit?gid=843302529#gid=843302529"
)
# Worksheet names to write
WS_DASHBOARD = "Dashboard"
WS_SUCCESSFUL = "Succesful"
WS_ADDR_NOT_FOUND = "Address Not Found"
WS_UNSUCCESSFUL = "Unsuccesful"
WS_ALL_PTP = "All PTP"

# Analytics DB (readonly) connection
ANALYTICS_DSN = "postgresql+psycopg2://postgres:Admin1@34.47.214.90:5432/analytics"

# Business constants
WORKSPACE = "crdhs"
EXCLUDED_INSTITUTES = ("SMFG_RURAL")
HARDCODED_ASSIGNER = "crdhs_9997034514"  # used in lead_payment query too

# Threading
MAX_WORKERS = 5

# ----------------------------- LOGGING SETUP ---------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fe-pipeline")

# ----------------------------- UTILITIES -------------------------------- #

def timer(fn):
    def _wrap(*args, **kwargs):
        t0 = time.time()
        out = fn(*args, **kwargs)
        log.info("%s took %.2fs", fn.__name__, time.time() - t0)
        return out
    return _wrap


def ensure_sql_tuple(values: Tuple[str, ...]) -> str:
    """Return a SQL-safe tuple string for IN (...) clause.
    Handles empty and single-element tuples correctly.
    """
    vals = tuple({str(v).strip() for v in values if str(v).strip() and str(v).strip().lower() != "nan"})
    if not vals:
        # Return a tuple that matches nothing
        return "('__NO_VALUES__')"
    if len(vals) == 1:
        return f"('{next(iter(vals))}',)"
    inside = ", ".join(f"'{v}'" for v in vals)
    return f"({inside})"


@timer
def auth_gspread() -> gspread.Client:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


@timer
def fetch_agent_codes(gc: gspread.Client) -> Tuple[str, ...]:
    sh = gc.open_by_url(AGENT_SHEET_URL)
    ws = sh.worksheet(AGENT_WORKSHEET_NAME)
    df = get_as_dataframe(ws, evaluate_formulas=True).dropna(how="all")
    if "agent_code" not in df.columns:
        raise KeyError("'agent_code' column missing in AgentDetails sheet")
    df["agent_code"] = df["agent_code"].astype(str).str.strip()
    codes = tuple(df.loc[df["agent_code"].notna() & (df["agent_code"] != "nan"), "agent_code"].unique())
    log.info("Loaded %d agent codes from sheet", len(codes))
    return codes


@timer
def parallel_fetch(agent_codes: Tuple[str, ...], analytics_engine) -> Dict[str, pd.DataFrame]:
    agent_codes_sql = ensure_sql_tuple(agent_codes)

    queries = {
        "df_user_assign": (
            f"""
            SELECT lu.lead_code, lu.user_code AS assigned_to, lu.updated_at AS assigned_date
            FROM lead_user_assignment AS lu
            JOIN lead AS l ON lu.lead_code = l.code
            WHERE lu.user_type = 'FIELD_AGENT'
              AND l.institute_name NOT IN {ensure_sql_tuple(EXCLUDED_INSTITUTES)}
              AND l.deleted = false
              AND lu.assigned_by = '{HARDCODED_ASSIGNER}'
            """,
            "main",
        ),
        "df_visit": (
            f"""
            SELECT v.lead_code, l.lan, l.principle, l.institute_name, v.date AS visited_date,
                   v.user_code AS agent_code, v.disposition AS visit_disposition,
                   v.payment_disposition AS visit_payment_disposition,
                   v.remarks AS visit_remarks, v.ptp_date AS ptp_date_by_visit,
                   v.ptp_amount AS ptp_amount_by_visit, v.followup_date AS visit_followup_date
            FROM visit AS v
            JOIN lead AS l ON v.lead_code = l.code
            WHERE v.deleted = false AND l.deleted = false
              AND l.workspace_code = '{WORKSPACE}'
              AND l.institute_name NOT IN {ensure_sql_tuple(EXCLUDED_INSTITUTES)}
              AND v.user_code IN {agent_codes_sql}
            """,
            "main",
        ),
        "df_payment": (
            f"""
            SELECT lp.lead_code,
                   SUM(lp.payment_amount) AS total_payment,
                   MAX(lp.payment_date) AS latest_payment_date,
                   SUM(CASE WHEN lp.payment_date = CURRENT_DATE THEN lp.payment_amount ELSE 0 END) AS today_payment
            FROM lead_payment AS lp
            JOIN lead AS l ON lp.lead_code = l.code
            WHERE lp.payment_upload_status = 'APPROVED'
              AND lp.user_code = '{HARDCODED_ASSIGNER}'
              AND lp.payment_date >= date_trunc('month', current_date)
              AND l.deleted = false
              AND l.institute_name NOT IN {ensure_sql_tuple(EXCLUDED_INSTITUTES)}
            GROUP BY lp.lead_code
            """,
            "main",
        ),
        "df_users": (
            f"""
            SELECT code AS agent_code, created_at AS joined_date, firstname, lastname, phone,
                   user_extra ->> 'city' AS city,
                   user_extra ->> 'state' AS state
            FROM users
            WHERE is_active = true AND NOT deleted
              AND workspace_code = '{WORKSPACE}'
            """,
            "main",
        ),
        "df_call": (
            f"""
            SELECT lead_code,
                   ptp_date   AS ptp_date_by_call,
                   ptp_amount AS ptp_amount_by_call,
                   followup_date AS call_followup_date,
                   date AS call_date,
                   payment_disposition AS call_payment_diposition
            FROM dialer_bot
            WHERE workspace_code = '{WORKSPACE}'
              AND date >= date_trunc('month', current_date)
              AND lead_code IN {agent_codes_sql}
              AND ptp_date IS NOT NULL
            """,
            "analytics",
        ),
    }

    results: Dict[str, pd.DataFrame] = {}

    def run_query(sql: str, which: str) -> pd.DataFrame:
        if which == "main":
            return cr.run_query(sql)
        elif which == "analytics":
            return pd.read_sql_query(sql, analytics_engine)
        else:
            raise ValueError("Unknown connection type")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fut_to_key = {ex.submit(run_query, q, conn): k for k, (q, conn) in queries.items()}
        for fut in as_completed(fut_to_key):
            k = fut_to_key[fut]
            df = fut.result()
            results[k] = df
            log.info("Fetched %s: %s rows", k, len(df))

    return results


# ---------------------------- TRANSFORM LOGIC --------------------------- #
@timer
def build_dashboard(results: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    today = pd.Timestamp.today().normalize()

    # --- Assignments ---
    df_latest_assign = (
        results["df_user_assign"].copy()
        .assign(assigned_date=lambda d: pd.to_datetime(d["assigned_date"], errors="coerce"))
        .sort_values(by="assigned_date", ascending=False)
        .drop_duplicates(subset="lead_code", keep="first")
    )

    # --- Visits (dedupe per lead per day) ---
    df_visit = results["df_visit"].copy()
    for col in ["visited_date", "ptp_date_by_visit", "visit_followup_date"]:
        if col in df_visit.columns:
            df_visit[col] = pd.to_datetime(df_visit[col], errors="coerce")
    df_visit["visit_day"] = df_visit["visited_date"].dt.date
    df_visit1 = (
        df_visit.sort_values("visited_date", ascending=False)
                .drop_duplicates(subset=["lead_code", "visit_day"], keep="first")
                .drop(columns=["visit_day"])
    )

    # --- Call (latest per lead) ---
    df_call = results["df_call"].copy()
    for col in ["ptp_date_by_call", "call_followup_date", "call_date"]:
        if col in df_call.columns:
            df_call[col] = pd.to_datetime(df_call[col], errors="coerce")
    df_latest_call = (
        df_call.sort_values(by="ptp_date_by_call", ascending=False)
               .drop_duplicates(subset="lead_code", keep="first")
    )

    # --- Merge visits + calls ---
    df_merge = pd.merge(df_visit1, df_latest_call, on="lead_code", how="left")

    # --- Payments (per lead) ---
    df_lead_pay = results["df_payment"].copy()
    if not df_lead_pay.empty:
        df_lead_pay["latest_payment_date"] = pd.to_datetime(df_lead_pay["latest_payment_date"], errors="coerce")
    else:
        df_lead_pay = pd.DataFrame(columns=["lead_code", "total_payment", "latest_payment_date", "today_payment"])  # keep schema

    # --- Users ---
    df_user = results["df_users"].copy()

    # -------------- Per-lead enrich -------------- #
    successful_dispo = ["MET_CUSTOMER", "MET_CUSTOMER_FAMILY_MEMBER"]
    df_merge["visit_status"] = np.where(df_merge["visit_disposition"].isin(successful_dispo), "Successful", "Unsuccessful")

    # Latest PTP by lead combining call + visit
    df_merge["ptp_date_by_visit"] = pd.to_datetime(df_merge["ptp_date_by_visit"], errors="coerce")
    df_merge["ptp_date_by_call"] = pd.to_datetime(df_merge["ptp_date_by_call"], errors="coerce")

    df_merge["latest_ptp_date"] = df_merge[["ptp_date_by_visit", "ptp_date_by_call"]].max(axis=1)
    df_merge["latest_ptp_amount"] = df_merge[["ptp_amount_by_visit", "ptp_amount_by_call"]].max(axis=1)

    # Latest record per lead for payment/ptp calcs
    df_latest_lead = df_merge.sort_values("visited_date").groupby("lead_code", as_index=False).tail(1)
    df_latest_lead = df_latest_lead.merge(df_lead_pay, on="lead_code", how="left")

    # Broken PTP flag
    df_latest_lead["broken_ptp"] = (
        (df_latest_lead["latest_ptp_date"] < today)
        & (df_latest_lead["total_payment"].fillna(0) == 0)
    )

    # -------------- Aggregations -------------- #
    visit_agg = (
        df_merge.groupby("agent_code").agg(
            total_visited_leads=("lead_code", "nunique"),
            repeat_visit=("lead_code", lambda x: int(x.count() - x.nunique())),
            successful_visit=("visit_status", lambda x: int((x == "Successful").sum())),
            unsuccessful_visit=("visit_status", lambda x: int((x == "Unsuccessful").sum())),
            last_working_day=("visited_date", "max"),
            today_visited_leads=("visited_date", lambda x: int((x.dt.normalize() == today).sum())),
            today_successful_visit=("visited_date", lambda x: int(((x.dt.normalize() == today) & (df_merge.loc[x.index, "visit_status"] == "Successful")).sum())),
            today_unsuccessful_visit=("visited_date", lambda x: int(((x.dt.normalize() == today) & (df_merge.loc[x.index, "visit_status"] == "Unsuccessful")).sum())),
        )
    ).reset_index()

    ptp_agg = (
        df_latest_lead.groupby("agent_code").agg(
            ptp_count=("latest_ptp_date", lambda x: int(x.notna().sum())),
            ptp_amount=("latest_ptp_amount", "sum"),
            broken_ptp=("broken_ptp", lambda x: int(x.sum())),
            broken_ptp_amount=("latest_ptp_amount", lambda s: s[df_latest_lead.loc[s.index, "broken_ptp"]].sum()),
            collection=("total_payment", "sum"),
            today_collection=("total_payment", lambda s: s[df_latest_lead.loc[s.index, "latest_payment_date"].dt.normalize() == today].sum()),
            today_ptp=("latest_ptp_date", lambda x: int(((x.dt.normalize() == today) & x.notna()).sum())),
            today_ptp_amount=("latest_ptp_amount", lambda s: s[df_latest_lead.loc[s.index, "latest_ptp_date"].dt.normalize() == today].sum()),
            future_ptp=("latest_ptp_date", lambda x: int(((x.dt.normalize() > today) & x.notna()).sum())),
            future_ptp_amount=("latest_ptp_amount", lambda s: s[df_latest_lead.loc[s.index, "latest_ptp_date"].dt.normalize() > today].sum()),
        )
    ).reset_index()

    principle_sum = (
        df_merge.groupby(["agent_code", "lead_code"])['principle']
                .first()
                .reset_index()
                .groupby("agent_code")["principle"].sum()
                .reset_index(name="principle")
    )

    assign_agg = (
        df_latest_assign.groupby("assigned_to").agg(
            total_assigned=("lead_code", "nunique"),
            today_assigned_leads=("assigned_date", lambda x: int((x.dt.normalize() == today).sum())),
        )
    ).reset_index()

    fe_dashboard = (
        visit_agg
        .merge(ptp_agg, on="agent_code", how="left")
        .merge(principle_sum, on="agent_code", how="left")
        .merge(assign_agg, left_on="agent_code", right_on="assigned_to", how="left")
        .drop(columns=["assigned_to"], errors="ignore")
        .merge(df_user, on="agent_code", how="left")
    )

    # Fill NA and post-process
    fe_dashboard["total_assigned"] = fe_dashboard["total_assigned"].fillna(0).astype(int)
    fe_dashboard["today_assigned_leads"] = fe_dashboard["today_assigned_leads"].fillna(0).astype(int)

    fe_dashboard["joined_date"] = pd.to_datetime(fe_dashboard["joined_date"], errors="coerce")
    fe_dashboard["joined_month"] = fe_dashboard["joined_date"].dt.to_period("M")
    current_month = today.to_period("M")
    fe_dashboard["months_diff"] = (current_month - fe_dashboard["joined_month"]).apply(lambda x: x.n if pd.notna(x) else 0)
    fe_dashboard["maturity_status"] = fe_dashboard["months_diff"].apply(lambda x: f"M+{x}" if x > 0 else "M")
    fe_dashboard["full_name"] = (fe_dashboard["firstname"].fillna("") + " " + fe_dashboard["lastname"].fillna("")).str.strip()
    fe_dashboard.drop(columns=["months_diff"], inplace=True, errors="ignore")

    # Pretty column case
    fe_dashboard.columns = [c[0].upper() + c[1:] if c else c for c in fe_dashboard.columns]

    desired_order = [
        'Agent_code', 'Full_name', 'Phone', 'City', 'State', 'Joined_date',
        'Maturity_status', 'Last_working_day', 'Total_assigned', 'Total_visited_leads', 'Principle', 'Repeat_visit',
        'Successful_visit', 'Unsuccessful_visit', 'Ptp_count', 'Ptp_amount', 'Broken_ptp', 'Broken_ptp_amount', 'Collection',
        'Today_assigned_leads', 'Today_visited_leads', 'Today_successful_visit', 'Today_unsuccessful_visit',
        'Today_ptp', 'Today_ptp_amount', 'Future_ptp', 'Future_ptp_amount', 'Today_collection'
    ]
    fe_dashboard = fe_dashboard.reindex(columns=desired_order)

    # ---------------- Lead-level breakouts ---------------- #
    lead_cols = ['lead_code', 'lan', 'visited_date', 'principle', 'institute_name', 'visit_remarks', 'agent_code', 'visit_disposition']

    successful_dispo = ['MET_CUSTOMER', 'MET_CUSTOMER_FAMILY_MEMBER']
    df_successful = df_merge[df_merge['visit_disposition'].isin(successful_dispo)][lead_cols].copy()
    df_address_not_found = df_merge[df_merge['visit_disposition'] == 'ADDRESS_NOT_FOUND'][lead_cols].copy()
    df_unsuccessful = df_merge[~df_merge['visit_disposition'].isin(successful_dispo + ['ADDRESS_NOT_FOUND'])][lead_cols].copy()

    # PTP status table
    df_ptp = df_merge.copy()
    df_ptp['latest_ptp_date'] = df_ptp[['ptp_date_by_visit', 'ptp_date_by_call']].max(axis=1)
    df_ptp['latest_ptp_amount'] = df_ptp[['ptp_amount_by_visit', 'ptp_amount_by_call']].max(axis=1)

    # bring in payments to compute BPTP vs paid
    df_ptp = df_ptp.merge(df_lead_pay[['lead_code', 'total_payment']].copy(), on='lead_code', how='left')

    def ptp_status(row):
        date = row['latest_ptp_date']
        if pd.isna(date):
            return np.nan
        if date.normalize() < today:
            return 'BPTP' if (row.get('total_payment', 0) in [0, np.nan, None] or pd.isna(row.get('total_payment'))) else 'FPTP'
        if date.normalize() == today:
            return 'TPTP'
        if date.normalize() == (today + pd.Timedelta(days=1)):
            return 'TOMPTP'
        return 'FPTP'

    df_ptp['ptp_status'] = df_ptp.apply(ptp_status, axis=1)
    df_ptp = df_ptp[df_ptp['ptp_status'].notna()]
    df_ptp = df_ptp[lead_cols + ['latest_ptp_date', 'latest_ptp_amount', 'ptp_status']]

    # Sort outputs for readability
    fe_dashboard = fe_dashboard.sort_values(['Maturity_status', 'Agent_code']).reset_index(drop=True)
    df_successful = df_successful.sort_values(['agent_code', 'visited_date', 'lead_code'], ascending=[True, False, True]).reset_index(drop=True)
    df_address_not_found = df_address_not_found.sort_values(['agent_code', 'visited_date', 'lead_code'], ascending=[True, False, True]).reset_index(drop=True)
    df_unsuccessful = df_unsuccessful.sort_values(['agent_code', 'visited_date', 'lead_code'], ascending=[True, False, True]).reset_index(drop=True)
    df_ptp = df_ptp.sort_values(['agent_code', 'latest_ptp_date', 'lead_code'], ascending=[True, True, True]).reset_index(drop=True)

    return fe_dashboard, df_successful, df_address_not_found, df_unsuccessful, df_ptp


# ------------------------------ OUTPUT --------------------------------- #
@timer
def upsert_and_write(gc: gspread.Client,
                     fe_dashboard: pd.DataFrame,
                     df_successful: pd.DataFrame,
                     df_address_not_found: pd.DataFrame,
                     df_unsuccessful: pd.DataFrame,
                     df_ptp: pd.DataFrame) -> None:
    sh = gc.open_by_url(OUTPUT_SHEET_URL)

    def get_or_create_ws(title: str):
        try:
            return sh.worksheet(title)
        except gspread.WorksheetNotFound:
            return sh.add_worksheet(title=title, rows="100", cols="26")

    def write_df(title: str, df: pd.DataFrame):
        ws = get_or_create_ws(title)
        ws.clear()
        set_with_dataframe(ws, df, row=1, col=1, include_index=False, include_column_header=True, resize=True)
        log.info("Wrote %s: %d rows, %d cols", title, df.shape[0], df.shape[1])

    write_df(WS_DASHBOARD, fe_dashboard)
    write_df(WS_SUCCESSFUL, df_successful)
    write_df(WS_ADDR_NOT_FOUND, df_address_not_found)
    write_df(WS_UNSUCCESSFUL, df_unsuccessful)
    write_df(WS_ALL_PTP, df_ptp)


# -------------------------------- MAIN ---------------------------------- #
@timer
def main():
    # Initialize connections
    log.info("Starting connections ...")
    cr.start_connection()
    analytics_engine = create_engine(ANALYTICS_DSN)

    # Google Sheets auth
    gc = auth_gspread()

    # Load agent codes from sheet
    agent_codes = fetch_agent_codes(gc)

    # Parallel fetch all required tables
    results = parallel_fetch(agent_codes, analytics_engine)

    # Build dashboards and detail tables
    fe_dashboard, df_successful, df_address_not_found, df_unsuccessful, df_ptp = build_dashboard(results)

    # Push to Google Sheets
    upsert_and_write(gc, fe_dashboard, df_successful, df_address_not_found, df_unsuccessful, df_ptp)

    log.info("Pipeline completed successfully ✔")


if __name__ == "__main__":
    main()
