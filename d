import json
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Health Domain Configuration", page_icon="🩺", layout="wide")
session = get_active_session()

ENVIRONMENT_ID = 1
HEALTH_DOMAIN_TABLE = "DSE_HEALTH_DOMAIN"
JOB_CONFIG_TABLE = "DSE_JOB_CONFIG"
TEST_PLAN_TABLE = "DSE_TESTPLAN"

CONFIG_COLUMNS = [
    "USECASE_ID", "USECASE_NAME", "USECASE_WEIGHT", "JOBID", "JOBNAME",
    "DSID", "TESTCASEDESCRIPTION", "DSID_WEIGHT", "CRITICAL"
]


def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=CONFIG_COLUMNS)


def init_state() -> None:
    defaults = {
        "configuration_rows": empty_df(),
        "loaded_health_area_id": None,
        "loaded_domain_id": None,
        "loaded_health_area_name": "",
        "loaded_domain_name": "",
        "loaded_domain_weight": 5,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


init_state()

st.markdown(
    """
    <style>
      .block-container {padding-top: 1.25rem; padding-bottom: 3rem;}
      .app-header {padding: 1.15rem 1.35rem; border: 1px solid rgba(128,128,128,.25);
                   border-radius: 14px; margin-bottom: 1rem;}
      .app-title {font-size: 2rem; font-weight: 700;}
      .app-subtitle {opacity: .75;}
      div[data-testid="stMetric"] {border:1px solid rgba(128,128,128,.25);
                                   padding:.75rem; border-radius:10px;}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=300, show_spinner=False)
def load_health_domains() -> pd.DataFrame:
    return session.sql(f"""
        SELECT
            HEALTH_AREA_ID::VARCHAR AS HEALTH_AREA_ID,
            COALESCE(HEALTH_AREA_NAME, '') AS HEALTH_AREA_NAME,
            DOMAIN_ID::VARCHAR AS DOMAIN_ID,
            COALESCE(DOMAIN_NAME, '') AS DOMAIN_NAME,
            COALESCE(
                TRY_TO_NUMBER(TO_VARCHAR(ASSOCIATED_JOBS:domain_weight)),
                5
            ) AS DOMAIN_WEIGHT
        FROM {HEALTH_DOMAIN_TABLE}
        ORDER BY HEALTH_AREA_NAME, DOMAIN_NAME
    """).to_pandas()


@st.cache_data(ttl=300, show_spinner=False)
def load_jobs() -> pd.DataFrame:
    return session.sql(
        f"""
        SELECT DISTINCT
            JOBID::NUMBER AS JOBID,
            COALESCE(JOBNAME, '') AS JOBNAME
        FROM {JOB_CONFIG_TABLE}
        WHERE ENVIRONMENT_ID = ?
          AND JOBID IS NOT NULL
          AND JOBNAME IS NOT NULL
        ORDER BY JOBNAME
        """,
        params=[ENVIRONMENT_ID],
    ).to_pandas()


@st.cache_data(ttl=300, show_spinner=False)
def load_tests_for_job(job_id: int) -> pd.DataFrame:
    return session.sql(
        f"""
        SELECT DISTINCT
            JOBID::NUMBER AS JOBID,
            DSID::VARCHAR AS DSID,
            COALESCE(TESTCASEDESCRIPTION, '') AS TESTCASEDESCRIPTION
        FROM {TEST_PLAN_TABLE}
        WHERE ENVIRONMENT_ID = ?
          AND JOBID = ?
          AND COALESCE(ACT_IND, 'Y') = 'Y'
          AND DSID IS NOT NULL
        ORDER BY DSID
        """,
        params=[ENVIRONMENT_ID, int(job_id)],
    ).to_pandas()


def load_existing_configuration(health_area_id: str, domain_id: str) -> tuple[int, pd.DataFrame]:
    result = session.sql(
        f"""
        SELECT ASSOCIATED_JOBS
        FROM {HEALTH_DOMAIN_TABLE}
        WHERE HEALTH_AREA_ID::VARCHAR = ?
          AND DOMAIN_ID::VARCHAR = ?
        """,
        params=[str(health_area_id), str(domain_id)],
    ).collect()

    if not result or result[0]["ASSOCIATED_JOBS"] is None:
        return 5, empty_df()

    raw = result[0]["ASSOCIATED_JOBS"]
    config = json.loads(raw) if isinstance(raw, str) else raw
    domain_weight = int(config.get("domain_weight", 5))

    jobs_df = load_jobs()
    job_name_by_id = {int(r["JOBID"]): str(r["JOBNAME"]) for _, r in jobs_df.iterrows()}
    test_desc_cache: dict[int, dict[str, str]] = {}
    records: list[dict[str, Any]] = []

    for usecase in config.get("usecases", []):
        usecase_id = str(usecase.get("usecase_id", ""))
        usecase_name = str(usecase.get("usecase_name", ""))
        usecase_weight = int(usecase.get("usecase_weight", 5))

        for job in usecase.get("jobs", []):
            job_id = int(job.get("jobid"))
            if job_id not in test_desc_cache:
                try:
                    tests = load_tests_for_job(job_id)
                    test_desc_cache[job_id] = {
                        str(r["DSID"]): str(r["TESTCASEDESCRIPTION"])
                        for _, r in tests.iterrows()
                    }
                except Exception:
                    test_desc_cache[job_id] = {}

            for test in job.get("tests", []):
                dsid = str(test.get("dsid", ""))
                records.append({
                    "USECASE_ID": usecase_id,
                    "USECASE_NAME": usecase_name,
                    "USECASE_WEIGHT": usecase_weight,
                    "JOBID": job_id,
                    "JOBNAME": job_name_by_id.get(job_id, ""),
                    "DSID": dsid,
                    "TESTCASEDESCRIPTION": test_desc_cache[job_id].get(dsid, ""),
                    "DSID_WEIGHT": int(test.get("weight", 5)),
                    "CRITICAL": bool(test.get("critical", False)),
                })

    return domain_weight, pd.DataFrame(records, columns=CONFIG_COLUMNS)


def normalize_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return empty_df()
    out = rows.copy()
    for c in ["USECASE_ID", "USECASE_NAME", "JOBNAME", "DSID", "TESTCASEDESCRIPTION"]:
        out[c] = out[c].fillna("").astype(str).str.strip()
    out["JOBID"] = pd.to_numeric(out["JOBID"], errors="coerce")
    out["USECASE_WEIGHT"] = pd.to_numeric(out["USECASE_WEIGHT"], errors="coerce").fillna(5).clip(1,10).astype(int)
    out["DSID_WEIGHT"] = pd.to_numeric(out["DSID_WEIGHT"], errors="coerce").fillna(5).clip(1,10).astype(int)
    out["CRITICAL"] = out["CRITICAL"].fillna(False).astype(bool)
    out = out[(out["USECASE_ID"] != "") & (out["USECASE_NAME"] != "") & out["JOBID"].notna() & (out["DSID"] != "")].copy()
    if not out.empty:
        out["JOBID"] = out["JOBID"].astype(int)
    return out


def validate_rows(rows: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    if rows.empty:
        return ["Add at least one DSID before saving."]
    missing = [c for c in CONFIG_COLUMNS if c not in rows.columns]
    if missing:
        return ["Missing required columns: " + ", ".join(missing)]
    if rows["USECASE_ID"].fillna("").astype(str).str.strip().eq("").any():
        errors.append("Use Case ID cannot be empty.")
    if rows["USECASE_NAME"].fillna("").astype(str).str.strip().eq("").any():
        errors.append("Use Case Name cannot be empty.")
    if rows["DSID"].fillna("").astype(str).str.strip().eq("").any():
        errors.append("DSID cannot be empty.")
    if pd.to_numeric(rows["JOBID"], errors="coerce").isna().any():
        errors.append("JOBID cannot be empty.")
    ucw = pd.to_numeric(rows["USECASE_WEIGHT"], errors="coerce")
    dsw = pd.to_numeric(rows["DSID_WEIGHT"], errors="coerce")
    if ucw.isna().any() or (~ucw.between(1,10)).any():
        errors.append("Use Case Weight must be between 1 and 10.")
    if dsw.isna().any() or (~dsw.between(1,10)).any():
        errors.append("DSID Weight must be between 1 and 10.")
    if rows.duplicated(subset=["USECASE_ID", "JOBID", "DSID"], keep=False).any():
        errors.append("Duplicate DSIDs exist for the same Use Case and Job.")
    consistency = rows.groupby("USECASE_ID").agg(
        NAME_COUNT=("USECASE_NAME", "nunique"),
        WEIGHT_COUNT=("USECASE_WEIGHT", "nunique")
    )
    if ((consistency["NAME_COUNT"] > 1) | (consistency["WEIGHT_COUNT"] > 1)).any():
        errors.append("A Use Case ID must have one consistent name and weight.")
    return errors


def build_json(domain_weight: int, rows: pd.DataFrame) -> dict[str, Any]:
    rows = normalize_rows(rows)
    result: dict[str, Any] = {"domain_weight": int(domain_weight), "usecases": []}
    for (uc_id, uc_name, uc_weight), uc_rows in rows.groupby(
        ["USECASE_ID", "USECASE_NAME", "USECASE_WEIGHT"], sort=False
    ):
        uc_obj: dict[str, Any] = {
            "usecase_id": str(uc_id),
            "usecase_name": str(uc_name),
            "usecase_weight": int(uc_weight),
            "jobs": [],
        }
        for job_id, job_rows in uc_rows.groupby("JOBID", sort=False):
            tests = [
                {
                    "dsid": str(r["DSID"]),
                    "weight": int(r["DSID_WEIGHT"]),
                    "critical": bool(r["CRITICAL"]),
                }
                for _, r in job_rows.iterrows()
            ]
            uc_obj["jobs"].append({"jobid": int(job_id), "tests": tests})
        result["usecases"].append(uc_obj)
    return result


def save_configuration(health_area_id: str, health_area_name: str, domain_id: str, domain_name: str, payload: dict[str, Any]) -> None:
    payload_text = json.dumps(payload, separators=(",", ":"))
    sql = f"""
        MERGE INTO {HEALTH_DOMAIN_TABLE} TARGET
        USING (
            SELECT
                ?::VARCHAR AS HEALTH_AREA_ID,
                ?::VARCHAR AS HEALTH_AREA_NAME,
                ?::VARCHAR AS DOMAIN_ID,
                ?::VARCHAR AS DOMAIN_NAME,
                PARSE_JSON(?) AS ASSOCIATED_JOBS
        ) SOURCE
          ON TARGET.HEALTH_AREA_ID::VARCHAR = SOURCE.HEALTH_AREA_ID
         AND TARGET.DOMAIN_ID::VARCHAR = SOURCE.DOMAIN_ID
        WHEN MATCHED THEN UPDATE SET
            TARGET.HEALTH_AREA_NAME = SOURCE.HEALTH_AREA_NAME,
            TARGET.DOMAIN_NAME = SOURCE.DOMAIN_NAME,
            TARGET.ASSOCIATED_JOBS = SOURCE.ASSOCIATED_JOBS,
            TARGET.UPDATED_ON = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            HEALTH_AREA_ID, HEALTH_AREA_NAME, DOMAIN_ID, DOMAIN_NAME,
            ASSOCIATED_JOBS, CREATED_BY, CREATED_ON, UPDATED_ON
        ) VALUES (
            SOURCE.HEALTH_AREA_ID, SOURCE.HEALTH_AREA_NAME,
            SOURCE.DOMAIN_ID, SOURCE.DOMAIN_NAME,
            SOURCE.ASSOCIATED_JOBS, CURRENT_USER(),
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
    """
    session.sql("BEGIN").collect()
    try:
        session.sql(sql, params=[health_area_id, health_area_name, domain_id, domain_name, payload_text]).collect()
        session.sql("COMMIT").collect()
    except Exception:
        session.sql("ROLLBACK").collect()
        raise


try:
    health_domains_df = load_health_domains()
    jobs_df = load_jobs()
except Exception as exc:
    st.error("Unable to load Snowflake configuration tables.")
    st.exception(exc)
    st.stop()

st.markdown(
    """
    <div class="app-header">
      <div class="app-title">Health Domain Configuration</div>
      <div class="app-subtitle">Business users can configure use cases, jobs, DSIDs, weights and critical indicators without writing JSON.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Navigation")
    page = st.radio("Page", ["Configuration Builder", "Existing Configurations"], label_visibility="collapsed")
    st.divider()
    st.caption(f"Environment ID: {ENVIRONMENT_ID}")
    st.caption(f"Loaded: {datetime.now():%Y-%m-%d %H:%M}")

if page == "Configuration Builder":
    build_tab, save_tab = st.tabs(["Build Configuration", "Review and Save"])

    with build_tab:
        st.subheader("1. Select Health Area and Domain")
        c1, c2 = st.columns(2)
        health_area_options = health_domains_df[["HEALTH_AREA_ID", "HEALTH_AREA_NAME"]].drop_duplicates().sort_values("HEALTH_AREA_NAME")
        health_area_labels = {
            f"{r['HEALTH_AREA_NAME']} ({r['HEALTH_AREA_ID']})": str(r["HEALTH_AREA_ID"])
            for _, r in health_area_options.iterrows()
        }
        with c1:
            health_area_label = st.selectbox("Health Area", list(health_area_labels), index=None, placeholder="Select a health area")
        health_area_id = health_area_labels.get(health_area_label) if health_area_label else None

        if health_area_id:
            area_rows = health_domains_df[health_domains_df["HEALTH_AREA_ID"].astype(str) == str(health_area_id)]
            health_area_name = str(area_rows.iloc[0]["HEALTH_AREA_NAME"])
            domain_labels = {
                f"{r['DOMAIN_NAME']} ({r['DOMAIN_ID']})": str(r["DOMAIN_ID"])
                for _, r in area_rows.iterrows()
            }
        else:
            health_area_name = ""
            domain_labels = {}

        with c2:
            domain_label = st.selectbox("Domain", list(domain_labels), index=None, placeholder="Select a domain", disabled=not health_area_id)
        domain_id = domain_labels.get(domain_label) if domain_label else None
        domain_name = ""
        if domain_id:
            domain_name = str(
                health_domains_df[
                    (health_domains_df["HEALTH_AREA_ID"].astype(str) == str(health_area_id)) &
                    (health_domains_df["DOMAIN_ID"].astype(str) == str(domain_id))
                ].iloc[0]["DOMAIN_NAME"]
            )

        b1, b2, _ = st.columns([1,1,3])
        with b1:
            load_btn = st.button("Load Existing", use_container_width=True, disabled=not (health_area_id and domain_id))
        with b2:
            clear_btn = st.button("Clear Working Copy", use_container_width=True)

        if clear_btn:
            st.session_state.configuration_rows = empty_df()
            st.session_state.loaded_health_area_id = None
            st.session_state.loaded_domain_id = None
            st.session_state.loaded_health_area_name = ""
            st.session_state.loaded_domain_name = ""
            st.session_state.loaded_domain_weight = 5
            st.rerun()

        if load_btn:
            try:
                weight, rows = load_existing_configuration(str(health_area_id), str(domain_id))
                st.session_state.configuration_rows = rows
                st.session_state.loaded_health_area_id = str(health_area_id)
                st.session_state.loaded_domain_id = str(domain_id)
                st.session_state.loaded_health_area_name = health_area_name
                st.session_state.loaded_domain_name = domain_name
                st.session_state.loaded_domain_weight = weight
                st.success("Configuration loaded.")
                st.rerun()
            except Exception as exc:
                st.error("Unable to load configuration.")
                st.exception(exc)

        st.divider()
        st.subheader("2. Domain Weight")
        st.session_state.loaded_domain_weight = st.slider(
            "Domain Weight", 1, 10, int(st.session_state.loaded_domain_weight),
            help="Importance of this domain when calculating the Health Area Score."
        )

        st.divider()
        st.subheader("3. Add Use Case, Job and DSIDs")
        with st.form("add_form"):
            f1, f2, f3 = st.columns([1,2,1])
            with f1:
                usecase_id = st.text_input("Use Case ID", placeholder="UC201")
            with f2:
                usecase_name = st.text_input("Use Case Name", placeholder="Members")
            with f3:
                usecase_weight = st.number_input("Use Case Weight", 1, 10, 5, 1)

            job_labels = {f"{r['JOBNAME']} ({int(r['JOBID'])})": int(r["JOBID"]) for _, r in jobs_df.iterrows()}
            job_label = st.selectbox("Job", list(job_labels), index=None, placeholder="Select a job")
            job_id = job_labels.get(job_label) if job_label else None
            job_name = job_label.rsplit(" (",1)[0] if job_label else ""

            tests_df = pd.DataFrame()
            selected_dsids: list[str] = []
            if job_id:
                try:
                    tests_df = load_tests_for_job(job_id)
                    test_labels = {
                        f"{r['DSID']} — {r['TESTCASEDESCRIPTION']}": str(r["DSID"])
                        for _, r in tests_df.iterrows()
                    }
                    chosen = st.multiselect("DSIDs", list(test_labels), placeholder="Select one or more DSIDs")
                    selected_dsids = [test_labels[x] for x in chosen]
                    if tests_df.empty:
                        st.info("No active DSIDs found for this job.")
                except Exception as exc:
                    st.error("Unable to load DSIDs.")
                    st.exception(exc)

            dsid_weight = st.number_input("Default DSID Weight", 1, 10, 5, 1)
            critical = st.checkbox("Mark selected DSIDs as critical")
            add_btn = st.form_submit_button("Add Selected DSIDs", use_container_width=True)

        if add_btn:
            errors = []
            if not usecase_id.strip(): errors.append("Enter a Use Case ID.")
            if not usecase_name.strip(): errors.append("Enter a Use Case Name.")
            if job_id is None: errors.append("Select a Job.")
            if not selected_dsids: errors.append("Select at least one DSID.")
            if errors:
                for e in errors: st.error(e)
            else:
                desc = {str(r["DSID"]): str(r["TESTCASEDESCRIPTION"]) for _, r in tests_df.iterrows()}
                new_rows = pd.DataFrame([
                    {
                        "USECASE_ID": usecase_id.strip(),
                        "USECASE_NAME": usecase_name.strip(),
                        "USECASE_WEIGHT": int(usecase_weight),
                        "JOBID": int(job_id),
                        "JOBNAME": job_name,
                        "DSID": dsid,
                        "TESTCASEDESCRIPTION": desc.get(dsid, ""),
                        "DSID_WEIGHT": int(dsid_weight),
                        "CRITICAL": bool(critical),
                    }
                    for dsid in selected_dsids
                ], columns=CONFIG_COLUMNS)
                st.session_state.configuration_rows = pd.concat(
                    [st.session_state.configuration_rows, new_rows], ignore_index=True
                )
                st.success(f"Added {len(new_rows)} DSID(s).")
                st.rerun()

        st.divider()
        st.subheader("4. Review and Edit Working Configuration")
        if st.session_state.configuration_rows.empty:
            st.info("No DSIDs have been added yet.")
        else:
            edited = st.data_editor(
                st.session_state.configuration_rows,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                column_config={
                    "USECASE_ID": st.column_config.TextColumn("Use Case ID", required=True),
                    "USECASE_NAME": st.column_config.TextColumn("Use Case Name", required=True),
                    "USECASE_WEIGHT": st.column_config.NumberColumn("Use Case Weight", min_value=1, max_value=10, step=1, required=True),
                    "JOBID": st.column_config.NumberColumn("Job ID", disabled=True),
                    "JOBNAME": st.column_config.TextColumn("Job Name", disabled=True),
                    "DSID": st.column_config.TextColumn("DSID", disabled=True),
                    "TESTCASEDESCRIPTION": st.column_config.TextColumn("Test Description", disabled=True, width="large"),
                    "DSID_WEIGHT": st.column_config.NumberColumn("DSID Weight", min_value=1, max_value=10, step=1, required=True),
                    "CRITICAL": st.column_config.CheckboxColumn("Critical", default=False),
                },
                key="config_editor",
            )
            st.session_state.configuration_rows = edited
            m1, m2, m3 = st.columns(3)
            m1.metric("Use Cases", edited["USECASE_ID"].nunique())
            m2.metric("Jobs", edited["JOBID"].nunique())
            m3.metric("DSIDs", len(edited))

    with save_tab:
        st.subheader("Review Generated Configuration")
        effective_area_id = health_area_id or st.session_state.loaded_health_area_id
        effective_domain_id = domain_id or st.session_state.loaded_domain_id
        effective_area_name = health_area_name or st.session_state.loaded_health_area_name
        effective_domain_name = domain_name or st.session_state.loaded_domain_name

        if not effective_area_id or not effective_domain_id:
            st.warning("Select a Health Area and Domain on the Build Configuration tab.")
        else:
            errors = validate_rows(st.session_state.configuration_rows)
            if errors:
                st.error("Resolve these issues before saving:")
                for e in errors: st.write(f"• {e}")
            else:
                payload = build_json(st.session_state.loaded_domain_weight, st.session_state.configuration_rows)
                s1, s2, s3 = st.columns(3)
                s1.metric("Health Area", effective_area_name)
                s2.metric("Domain", effective_domain_name)
                s3.metric("Domain Weight", st.session_state.loaded_domain_weight)
                st.dataframe(st.session_state.configuration_rows, use_container_width=True, hide_index=True)
                with st.expander("Generated JSON preview"):
                    st.json(payload)
                confirm = st.checkbox("I reviewed this configuration and want to save it.")
                if st.button("Save to DSE_HEALTH_DOMAIN", type="primary", use_container_width=True, disabled=not confirm):
                    try:
                        save_configuration(
                            str(effective_area_id), str(effective_area_name),
                            str(effective_domain_id), str(effective_domain_name), payload
                        )
                        load_health_domains.clear()
                        st.success("Configuration saved successfully.")
                    except Exception as exc:
                        st.error("Unable to save configuration.")
                        st.exception(exc)

else:
    st.subheader("Existing Health Domain Configurations")
    f1, f2 = st.columns(2)
    with f1:
        area_filter = st.multiselect("Filter by Health Area", sorted(health_domains_df["HEALTH_AREA_NAME"].dropna().unique().tolist()))
    with f2:
        domain_filter = st.multiselect("Filter by Domain", sorted(health_domains_df["DOMAIN_NAME"].dropna().unique().tolist()))
    filtered = health_domains_df.copy()
    if area_filter:
        filtered = filtered[filtered["HEALTH_AREA_NAME"].isin(area_filter)]
    if domain_filter:
        filtered = filtered[filtered["DOMAIN_NAME"].isin(domain_filter)]
    st.dataframe(filtered, use_container_width=True, hide_index=True)
    st.caption("Use Configuration Builder to load and edit a domain.")
