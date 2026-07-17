import json
import re
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session


# ============================================================
# APP SETTINGS
# ============================================================

st.set_page_config(
    page_title="Health Configuration",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="collapsed",
)

session = get_active_session()

ENVIRONMENT_ID = "1"

# Replace with fully qualified names when required.
HEALTH_DOMAIN_TABLE = "DSE_HEALTH_DOMAIN"
JOB_CONFIG_TABLE = "DSE_JOB_CONFIG"
TEST_PLAN_TABLE = "DSE_TESTPLAN"


# ============================================================
# STYLE
# ============================================================

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1rem;
            padding-bottom: 3rem;
        }
        .app-title {
            font-size: 2rem;
            font-weight: 760;
            margin-bottom: 0.15rem;
        }
        .app-subtitle {
            opacity: 0.68;
            margin-bottom: 0.9rem;
        }
        .context-card {
            border: 1px solid rgba(128, 128, 128, 0.25);
            border-radius: 14px;
            padding: 0.8rem 1rem;
            margin-bottom: 0.9rem;
            background: rgba(128, 128, 128, 0.035);
        }
        .context-title {
            font-size: 1.1rem;
            font-weight: 700;
        }
        .context-subtitle {
            opacity: 0.68;
            margin-top: 0.1rem;
        }
        div[data-testid="stMetric"] {
            border: 1px solid rgba(128, 128, 128, 0.24);
            border-radius: 12px;
            padding: 0.75rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# DATA STRUCTURES
# ============================================================

USECASE_COLUMNS = [
    "USECASE_ID",
    "USECASE_NAME",
    "USECASE_WEIGHT",
]

TEST_CONFIG_COLUMNS = [
    "USECASE_ID",
    "JOBID",
    "JOBNAME",
    "DSID",
    "TESTCASEDESCRIPTION",
    "DSID_WEIGHT",
    "CRITICAL",
]


def empty_usecases_dataframe() -> pd.DataFrame:
    return pd.DataFrame(columns=USECASE_COLUMNS)


def empty_tests_dataframe() -> pd.DataFrame:
    return pd.DataFrame(columns=TEST_CONFIG_COLUMNS)


# ============================================================
# SESSION STATE
# ============================================================


def initialize_session_state() -> None:
    defaults: dict[str, Any] = {
        "page": "home",
        "loaded_context_key": None,
        "active_health_area_id": None,
        "active_health_area_name": "",
        "active_domain_id": None,
        "active_domain_name": "",
        "domain_weight": 5,
        "usecases_df": empty_usecases_dataframe(),
        "tests_df": empty_tests_dataframe(),
        "selected_usecase_id": None,
        "selected_job_id": None,
        "editor_revision": 0,
        "new_usecase_revision": 0,
        "last_saved_at": None,
        "flash_message": None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


initialize_session_state()


# ============================================================
# GENERAL HELPERS
# ============================================================


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return clean_text(value).lower() in {"true", "1", "yes", "y", "t"}


def parse_variant_object(raw_value: Any) -> dict[str, Any]:
    if raw_value is None:
        return {}
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str):
        return json.loads(raw_value)
    return json.loads(str(raw_value))


def normalize_usecases(usecases_df: pd.DataFrame) -> pd.DataFrame:
    if usecases_df is None or usecases_df.empty:
        return empty_usecases_dataframe()

    rows = usecases_df.copy()
    for column_name in USECASE_COLUMNS:
        if column_name not in rows.columns:
            rows[column_name] = None

    rows["USECASE_ID"] = rows["USECASE_ID"].fillna("").astype(str).str.strip()
    rows["USECASE_NAME"] = rows["USECASE_NAME"].fillna("").astype(str).str.strip()
    rows["USECASE_WEIGHT"] = (
        pd.to_numeric(rows["USECASE_WEIGHT"], errors="coerce")
        .fillna(5)
        .clip(1, 10)
        .astype(int)
    )

    rows = rows[rows["USECASE_ID"] != ""].copy()
    rows = rows.drop_duplicates(subset=["USECASE_ID"], keep="last")
    return rows[USECASE_COLUMNS].reset_index(drop=True)


def normalize_tests(tests_df: pd.DataFrame) -> pd.DataFrame:
    if tests_df is None or tests_df.empty:
        return empty_tests_dataframe()

    rows = tests_df.copy()
    for column_name in TEST_CONFIG_COLUMNS:
        if column_name not in rows.columns:
            rows[column_name] = None

    for column_name in [
        "USECASE_ID",
        "JOBID",
        "JOBNAME",
        "DSID",
        "TESTCASEDESCRIPTION",
    ]:
        rows[column_name] = rows[column_name].fillna("").astype(str).str.strip()

    rows["DSID_WEIGHT"] = (
        pd.to_numeric(rows["DSID_WEIGHT"], errors="coerce")
        .fillna(5)
        .clip(1, 10)
        .astype(int)
    )
    rows["CRITICAL"] = rows["CRITICAL"].apply(coerce_bool)

    rows = rows[
        (rows["USECASE_ID"] != "")
        & (rows["JOBID"] != "")
        & (rows["DSID"] != "")
    ].copy()

    rows = rows.drop_duplicates(
        subset=["USECASE_ID", "JOBID", "DSID"],
        keep="last",
    )
    return rows[TEST_CONFIG_COLUMNS].reset_index(drop=True)


def next_editor_revision() -> None:
    st.session_state.editor_revision += 1


def set_flash(message: str) -> None:
    st.session_state.flash_message = message


def show_flash() -> None:
    message = st.session_state.pop("flash_message", None)
    if message:
        st.success(message)


def format_saved_time() -> str:
    saved_at = st.session_state.last_saved_at
    if not saved_at:
        return ""
    return saved_at.strftime("%I:%M:%S %p")


# ============================================================
# DATABASE READS
# ============================================================


@st.cache_data(ttl=120, show_spinner=False)
def load_health_domains() -> pd.DataFrame:
    query = f"""
        SELECT
            TRIM(TO_VARCHAR(HEALTH_AREA_ID)) AS HEALTH_AREA_ID,
            COALESCE(TRIM(TO_VARCHAR(HEALTH_AREA_NAME)), '') AS HEALTH_AREA_NAME,
            TRIM(TO_VARCHAR(DOMAIN_ID)) AS DOMAIN_ID,
            COALESCE(TRIM(TO_VARCHAR(DOMAIN_NAME)), '') AS DOMAIN_NAME,
            COALESCE(
                TRY_TO_NUMBER(TO_VARCHAR(ASSOCIATED_JOBS:domain_weight)),
                5
            ) AS DOMAIN_WEIGHT
        FROM {HEALTH_DOMAIN_TABLE}
        ORDER BY HEALTH_AREA_NAME, DOMAIN_NAME
    """
    return session.sql(query).to_pandas()


@st.cache_data(ttl=120, show_spinner=False)
def load_jobs() -> pd.DataFrame:
    query = f"""
        SELECT DISTINCT
            TRIM(TO_VARCHAR(JOBID)) AS JOBID,
            COALESCE(TRIM(TO_VARCHAR(JOBNAME)), '') AS JOBNAME
        FROM {JOB_CONFIG_TABLE}
        WHERE TRIM(TO_VARCHAR(ENVIRONMENT_ID)) = ?
          AND NULLIF(TRIM(TO_VARCHAR(JOBID)), '') IS NOT NULL
          AND NULLIF(TRIM(TO_VARCHAR(JOBNAME)), '') IS NOT NULL
        ORDER BY JOBNAME
    """
    return session.sql(query, params=[ENVIRONMENT_ID]).to_pandas()


@st.cache_data(ttl=60, show_spinner=False)
def load_tests_for_job(job_id: str) -> pd.DataFrame:
    selected_job_id = clean_text(job_id)
    query = f"""
        SELECT DISTINCT
            TRIM(TO_VARCHAR(JOBID)) AS JOBID,
            TRIM(TO_VARCHAR(DSID)) AS DSID,
            COALESCE(TRIM(TO_VARCHAR(TESTCASEDESCRIPTION)), '')
                AS TESTCASEDESCRIPTION
        FROM {TEST_PLAN_TABLE}
        WHERE
            (
                UPPER(TRIM(TO_VARCHAR(JOBID))) = UPPER(TRIM(?))
                OR
                (
                    TRY_TO_NUMBER(TRIM(TO_VARCHAR(JOBID))) IS NOT NULL
                    AND TRY_TO_NUMBER(TRIM(?)) IS NOT NULL
                    AND TRY_TO_NUMBER(TRIM(TO_VARCHAR(JOBID))) =
                        TRY_TO_NUMBER(TRIM(?))
                )
            )
          AND NULLIF(TRIM(TO_VARCHAR(DSID)), '') IS NOT NULL
        ORDER BY DSID
    """
    return session.sql(
        query,
        params=[selected_job_id, selected_job_id, selected_job_id],
    ).to_pandas()


def load_saved_configuration_rows() -> pd.DataFrame:
    """Always query the table so Analysis never shows stale counts."""
    query = f"""
        SELECT
            TRIM(TO_VARCHAR(HEALTH_AREA_ID)) AS HEALTH_AREA_ID,
            COALESCE(TRIM(TO_VARCHAR(HEALTH_AREA_NAME)), '') AS HEALTH_AREA_NAME,
            TRIM(TO_VARCHAR(DOMAIN_ID)) AS DOMAIN_ID,
            COALESCE(TRIM(TO_VARCHAR(DOMAIN_NAME)), '') AS DOMAIN_NAME,
            ASSOCIATED_JOBS
        FROM {HEALTH_DOMAIN_TABLE}
        ORDER BY HEALTH_AREA_NAME, DOMAIN_NAME
    """
    return session.sql(query).to_pandas()


def load_existing_configuration(
    health_area_id: str,
    domain_id: str,
    job_name_by_id: dict[str, str],
) -> tuple[int, pd.DataFrame, pd.DataFrame]:
    query = f"""
        SELECT ASSOCIATED_JOBS
        FROM {HEALTH_DOMAIN_TABLE}
        WHERE TRIM(TO_VARCHAR(HEALTH_AREA_ID)) = ?
          AND TRIM(TO_VARCHAR(DOMAIN_ID)) = ?
    """

    result = session.sql(
        query,
        params=[clean_text(health_area_id), clean_text(domain_id)],
    ).collect()

    if not result or result[0]["ASSOCIATED_JOBS"] is None:
        return 5, empty_usecases_dataframe(), empty_tests_dataframe()

    configuration = parse_variant_object(result[0]["ASSOCIATED_JOBS"])
    domain_weight = int(configuration.get("domain_weight", 5))

    usecase_records: list[dict[str, Any]] = []
    test_records: list[dict[str, Any]] = []
    description_cache: dict[str, dict[str, str]] = {}

    for usecase in configuration.get("usecases", []) or []:
        usecase_id = clean_text(usecase.get("usecase_id"))
        if not usecase_id:
            continue

        usecase_records.append(
            {
                "USECASE_ID": usecase_id,
                "USECASE_NAME": clean_text(usecase.get("usecase_name")),
                "USECASE_WEIGHT": int(usecase.get("usecase_weight", 5)),
            }
        )

        for job in usecase.get("jobs", []) or []:
            job_id = clean_text(job.get("jobid"))
            if not job_id:
                continue

            if job_id not in description_cache:
                try:
                    source_df = load_tests_for_job(job_id)
                    description_cache[job_id] = {
                        clean_text(row["DSID"]): clean_text(
                            row["TESTCASEDESCRIPTION"]
                        )
                        for _, row in source_df.iterrows()
                    }
                except Exception:
                    description_cache[job_id] = {}

            for test in job.get("tests", []) or []:
                dsid = clean_text(test.get("dsid"))
                if not dsid:
                    continue

                test_records.append(
                    {
                        "USECASE_ID": usecase_id,
                        "JOBID": job_id,
                        "JOBNAME": job_name_by_id.get(job_id, ""),
                        "DSID": dsid,
                        "TESTCASEDESCRIPTION": description_cache[job_id].get(
                            dsid, ""
                        ),
                        "DSID_WEIGHT": int(test.get("weight", 5)),
                        "CRITICAL": coerce_bool(test.get("critical", False)),
                    }
                )

    return (
        domain_weight,
        normalize_usecases(pd.DataFrame(usecase_records)),
        normalize_tests(pd.DataFrame(test_records)),
    )


# ============================================================
# JSON AND DATABASE SAVE
# ============================================================


def build_associated_jobs_json(
    domain_weight: int,
    usecases_df: pd.DataFrame,
    tests_df: pd.DataFrame,
) -> dict[str, Any]:
    usecases = normalize_usecases(usecases_df)
    tests = normalize_tests(tests_df)

    configuration: dict[str, Any] = {
        "domain_weight": int(domain_weight),
        "usecases": [],
    }

    for _, usecase_row in usecases.iterrows():
        usecase_id = clean_text(usecase_row["USECASE_ID"])
        usecase_tests = tests[tests["USECASE_ID"] == usecase_id]

        usecase_object: dict[str, Any] = {
            "usecase_id": usecase_id,
            "usecase_name": clean_text(usecase_row["USECASE_NAME"]),
            "usecase_weight": int(usecase_row["USECASE_WEIGHT"]),
            "jobs": [],
        }

        for job_id, job_rows in usecase_tests.groupby(
            "JOBID", sort=False, dropna=False
        ):
            job_object = {
                "jobid": clean_text(job_id),
                "tests": [],
            }

            for _, test_row in job_rows.iterrows():
                job_object["tests"].append(
                    {
                        "dsid": clean_text(test_row["DSID"]),
                        "weight": int(test_row["DSID_WEIGHT"]),
                        "critical": bool(test_row["CRITICAL"]),
                    }
                )

            usecase_object["jobs"].append(job_object)

        configuration["usecases"].append(usecase_object)

    return configuration


def save_configuration_to_backend(
    health_area_id: str,
    health_area_name: str,
    domain_id: str,
    domain_name: str,
    associated_jobs: dict[str, Any],
) -> None:
    json_text = json.dumps(associated_jobs, separators=(",", ":"))

    merge_sql = f"""
        MERGE INTO {HEALTH_DOMAIN_TABLE} TARGET
        USING
        (
            SELECT
                ?::VARCHAR AS HEALTH_AREA_ID,
                ?::VARCHAR AS HEALTH_AREA_NAME,
                ?::VARCHAR AS DOMAIN_ID,
                ?::VARCHAR AS DOMAIN_NAME,
                PARSE_JSON(?) AS ASSOCIATED_JOBS
        ) SOURCE
        ON TRIM(TO_VARCHAR(TARGET.HEALTH_AREA_ID)) = SOURCE.HEALTH_AREA_ID
       AND TRIM(TO_VARCHAR(TARGET.DOMAIN_ID)) = SOURCE.DOMAIN_ID

        WHEN MATCHED THEN UPDATE SET
            TARGET.HEALTH_AREA_NAME = SOURCE.HEALTH_AREA_NAME,
            TARGET.DOMAIN_NAME = SOURCE.DOMAIN_NAME,
            TARGET.ASSOCIATED_JOBS = SOURCE.ASSOCIATED_JOBS,
            TARGET.UPDATED_ON = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN INSERT
        (
            HEALTH_AREA_ID,
            HEALTH_AREA_NAME,
            DOMAIN_ID,
            DOMAIN_NAME,
            ASSOCIATED_JOBS,
            CREATED_BY,
            CREATED_ON,
            UPDATED_ON
        )
        VALUES
        (
            SOURCE.HEALTH_AREA_ID,
            SOURCE.HEALTH_AREA_NAME,
            SOURCE.DOMAIN_ID,
            SOURCE.DOMAIN_NAME,
            SOURCE.ASSOCIATED_JOBS,
            CURRENT_USER(),
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
    """

    session.sql("BEGIN").collect()
    try:
        session.sql(
            merge_sql,
            params=[
                clean_text(health_area_id),
                clean_text(health_area_name),
                clean_text(domain_id),
                clean_text(domain_name),
                json_text,
            ],
        ).collect()

        verify_sql = f"""
            SELECT TO_JSON(ASSOCIATED_JOBS) AS ASSOCIATED_JOBS_JSON
            FROM {HEALTH_DOMAIN_TABLE}
            WHERE TRIM(TO_VARCHAR(HEALTH_AREA_ID)) = ?
              AND TRIM(TO_VARCHAR(DOMAIN_ID)) = ?
        """
        verification = session.sql(
            verify_sql,
            params=[clean_text(health_area_id), clean_text(domain_id)],
        ).collect()

        if not verification or not verification[0]["ASSOCIATED_JOBS_JSON"]:
            raise RuntimeError("The saved configuration could not be verified.")

        saved_object = json.loads(verification[0]["ASSOCIATED_JOBS_JSON"])
        if saved_object != associated_jobs:
            raise RuntimeError("The verified backend value does not match the screen.")

        session.sql("COMMIT").collect()
    except Exception:
        session.sql("ROLLBACK").collect()
        raise


def persist_current_configuration(message: str) -> None:
    associated_jobs = build_associated_jobs_json(
        int(st.session_state.domain_weight),
        st.session_state.usecases_df,
        st.session_state.tests_df,
    )

    save_configuration_to_backend(
        health_area_id=clean_text(st.session_state.active_health_area_id),
        health_area_name=clean_text(st.session_state.active_health_area_name),
        domain_id=clean_text(st.session_state.active_domain_id),
        domain_name=clean_text(st.session_state.active_domain_name),
        associated_jobs=associated_jobs,
    )

    st.session_state.last_saved_at = datetime.now()
    load_health_domains.clear()
    set_flash(message)


# ============================================================
# IDS, COUNTS, AND VALIDATION
# ============================================================


def generate_next_usecase_id(domain_id: str, usecases_df: pd.DataFrame) -> str:
    cleaned_domain_id = re.sub(r"[^A-Za-z0-9]", "", clean_text(domain_id))
    prefix = f"UC{cleaned_domain_id}-"
    highest_sequence = 0
    pattern = re.compile(rf"^{re.escape(prefix)}(\d+)$", re.IGNORECASE)

    for usecase_id in normalize_usecases(usecases_df)["USECASE_ID"].tolist():
        match = pattern.match(usecase_id)
        if match:
            highest_sequence = max(highest_sequence, int(match.group(1)))

    return f"{prefix}{highest_sequence + 1:03d}"


def working_counts() -> dict[str, int]:
    usecases = normalize_usecases(st.session_state.usecases_df)
    tests = normalize_tests(st.session_state.tests_df)

    return {
        "USECASE_COUNT": int(usecases["USECASE_ID"].nunique())
        if not usecases.empty
        else 0,
        "JOB_COUNT": int(tests["JOBID"].nunique()) if not tests.empty else 0,
        "DSID_COUNT": len(tests),
        "CRITICAL_COUNT": int(tests["CRITICAL"].sum()) if not tests.empty else 0,
    }


def configuration_counts(configuration: dict[str, Any]) -> dict[str, int]:
    usecase_ids: set[str] = set()
    job_ids: set[str] = set()
    dsid_keys: set[tuple[str, str, str]] = set()
    critical_keys: set[tuple[str, str, str]] = set()

    for usecase_index, usecase in enumerate(configuration.get("usecases", []) or []):
        usecase_id = clean_text(usecase.get("usecase_id")) or f"MISSING_{usecase_index}"
        usecase_ids.add(usecase_id)

        for job in usecase.get("jobs", []) or []:
            job_id = clean_text(job.get("jobid"))
            if job_id:
                job_ids.add(job_id)

            for test in job.get("tests", []) or []:
                dsid = clean_text(test.get("dsid"))
                if not job_id or not dsid:
                    continue
                key = (usecase_id, job_id, dsid)
                dsid_keys.add(key)
                if coerce_bool(test.get("critical", False)):
                    critical_keys.add(key)

    return {
        "USECASE_COUNT": len(usecase_ids),
        "JOB_COUNT": len(job_ids),
        "DSID_COUNT": len(dsid_keys),
        "CRITICAL_COUNT": len(critical_keys),
    }


def configuration_status(configuration: dict[str, Any]) -> str:
    counts = configuration_counts(configuration)
    if counts["USECASE_COUNT"] == 0:
        return "Not configured"
    if counts["DSID_COUNT"] == 0:
        return "Needs DSIDs"
    return "Configured"


def usecase_summary_dataframe() -> pd.DataFrame:
    usecases = normalize_usecases(st.session_state.usecases_df)
    tests = normalize_tests(st.session_state.tests_df)

    if usecases.empty:
        return pd.DataFrame(
            columns=[
                "USECASE_ID",
                "USECASE_NAME",
                "USECASE_WEIGHT",
                "JOB_COUNT",
                "DSID_COUNT",
                "CRITICAL_COUNT",
            ]
        )

    if tests.empty:
        counts = pd.DataFrame(
            columns=["USECASE_ID", "JOB_COUNT", "DSID_COUNT", "CRITICAL_COUNT"]
        )
    else:
        counts = (
            tests.groupby("USECASE_ID", dropna=False)
            .agg(
                JOB_COUNT=("JOBID", "nunique"),
                DSID_COUNT=("DSID", "count"),
                CRITICAL_COUNT=("CRITICAL", "sum"),
            )
            .reset_index()
        )

    summary = usecases.merge(counts, on="USECASE_ID", how="left")
    for column_name in ["JOB_COUNT", "DSID_COUNT", "CRITICAL_COUNT"]:
        summary[column_name] = (
            pd.to_numeric(summary[column_name], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    return summary[
        [
            "USECASE_ID",
            "USECASE_NAME",
            "USECASE_WEIGHT",
            "JOB_COUNT",
            "DSID_COUNT",
            "CRITICAL_COUNT",
        ]
    ]


def validate_for_analysis(
    usecases_df: pd.DataFrame,
    tests_df: pd.DataFrame,
) -> list[str]:
    issues: list[str] = []
    usecases = normalize_usecases(usecases_df)
    tests = normalize_tests(tests_df)

    if usecases.empty:
        issues.append("No Use Cases are configured.")
        return issues

    if tests.empty:
        issues.append("No DSIDs are configured.")
        return issues

    unknown_usecases = set(tests["USECASE_ID"]) - set(usecases["USECASE_ID"])
    if unknown_usecases:
        issues.append("Some DSIDs reference a Use Case that no longer exists.")

    duplicates = tests.duplicated(subset=["JOBID", "DSID"], keep=False)
    if duplicates.any():
        issues.append("A Job and DSID combination is assigned to multiple Use Cases.")

    tests_by_usecase = tests.groupby("USECASE_ID").size().to_dict()
    empty_names = [
        clean_text(row["USECASE_NAME"])
        for _, row in usecases.iterrows()
        if int(tests_by_usecase.get(row["USECASE_ID"], 0)) == 0
    ]
    if empty_names:
        issues.append("Use Cases without DSIDs: " + ", ".join(empty_names))

    return issues


# ============================================================
# INITIAL LOAD
# ============================================================

try:
    health_domains_df = load_health_domains().copy()
    jobs_df = load_jobs().copy()
except Exception as error:
    st.error("Unable to load the Snowflake configuration tables.")
    st.exception(error)
    st.stop()

for column_name in ["HEALTH_AREA_ID", "DOMAIN_ID"]:
    health_domains_df[column_name] = (
        health_domains_df[column_name].fillna("").astype(str).str.strip()
    )

jobs_df["JOBID"] = jobs_df["JOBID"].fillna("").astype(str).str.strip()
jobs_df["JOBNAME"] = jobs_df["JOBNAME"].fillna("").astype(str).str.strip()

job_name_by_id = {
    clean_text(row["JOBID"]): clean_text(row["JOBNAME"])
    for _, row in jobs_df.iterrows()
}


# ============================================================
# CONTEXT AND NAVIGATION
# ============================================================


def open_context(
    health_area_id: str,
    health_area_name: str,
    domain_id: str,
    domain_name: str,
) -> None:
    domain_weight, usecases_df, tests_df = load_existing_configuration(
        health_area_id,
        domain_id,
        job_name_by_id,
    )

    st.session_state.active_health_area_id = clean_text(health_area_id)
    st.session_state.active_health_area_name = clean_text(health_area_name)
    st.session_state.active_domain_id = clean_text(domain_id)
    st.session_state.active_domain_name = clean_text(domain_name)
    st.session_state.domain_weight = int(domain_weight)
    st.session_state.usecases_df = usecases_df
    st.session_state.tests_df = tests_df
    st.session_state.loaded_context_key = (
        clean_text(health_area_id),
        clean_text(domain_id),
    )

    usecase_ids = usecases_df["USECASE_ID"].tolist()
    st.session_state.selected_usecase_id = usecase_ids[0] if usecase_ids else None

    if not tests_df.empty:
        selected_usecase_tests = tests_df[
            tests_df["USECASE_ID"] == st.session_state.selected_usecase_id
        ]
        st.session_state.selected_job_id = (
            clean_text(selected_usecase_tests.iloc[0]["JOBID"])
            if not selected_usecase_tests.empty
            else None
        )
    else:
        st.session_state.selected_job_id = None

    next_editor_revision()


def go_to(page_name: str) -> None:
    st.session_state.page = page_name
    st.rerun()


def render_header(
    title: str,
    show_home: bool = True,
    show_context: bool = True,
) -> None:
    title_col, action_col = st.columns([6, 1])
    with title_col:
        st.markdown(f'<div class="app-title">{title}</div>', unsafe_allow_html=True)
    with action_col:
        if show_home and st.button("Home", use_container_width=True):
            go_to("home")

    if show_context and st.session_state.active_domain_id:
        st.markdown(
            f"""
            <div class="context-card">
                <div class="context-title">
                    {st.session_state.active_health_area_name} ·
                    {st.session_state.active_domain_name}
                </div>
                <div class="context-subtitle">
                    Domain ID: {st.session_state.active_domain_id}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_saved_caption() -> None:
    saved_time = format_saved_time()
    if saved_time:
        st.caption(f"Saved to {HEALTH_DOMAIN_TABLE} at {saved_time}")
    else:
        st.caption(f"Changes are stored in {HEALTH_DOMAIN_TABLE}.ASSOCIATED_JOBS")


# ============================================================
# HOME PAGE
# ============================================================


def render_home() -> None:
    render_header(
        "Health Configuration",
        show_home=False,
        show_context=False,
    )
    show_flash()

    area_options_df = (
        health_domains_df[["HEALTH_AREA_ID", "HEALTH_AREA_NAME"]]
        .drop_duplicates()
        .sort_values(["HEALTH_AREA_NAME", "HEALTH_AREA_ID"])
        .reset_index(drop=True)
    )

    if area_options_df.empty:
        st.warning("No Health Areas are available.")
        return

    current_area_id = clean_text(st.session_state.active_health_area_id)
    area_ids = area_options_df["HEALTH_AREA_ID"].tolist()
    default_area_index = area_ids.index(current_area_id) if current_area_id in area_ids else 0

    selector_1, selector_2, selector_3 = st.columns([2, 2, 1])

    with selector_1:
        selected_area_id = st.selectbox(
            "Health Area",
            options=area_ids,
            index=default_area_index,
            format_func=lambda value: area_options_df.loc[
                area_options_df["HEALTH_AREA_ID"] == value,
                "HEALTH_AREA_NAME",
            ].iloc[0],
        )

    selected_area_row = area_options_df[
        area_options_df["HEALTH_AREA_ID"] == selected_area_id
    ].iloc[0]
    selected_area_name = clean_text(selected_area_row["HEALTH_AREA_NAME"])

    domain_options_df = health_domains_df[
        health_domains_df["HEALTH_AREA_ID"] == selected_area_id
    ].copy()
    domain_options_df = domain_options_df.sort_values(
        ["DOMAIN_NAME", "DOMAIN_ID"]
    ).reset_index(drop=True)
    domain_ids = domain_options_df["DOMAIN_ID"].tolist()

    if not domain_ids:
        st.warning("No Domains are available for this Health Area.")
        return

    current_domain_id = clean_text(st.session_state.active_domain_id)
    default_domain_index = (
        domain_ids.index(current_domain_id)
        if current_domain_id in domain_ids
        else 0
    )

    with selector_2:
        selected_domain_id = st.selectbox(
            "Domain",
            options=domain_ids,
            index=default_domain_index,
            format_func=lambda value: domain_options_df.loc[
                domain_options_df["DOMAIN_ID"] == value,
                "DOMAIN_NAME",
            ].iloc[0],
        )

    selected_domain_row = domain_options_df[
        domain_options_df["DOMAIN_ID"] == selected_domain_id
    ].iloc[0]
    selected_domain_name = clean_text(selected_domain_row["DOMAIN_NAME"])

    with selector_3:
        st.text_input("Domain ID", value=selected_domain_id, disabled=True)

    context_key = (clean_text(selected_area_id), clean_text(selected_domain_id))
    if st.session_state.loaded_context_key != context_key:
        try:
            open_context(
                selected_area_id,
                selected_area_name,
                selected_domain_id,
                selected_domain_name,
            )
            st.rerun()
        except Exception as error:
            st.error("Unable to load the selected configuration.")
            st.exception(error)
            return

    st.write("")
    action_1, action_2 = st.columns(2)
    with action_1:
        if st.button(
            "Manage Configuration",
            type="primary",
            use_container_width=True,
        ):
            go_to("configuration")
    with action_2:
        if st.button("View Analysis", use_container_width=True):
            go_to("analysis")


# ============================================================
# CONFIGURATION PAGE
# ============================================================


def render_configuration() -> None:
    render_header("Configuration")
    show_flash()

    # Domain weight
    with st.form("domain_weight_form"):
        weight_col, button_col = st.columns([4, 1])
        with weight_col:
            new_domain_weight = st.number_input(
                "Domain Weight",
                min_value=1,
                max_value=10,
                value=int(st.session_state.domain_weight),
                step=1,
                help="Relative importance of this Domain inside the Health Area.",
            )
        with button_col:
            st.write("")
            st.write("")
            save_weight = st.form_submit_button(
                "Save Weight", type="primary", use_container_width=True
            )

    if save_weight:
        try:
            st.session_state.domain_weight = int(new_domain_weight)
            persist_current_configuration("Domain Weight saved.")
            st.rerun()
        except Exception as error:
            st.error("Unable to save Domain Weight.")
            st.exception(error)

    st.divider()
    st.markdown("### Use Cases")

    usecases_df = normalize_usecases(st.session_state.usecases_df)
    tests_df = normalize_tests(st.session_state.tests_df)
    st.session_state.usecases_df = usecases_df
    st.session_state.tests_df = tests_df

    summary_df = usecase_summary_dataframe()
    if summary_df.empty:
        st.info("No Use Cases configured.")
    else:
        editor_df = summary_df.copy()
        editor_df.insert(0, "REMOVE", False)

        edited_usecases_df = st.data_editor(
            editor_df,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_order=[
                "REMOVE",
                "USECASE_WEIGHT",
                "USECASE_NAME",
                "USECASE_ID",
                "JOB_COUNT",
                "DSID_COUNT",
                "CRITICAL_COUNT",
            ],
            disabled=[
                "USECASE_ID",
                "JOB_COUNT",
                "DSID_COUNT",
                "CRITICAL_COUNT",
            ],
            column_config={
                "REMOVE": st.column_config.CheckboxColumn(
                    "Remove", width="small"
                ),
                "USECASE_WEIGHT": st.column_config.NumberColumn(
                    "Weight",
                    min_value=1,
                    max_value=10,
                    step=1,
                    required=True,
                    width="small",
                    help="Relative importance inside this Domain.",
                ),
                "USECASE_NAME": st.column_config.TextColumn(
                    "Use Case", required=True, width="large"
                ),
                "USECASE_ID": st.column_config.TextColumn(
                    "Use Case ID", width="medium"
                ),
                "JOB_COUNT": st.column_config.NumberColumn("Jobs", width="small"),
                "DSID_COUNT": st.column_config.NumberColumn("DSIDs", width="small"),
                "CRITICAL_COUNT": st.column_config.NumberColumn(
                    "Critical", width="small"
                ),
            },
            key=f"usecase_editor_{st.session_state.editor_revision}",
        )

        if st.button(
            "Save Use Case Changes",
            type="primary",
            use_container_width=True,
        ):
            rows = edited_usecases_df.copy()
            rows["USECASE_NAME"] = rows["USECASE_NAME"].fillna("").astype(str).str.strip()
            rows["USECASE_WEIGHT"] = pd.to_numeric(
                rows["USECASE_WEIGHT"], errors="coerce"
            )

            if rows["USECASE_NAME"].eq("").any():
                st.error("Use Case Name cannot be empty.")
            elif rows["USECASE_WEIGHT"].isna().any() or (
                ~rows["USECASE_WEIGHT"].between(1, 10)
            ).any():
                st.error("Every Use Case Weight must be between 1 and 10.")
            else:
                try:
                    remove_ids = set(
                        rows.loc[rows["REMOVE"] == True, "USECASE_ID"].astype(str)
                    )
                    kept_usecases = rows[rows["REMOVE"] != True][
                        USECASE_COLUMNS
                    ].copy()
                    kept_usecases["USECASE_WEIGHT"] = kept_usecases[
                        "USECASE_WEIGHT"
                    ].astype(int)

                    st.session_state.usecases_df = normalize_usecases(kept_usecases)
                    st.session_state.tests_df = normalize_tests(
                        tests_df[~tests_df["USECASE_ID"].isin(remove_ids)]
                    )

                    remaining_ids = st.session_state.usecases_df[
                        "USECASE_ID"
                    ].tolist()
                    if st.session_state.selected_usecase_id not in remaining_ids:
                        st.session_state.selected_usecase_id = (
                            remaining_ids[0] if remaining_ids else None
                        )

                    persist_current_configuration("Use Case changes saved.")
                    next_editor_revision()
                    st.rerun()
                except Exception as error:
                    st.error("Unable to save Use Case changes.")
                    st.exception(error)

    with st.expander("Add Use Case", expanded=summary_df.empty):
        next_usecase_id = generate_next_usecase_id(
            st.session_state.active_domain_id,
            st.session_state.usecases_df,
        )

        with st.form(f"add_usecase_form_{st.session_state.new_usecase_revision}"):
            add_col_1, add_col_2, add_col_3 = st.columns([1, 2, 1])
            with add_col_1:
                st.text_input("Use Case ID", value=next_usecase_id, disabled=True)
            with add_col_2:
                new_usecase_name = st.text_input("Use Case Name")
            with add_col_3:
                new_usecase_weight = st.number_input(
                    "Weight",
                    min_value=1,
                    max_value=10,
                    value=5,
                    step=1,
                    help="Relative importance inside this Domain.",
                )

            add_usecase = st.form_submit_button(
                "Add Use Case", type="primary", use_container_width=True
            )

        if add_usecase:
            if not new_usecase_name.strip():
                st.error("Enter a Use Case Name.")
            else:
                try:
                    new_row = pd.DataFrame(
                        [
                            {
                                "USECASE_ID": next_usecase_id,
                                "USECASE_NAME": new_usecase_name.strip(),
                                "USECASE_WEIGHT": int(new_usecase_weight),
                            }
                        ],
                        columns=USECASE_COLUMNS,
                    )
                    st.session_state.usecases_df = normalize_usecases(
                        pd.concat(
                            [st.session_state.usecases_df, new_row],
                            ignore_index=True,
                        )
                    )
                    st.session_state.selected_usecase_id = next_usecase_id
                    st.session_state.new_usecase_revision += 1
                    persist_current_configuration("Use Case added and saved.")
                    next_editor_revision()
                    st.rerun()
                except Exception as error:
                    st.error("Unable to add the Use Case.")
                    st.exception(error)

    st.divider()
    st.markdown("### DSIDs")

    usecases_df = normalize_usecases(st.session_state.usecases_df)
    tests_df = normalize_tests(st.session_state.tests_df)

    if usecases_df.empty:
        st.info("Add a Use Case before configuring DSIDs.")
        render_saved_caption()
        return

    usecase_ids = usecases_df["USECASE_ID"].tolist()
    usecase_name_by_id = {
        clean_text(row["USECASE_ID"]): clean_text(row["USECASE_NAME"])
        for _, row in usecases_df.iterrows()
    }

    if st.session_state.selected_usecase_id not in usecase_ids:
        st.session_state.selected_usecase_id = usecase_ids[0]

    selector_1, selector_2 = st.columns(2)
    with selector_1:
        selected_usecase_id = st.selectbox(
            "Use Case",
            options=usecase_ids,
            index=usecase_ids.index(st.session_state.selected_usecase_id),
            format_func=lambda value: usecase_name_by_id.get(value, value),
            key="dsid_usecase_selector",
        )

    st.session_state.selected_usecase_id = selected_usecase_id

    job_ids = jobs_df["JOBID"].tolist()
    if not job_ids:
        st.warning("No Jobs are available.")
        render_saved_caption()
        return

    configured_for_usecase = tests_df[
        tests_df["USECASE_ID"] == selected_usecase_id
    ]
    preferred_job_id = (
        clean_text(configured_for_usecase.iloc[0]["JOBID"])
        if not configured_for_usecase.empty
        else None
    )

    if st.session_state.selected_job_id not in job_ids:
        st.session_state.selected_job_id = (
            preferred_job_id if preferred_job_id in job_ids else job_ids[0]
        )

    with selector_2:
        selected_job_id = st.selectbox(
            "Job",
            options=job_ids,
            index=job_ids.index(st.session_state.selected_job_id),
            format_func=lambda value: f"{job_name_by_id.get(value, value)} ({value})",
            key="dsid_job_selector",
        )

    st.session_state.selected_job_id = selected_job_id

    configured_tab, add_tab = st.tabs(["Configured DSIDs", "Add DSIDs"])

    with configured_tab:
        configured_df = tests_df[
            (tests_df["USECASE_ID"] == selected_usecase_id)
            & (tests_df["JOBID"] == selected_job_id)
        ].copy()

        if configured_df.empty:
            st.info("No DSIDs configured for this Use Case and Job.")
        else:
            configured_editor_df = configured_df.copy()
            configured_editor_df.insert(0, "REMOVE", False)

            edited_configured_df = st.data_editor(
                configured_editor_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_order=[
                    "REMOVE",
                    "DSID_WEIGHT",
                    "CRITICAL",
                    "DSID",
                    "TESTCASEDESCRIPTION",
                    "JOBID",
                ],
                disabled=["DSID", "TESTCASEDESCRIPTION", "JOBID"],
                column_config={
                    "REMOVE": st.column_config.CheckboxColumn(
                        "Remove", width="small"
                    ),
                    "DSID_WEIGHT": st.column_config.NumberColumn(
                        "Weight",
                        min_value=1,
                        max_value=10,
                        step=1,
                        required=True,
                        width="small",
                        help="Relative importance inside this Use Case.",
                    ),
                    "CRITICAL": st.column_config.CheckboxColumn(
                        "Critical",
                        width="small",
                        help="Select only for business-critical tests.",
                    ),
                    "DSID": st.column_config.TextColumn("DSID", width="medium"),
                    "TESTCASEDESCRIPTION": st.column_config.TextColumn(
                        "Test Case Description", width="large"
                    ),
                    "JOBID": st.column_config.TextColumn("Job ID", width="small"),
                },
                key=f"configured_dsid_editor_{st.session_state.editor_revision}",
            )

            if st.button(
                "Save DSID Changes",
                type="primary",
                use_container_width=True,
                key="save_configured_dsids",
            ):
                rows = edited_configured_df.copy()
                rows["DSID_WEIGHT"] = pd.to_numeric(
                    rows["DSID_WEIGHT"], errors="coerce"
                )

                if rows["DSID_WEIGHT"].isna().any() or (
                    ~rows["DSID_WEIGHT"].between(1, 10)
                ).any():
                    st.error("Every DSID Weight must be between 1 and 10.")
                else:
                    try:
                        existing_other_rows = tests_df[
                            ~(
                                (tests_df["USECASE_ID"] == selected_usecase_id)
                                & (tests_df["JOBID"] == selected_job_id)
                            )
                        ].copy()

                        kept_rows = rows[rows["REMOVE"] != True][
                            TEST_CONFIG_COLUMNS
                        ].copy()
                        kept_rows["DSID_WEIGHT"] = kept_rows[
                            "DSID_WEIGHT"
                        ].astype(int)
                        kept_rows["CRITICAL"] = kept_rows["CRITICAL"].apply(
                            coerce_bool
                        )

                        st.session_state.tests_df = normalize_tests(
                            pd.concat(
                                [existing_other_rows, kept_rows],
                                ignore_index=True,
                            )
                        )
                        persist_current_configuration("DSID changes saved.")
                        next_editor_revision()
                        st.rerun()
                    except Exception as error:
                        st.error("Unable to save DSID changes.")
                        st.exception(error)

    with add_tab:
        try:
            source_tests_df = load_tests_for_job(selected_job_id).copy()
            source_tests_df["JOBID"] = (
                source_tests_df["JOBID"].fillna("").astype(str).str.strip()
            )
            source_tests_df["DSID"] = (
                source_tests_df["DSID"].fillna("").astype(str).str.strip()
            )
            source_tests_df["TESTCASEDESCRIPTION"] = (
                source_tests_df["TESTCASEDESCRIPTION"]
                .fillna("")
                .astype(str)
                .str.strip()
            )
        except Exception as error:
            st.error("Unable to load DSIDs for the selected Job.")
            st.exception(error)
            source_tests_df = pd.DataFrame(
                columns=["JOBID", "DSID", "TESTCASEDESCRIPTION"]
            )

        used_keys = set(
            zip(
                tests_df["JOBID"].astype(str),
                tests_df["DSID"].astype(str),
            )
        )

        available_df = source_tests_df[
            ~source_tests_df.apply(
                lambda row: (
                    clean_text(row["JOBID"]),
                    clean_text(row["DSID"]),
                )
                in used_keys,
                axis=1,
            )
        ].copy()

        if available_df.empty:
            st.info("No additional DSIDs are available for this Job.")
        else:
            available_df.insert(0, "SELECT", False)
            available_df.insert(1, "DSID_WEIGHT", 5)
            available_df.insert(2, "CRITICAL", False)

            edited_available_df = st.data_editor(
                available_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_order=[
                    "SELECT",
                    "DSID_WEIGHT",
                    "CRITICAL",
                    "DSID",
                    "TESTCASEDESCRIPTION",
                ],
                disabled=["DSID", "TESTCASEDESCRIPTION"],
                column_config={
                    "SELECT": st.column_config.CheckboxColumn(
                        "Select", width="small"
                    ),
                    "DSID_WEIGHT": st.column_config.NumberColumn(
                        "Weight",
                        min_value=1,
                        max_value=10,
                        step=1,
                        required=True,
                        width="small",
                        help="Relative importance inside this Use Case.",
                    ),
                    "CRITICAL": st.column_config.CheckboxColumn(
                        "Critical",
                        width="small",
                        help="Select only for business-critical tests.",
                    ),
                    "DSID": st.column_config.TextColumn("DSID", width="medium"),
                    "TESTCASEDESCRIPTION": st.column_config.TextColumn(
                        "Test Case Description", width="large"
                    ),
                },
                key=f"available_dsid_editor_{st.session_state.editor_revision}",
            )

            if st.button(
                "Add Selected DSIDs",
                type="primary",
                use_container_width=True,
                key="add_selected_dsids",
            ):
                selected_rows = edited_available_df[
                    edited_available_df["SELECT"] == True
                ].copy()

                if selected_rows.empty:
                    st.warning("Select at least one DSID.")
                else:
                    selected_rows["DSID_WEIGHT"] = pd.to_numeric(
                        selected_rows["DSID_WEIGHT"], errors="coerce"
                    )
                    if selected_rows["DSID_WEIGHT"].isna().any() or (
                        ~selected_rows["DSID_WEIGHT"].between(1, 10)
                    ).any():
                        st.error("Every selected DSID Weight must be between 1 and 10.")
                    else:
                        try:
                            new_rows: list[dict[str, Any]] = []
                            for _, row in selected_rows.iterrows():
                                new_rows.append(
                                    {
                                        "USECASE_ID": selected_usecase_id,
                                        "JOBID": selected_job_id,
                                        "JOBNAME": job_name_by_id.get(
                                            selected_job_id, ""
                                        ),
                                        "DSID": clean_text(row["DSID"]),
                                        "TESTCASEDESCRIPTION": clean_text(
                                            row["TESTCASEDESCRIPTION"]
                                        ),
                                        "DSID_WEIGHT": int(row["DSID_WEIGHT"]),
                                        "CRITICAL": coerce_bool(row["CRITICAL"]),
                                    }
                                )

                            st.session_state.tests_df = normalize_tests(
                                pd.concat(
                                    [
                                        tests_df,
                                        pd.DataFrame(
                                            new_rows,
                                            columns=TEST_CONFIG_COLUMNS,
                                        ),
                                    ],
                                    ignore_index=True,
                                )
                            )
                            persist_current_configuration(
                                "Selected DSIDs added and saved."
                            )
                            next_editor_revision()
                            st.rerun()
                        except Exception as error:
                            st.error("Unable to add the selected DSIDs.")
                            st.exception(error)

    render_saved_caption()


# ============================================================
# ANALYSIS PAGE
# ============================================================


def render_analysis() -> None:
    render_header("Analysis")
    show_flash()

    try:
        saved_rows_df = load_saved_configuration_rows().copy()
    except Exception as error:
        st.error("Unable to load saved configurations.")
        st.exception(error)
        return

    active_area_id = clean_text(st.session_state.active_health_area_id)
    area_domains_df = health_domains_df[
        health_domains_df["HEALTH_AREA_ID"] == active_area_id
    ].copy()
    saved_area_df = saved_rows_df[
        saved_rows_df["HEALTH_AREA_ID"].astype(str).str.strip() == active_area_id
    ].copy()

    saved_by_domain = {
        clean_text(row["DOMAIN_ID"]): row for _, row in saved_area_df.iterrows()
    }

    overview_rows: list[dict[str, Any]] = []
    area_usecase_keys: set[tuple[str, str]] = set()
    area_job_ids: set[str] = set()
    area_dsid_keys: set[tuple[str, str, str, str]] = set()
    area_critical_keys: set[tuple[str, str, str, str]] = set()

    configured_domain_count = 0

    for _, domain_row in area_domains_df.iterrows():
        domain_id = clean_text(domain_row["DOMAIN_ID"])
        domain_name = clean_text(domain_row["DOMAIN_NAME"])
        saved_row = saved_by_domain.get(domain_id)

        try:
            configuration = (
                parse_variant_object(saved_row["ASSOCIATED_JOBS"])
                if saved_row is not None and saved_row["ASSOCIATED_JOBS"] is not None
                else {}
            )
        except Exception:
            configuration = {}

        counts = configuration_counts(configuration)
        status = configuration_status(configuration)
        if status == "Configured":
            configured_domain_count += 1

        overview_rows.append(
            {
                "DOMAIN_ID": domain_id,
                "DOMAIN_NAME": domain_name,
                "STATUS": status,
                "DOMAIN_WEIGHT": int(configuration.get("domain_weight", 5)),
                **counts,
            }
        )

        for usecase_index, usecase in enumerate(configuration.get("usecases", []) or []):
            usecase_id = clean_text(usecase.get("usecase_id")) or f"MISSING_{usecase_index}"
            area_usecase_keys.add((domain_id, usecase_id))

            for job in usecase.get("jobs", []) or []:
                job_id = clean_text(job.get("jobid"))
                if job_id:
                    area_job_ids.add(job_id)

                for test in job.get("tests", []) or []:
                    dsid = clean_text(test.get("dsid"))
                    if not job_id or not dsid:
                        continue
                    key = (domain_id, usecase_id, job_id, dsid)
                    area_dsid_keys.add(key)
                    if coerce_bool(test.get("critical", False)):
                        area_critical_keys.add(key)

    overview_df = pd.DataFrame(overview_rows)

    metric_1, metric_2, metric_3, metric_4, metric_5 = st.columns(5)
    metric_1.metric("Domains", len(area_domains_df))
    metric_2.metric("Configured", configured_domain_count)
    metric_3.metric("Use Cases", len(area_usecase_keys))
    metric_4.metric("Jobs", len(area_job_ids))
    metric_5.metric("DSIDs", len(area_dsid_keys))

    if not overview_df.empty:
        st.dataframe(
            overview_df,
            use_container_width=True,
            hide_index=True,
            column_order=[
                "DOMAIN_NAME",
                "STATUS",
                "DOMAIN_WEIGHT",
                "USECASE_COUNT",
                "JOB_COUNT",
                "DSID_COUNT",
                "CRITICAL_COUNT",
            ],
            column_config={
                "DOMAIN_NAME": st.column_config.TextColumn(
                    "Domain", width="medium"
                ),
                "STATUS": st.column_config.TextColumn("Status", width="small"),
                "DOMAIN_WEIGHT": st.column_config.NumberColumn(
                    "Weight", width="small"
                ),
                "USECASE_COUNT": st.column_config.NumberColumn(
                    "Use Cases", width="small"
                ),
                "JOB_COUNT": st.column_config.NumberColumn("Jobs", width="small"),
                "DSID_COUNT": st.column_config.NumberColumn("DSIDs", width="small"),
                "CRITICAL_COUNT": st.column_config.NumberColumn(
                    "Critical", width="small"
                ),
            },
        )

    st.markdown("### Selected Domain")

    try:
        domain_weight, saved_usecases_df, saved_tests_df = load_existing_configuration(
            st.session_state.active_health_area_id,
            st.session_state.active_domain_id,
            job_name_by_id,
        )
    except Exception as error:
        st.error("Unable to load the selected Domain from the backend.")
        st.exception(error)
        return

    selected_counts = {
        "USECASE_COUNT": int(saved_usecases_df["USECASE_ID"].nunique())
        if not saved_usecases_df.empty
        else 0,
        "JOB_COUNT": int(saved_tests_df["JOBID"].nunique())
        if not saved_tests_df.empty
        else 0,
        "DSID_COUNT": len(saved_tests_df),
        "CRITICAL_COUNT": int(saved_tests_df["CRITICAL"].sum())
        if not saved_tests_df.empty
        else 0,
    }

    selected_1, selected_2, selected_3, selected_4, selected_5 = st.columns(5)
    selected_1.metric("Domain Weight", int(domain_weight))
    selected_2.metric("Use Cases", selected_counts["USECASE_COUNT"])
    selected_3.metric("Jobs", selected_counts["JOB_COUNT"])
    selected_4.metric("DSIDs", selected_counts["DSID_COUNT"])
    selected_5.metric("Critical", selected_counts["CRITICAL_COUNT"])

    if not saved_usecases_df.empty:
        saved_test_counts = (
            saved_tests_df.groupby("USECASE_ID")
            .agg(
                JOB_COUNT=("JOBID", "nunique"),
                DSID_COUNT=("DSID", "count"),
                CRITICAL_COUNT=("CRITICAL", "sum"),
            )
            .reset_index()
            if not saved_tests_df.empty
            else pd.DataFrame(
                columns=[
                    "USECASE_ID",
                    "JOB_COUNT",
                    "DSID_COUNT",
                    "CRITICAL_COUNT",
                ]
            )
        )

        saved_usecase_summary = saved_usecases_df.merge(
            saved_test_counts,
            on="USECASE_ID",
            how="left",
        )
        for column_name in ["JOB_COUNT", "DSID_COUNT", "CRITICAL_COUNT"]:
            saved_usecase_summary[column_name] = (
                pd.to_numeric(
                    saved_usecase_summary[column_name], errors="coerce"
                )
                .fillna(0)
                .astype(int)
            )

        st.dataframe(
            saved_usecase_summary,
            use_container_width=True,
            hide_index=True,
            column_order=[
                "USECASE_NAME",
                "USECASE_WEIGHT",
                "JOB_COUNT",
                "DSID_COUNT",
                "CRITICAL_COUNT",
                "USECASE_ID",
            ],
            column_config={
                "USECASE_NAME": st.column_config.TextColumn(
                    "Use Case", width="large"
                ),
                "USECASE_WEIGHT": st.column_config.NumberColumn(
                    "Weight", width="small"
                ),
                "JOB_COUNT": st.column_config.NumberColumn("Jobs", width="small"),
                "DSID_COUNT": st.column_config.NumberColumn("DSIDs", width="small"),
                "CRITICAL_COUNT": st.column_config.NumberColumn(
                    "Critical", width="small"
                ),
                "USECASE_ID": st.column_config.TextColumn(
                    "Use Case ID", width="medium"
                ),
            },
        )

    if not saved_tests_df.empty:
        with st.expander("DSID Details"):
            detail_df = saved_tests_df.merge(
                saved_usecases_df[["USECASE_ID", "USECASE_NAME"]],
                on="USECASE_ID",
                how="left",
            )
            st.dataframe(
                detail_df,
                use_container_width=True,
                hide_index=True,
                column_order=[
                    "USECASE_NAME",
                    "JOBNAME",
                    "DSID_WEIGHT",
                    "CRITICAL",
                    "DSID",
                    "TESTCASEDESCRIPTION",
                ],
                column_config={
                    "USECASE_NAME": st.column_config.TextColumn(
                        "Use Case", width="medium"
                    ),
                    "JOBNAME": st.column_config.TextColumn("Job", width="medium"),
                    "DSID_WEIGHT": st.column_config.NumberColumn(
                        "Weight", width="small"
                    ),
                    "CRITICAL": st.column_config.CheckboxColumn(
                        "Critical", width="small"
                    ),
                    "DSID": st.column_config.TextColumn("DSID", width="medium"),
                    "TESTCASEDESCRIPTION": st.column_config.TextColumn(
                        "Test Case Description", width="large"
                    ),
                },
            )

    issues = validate_for_analysis(saved_usecases_df, saved_tests_df)
    if issues:
        for issue in issues:
            st.warning(issue)
    else:
        st.success("The selected Domain configuration is complete.")

    if st.button("Reload from Backend", use_container_width=True):
        try:
            open_context(
                st.session_state.active_health_area_id,
                st.session_state.active_health_area_name,
                st.session_state.active_domain_id,
                st.session_state.active_domain_name,
            )
            set_flash("Configuration reloaded from the backend.")
            st.rerun()
        except Exception as error:
            st.error("Unable to reload the configuration.")
            st.exception(error)


# ============================================================
# ROUTER
# ============================================================

if st.session_state.page != "home" and not st.session_state.active_domain_id:
    st.session_state.page = "home"

if st.session_state.page == "configuration":
    render_configuration()
elif st.session_state.page == "analysis":
    render_analysis()
else:
    render_home()
