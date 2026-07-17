import json
import re
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session


# ============================================================
# APP CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Health Domain Configuration",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="expanded",
)

session = get_active_session()

ENVIRONMENT_ID = "1"

# Use fully-qualified names when needed:
# DATABASE.SCHEMA.TABLE_NAME
HEALTH_DOMAIN_TABLE = "DSE_HEALTH_DOMAIN"
JOB_CONFIG_TABLE = "DSE_JOB_CONFIG"
TEST_PLAN_TABLE = "DSE_TESTPLAN"


# ============================================================
# EXPECTED TABLE COLUMNS
# ============================================================
#
# DSE_HEALTH_DOMAIN
#   HEALTH_AREA_ID
#   HEALTH_AREA_NAME
#   DOMAIN_ID
#   DOMAIN_NAME
#   ASSOCIATED_JOBS
#   CREATED_BY
#   CREATED_ON
#   UPDATED_ON
#
# DSE_JOB_CONFIG
#   JOBID              VARCHAR
#   JOBNAME
#   ENVIRONMENT_ID
#
# DSE_TESTPLAN
#   JOBID              VARCHAR
#   DSID
#   TESTCASEDESCRIPTION
#
# DSE_TESTPLAN is intentionally not filtered by ENVIRONMENT_ID or ACT_IND.


# ============================================================
# UI STYLING
# ============================================================

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.1rem;
            padding-bottom: 3rem;
        }

        .app-header {
            border: 1px solid rgba(128, 128, 128, 0.25);
            border-radius: 16px;
            padding: 1.2rem 1.4rem;
            margin-bottom: 1rem;
        }

        .app-title {
            font-size: 2rem;
            font-weight: 750;
            margin-bottom: 0.15rem;
        }

        .app-subtitle {
            opacity: 0.72;
        }

        .instruction-box {
            border: 1px solid rgba(128, 128, 128, 0.25);
            border-radius: 12px;
            padding: 0.9rem 1rem;
            margin: 0.5rem 0 0.8rem 0;
        }

        div[data-testid="stMetric"] {
            border: 1px solid rgba(128, 128, 128, 0.25);
            border-radius: 12px;
            padding: 0.8rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# SESSION STATE
# ============================================================

CONFIG_COLUMNS = [
    "USECASE_ID",
    "USECASE_NAME",
    "USECASE_WEIGHT",
    "JOBID",
    "JOBNAME",
    "DSID",
    "TESTCASEDESCRIPTION",
    "DSID_WEIGHT",
    "CRITICAL",
]


def empty_configuration_dataframe() -> pd.DataFrame:
    return pd.DataFrame(columns=CONFIG_COLUMNS)


def initialize_session_state() -> None:
    defaults = {
        "configuration_rows": empty_configuration_dataframe(),
        "loaded_health_area_id": None,
        "loaded_health_area_name": "",
        "loaded_domain_id": None,
        "loaded_domain_name": "",
        "loaded_domain_weight": 5,
        "new_usecase_name": "",
        "new_usecase_weight": 5,
        "selected_job_label": None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


initialize_session_state()


# ============================================================
# DATABASE READ FUNCTIONS
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
                TRY_TO_NUMBER(
                    TO_VARCHAR(ASSOCIATED_JOBS:domain_weight)
                ),
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

    return session.sql(
        query,
        params=[ENVIRONMENT_ID],
    ).to_pandas()


@st.cache_data(ttl=60, show_spinner=False)
def load_tests_for_job(job_id: str) -> pd.DataFrame:
    selected_job_id = str(job_id).strip()

    query = f"""
        SELECT DISTINCT
            TRIM(TO_VARCHAR(JOBID)) AS JOBID,
            TRIM(TO_VARCHAR(DSID)) AS DSID,
            COALESCE(
                TRIM(TO_VARCHAR(TESTCASEDESCRIPTION)),
                ''
            ) AS TESTCASEDESCRIPTION
        FROM {TEST_PLAN_TABLE}
        WHERE
            (
                UPPER(TRIM(TO_VARCHAR(JOBID))) =
                    UPPER(TRIM(?))
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
        params=[
            selected_job_id,
            selected_job_id,
            selected_job_id,
        ],
    ).to_pandas()


@st.cache_data(ttl=60, show_spinner=False)
def load_testplan_jobid_samples() -> pd.DataFrame:
    query = f"""
        SELECT
            TRIM(TO_VARCHAR(JOBID)) AS JOBID,
            COUNT(DISTINCT TRIM(TO_VARCHAR(DSID))) AS DSID_COUNT
        FROM {TEST_PLAN_TABLE}
        WHERE NULLIF(TRIM(TO_VARCHAR(JOBID)), '') IS NOT NULL
        GROUP BY TRIM(TO_VARCHAR(JOBID))
        ORDER BY DSID_COUNT DESC, JOBID
        LIMIT 100
    """

    return session.sql(query).to_pandas()


def load_existing_configuration(
    health_area_id: str,
    domain_id: str,
) -> tuple[int, pd.DataFrame]:
    query = f"""
        SELECT ASSOCIATED_JOBS
        FROM {HEALTH_DOMAIN_TABLE}
        WHERE TRIM(TO_VARCHAR(HEALTH_AREA_ID)) = ?
          AND TRIM(TO_VARCHAR(DOMAIN_ID)) = ?
    """

    result = session.sql(
        query,
        params=[
            str(health_area_id).strip(),
            str(domain_id).strip(),
        ],
    ).collect()

    if not result or result[0]["ASSOCIATED_JOBS"] is None:
        return 5, empty_configuration_dataframe()

    raw_configuration = result[0]["ASSOCIATED_JOBS"]

    if isinstance(raw_configuration, str):
        configuration = json.loads(raw_configuration)
    else:
        configuration = raw_configuration

    domain_weight = int(configuration.get("domain_weight", 5))
    records: list[dict[str, Any]] = []

    jobs_df = load_jobs()
    job_name_by_id = {
        str(row["JOBID"]).strip(): str(row["JOBNAME"]).strip()
        for _, row in jobs_df.iterrows()
    }

    description_cache: dict[str, dict[str, str]] = {}

    for usecase in configuration.get("usecases", []):
        usecase_id = str(usecase.get("usecase_id", "")).strip()
        usecase_name = str(usecase.get("usecase_name", "")).strip()
        usecase_weight = int(usecase.get("usecase_weight", 5))

        for job in usecase.get("jobs", []):
            job_id = str(job.get("jobid", "")).strip()
            job_name = job_name_by_id.get(job_id, "")

            if job_id not in description_cache:
                try:
                    tests_df = load_tests_for_job(job_id)
                    description_cache[job_id] = {
                        str(row["DSID"]).strip(): str(
                            row["TESTCASEDESCRIPTION"]
                        )
                        for _, row in tests_df.iterrows()
                    }
                except Exception:
                    description_cache[job_id] = {}

            for test in job.get("tests", []):
                dsid = str(test.get("dsid", "")).strip()

                records.append(
                    {
                        "USECASE_ID": usecase_id,
                        "USECASE_NAME": usecase_name,
                        "USECASE_WEIGHT": usecase_weight,
                        "JOBID": job_id,
                        "JOBNAME": job_name,
                        "DSID": dsid,
                        "TESTCASEDESCRIPTION": (
                            description_cache[job_id].get(dsid, "")
                        ),
                        "DSID_WEIGHT": int(test.get("weight", 5)),
                        "CRITICAL": bool(test.get("critical", False)),
                    }
                )

    return domain_weight, pd.DataFrame(records, columns=CONFIG_COLUMNS)


# ============================================================
# USE CASE ID GENERATION
# ============================================================

def generate_next_usecase_id(
    domain_id: str,
    configuration_rows: pd.DataFrame,
) -> str:
    """
    Generates a stable business-friendly ID:
      UC<DOMAIN_ID>-001
      UC<DOMAIN_ID>-002
      ...

    Existing IDs are inspected so the next available sequence is used.
    """

    clean_domain_id = re.sub(
        r"[^A-Za-z0-9]",
        "",
        str(domain_id).strip(),
    )

    prefix = f"UC{clean_domain_id}-"
    highest_sequence = 0

    if not configuration_rows.empty and "USECASE_ID" in configuration_rows.columns:
        existing_ids = (
            configuration_rows["USECASE_ID"]
            .fillna("")
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )

        pattern = re.compile(
            rf"^{re.escape(prefix)}(\d+)$",
            re.IGNORECASE,
        )

        for existing_id in existing_ids:
            match = pattern.match(existing_id)

            if match:
                highest_sequence = max(
                    highest_sequence,
                    int(match.group(1)),
                )

    return f"{prefix}{highest_sequence + 1:03d}"


# ============================================================
# VALIDATION AND JSON BUILD
# ============================================================

def normalize_configuration_rows(
    configuration_rows: pd.DataFrame,
) -> pd.DataFrame:
    if configuration_rows.empty:
        return empty_configuration_dataframe()

    rows = configuration_rows.copy()

    for column_name in [
        "USECASE_ID",
        "USECASE_NAME",
        "JOBID",
        "JOBNAME",
        "DSID",
        "TESTCASEDESCRIPTION",
    ]:
        rows[column_name] = (
            rows[column_name]
            .fillna("")
            .astype(str)
            .str.strip()
        )

    rows["USECASE_WEIGHT"] = (
        pd.to_numeric(
            rows["USECASE_WEIGHT"],
            errors="coerce",
        )
        .fillna(5)
        .clip(1, 10)
        .astype(int)
    )

    rows["DSID_WEIGHT"] = (
        pd.to_numeric(
            rows["DSID_WEIGHT"],
            errors="coerce",
        )
        .fillna(5)
        .clip(1, 10)
        .astype(int)
    )

    rows["CRITICAL"] = (
        rows["CRITICAL"]
        .fillna(False)
        .astype(bool)
    )

    return rows[
        (rows["USECASE_ID"] != "")
        & (rows["USECASE_NAME"] != "")
        & (rows["JOBID"] != "")
        & (rows["DSID"] != "")
    ].copy()


def validate_configuration(
    configuration_rows: pd.DataFrame,
) -> list[str]:
    if configuration_rows.empty:
        return ["Add at least one DSID before saving."]

    errors: list[str] = []
    rows = configuration_rows.copy()

    for required_column in CONFIG_COLUMNS:
        if required_column not in rows.columns:
            errors.append(f"Missing required column: {required_column}")

    if errors:
        return errors

    if rows["USECASE_ID"].fillna("").astype(str).str.strip().eq("").any():
        errors.append("Use Case ID cannot be empty.")

    if rows["USECASE_NAME"].fillna("").astype(str).str.strip().eq("").any():
        errors.append("Use Case Name cannot be empty.")

    if rows["JOBID"].fillna("").astype(str).str.strip().eq("").any():
        errors.append("Job ID cannot be empty.")

    if rows["DSID"].fillna("").astype(str).str.strip().eq("").any():
        errors.append("DSID cannot be empty.")

    usecase_weights = pd.to_numeric(
        rows["USECASE_WEIGHT"],
        errors="coerce",
    )

    if (
        usecase_weights.isna().any()
        or (~usecase_weights.between(1, 10)).any()
    ):
        errors.append("Use Case Weight must be between 1 and 10.")

    dsid_weights = pd.to_numeric(
        rows["DSID_WEIGHT"],
        errors="coerce",
    )

    if (
        dsid_weights.isna().any()
        or (~dsid_weights.between(1, 10)).any()
    ):
        errors.append("Each DSID Weight must be between 1 and 10.")

    duplicate_mask = rows.duplicated(
        subset=[
            "USECASE_ID",
            "JOBID",
            "DSID",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        errors.append(
            "The same DSID cannot be repeated for the same Use Case and Job."
        )

    consistency = (
        rows.groupby("USECASE_ID", dropna=False)
        .agg(
            USECASE_NAME_COUNT=("USECASE_NAME", "nunique"),
            USECASE_WEIGHT_COUNT=("USECASE_WEIGHT", "nunique"),
        )
        .reset_index()
    )

    if (
        (consistency["USECASE_NAME_COUNT"] > 1)
        | (consistency["USECASE_WEIGHT_COUNT"] > 1)
    ).any():
        errors.append(
            "Each Use Case ID must have one consistent name and weight."
        )

    return errors


def build_associated_jobs_json(
    domain_weight: int,
    configuration_rows: pd.DataFrame,
) -> dict[str, Any]:
    rows = normalize_configuration_rows(configuration_rows)

    configuration: dict[str, Any] = {
        "domain_weight": int(domain_weight),
        "usecases": [],
    }

    for (
        usecase_id,
        usecase_name,
        usecase_weight,
    ), usecase_rows in rows.groupby(
        [
            "USECASE_ID",
            "USECASE_NAME",
            "USECASE_WEIGHT",
        ],
        sort=False,
        dropna=False,
    ):
        usecase_object: dict[str, Any] = {
            "usecase_id": str(usecase_id),
            "usecase_name": str(usecase_name),
            "usecase_weight": int(usecase_weight),
            "jobs": [],
        }

        for job_id, job_rows in usecase_rows.groupby(
            "JOBID",
            sort=False,
            dropna=False,
        ):
            tests = []

            for _, test_row in job_rows.iterrows():
                tests.append(
                    {
                        "dsid": str(test_row["DSID"]),
                        "weight": int(test_row["DSID_WEIGHT"]),
                        "critical": bool(test_row["CRITICAL"]),
                    }
                )

            usecase_object["jobs"].append(
                {
                    "jobid": str(job_id),
                    "tests": tests,
                }
            )

        configuration["usecases"].append(usecase_object)

    return configuration


# ============================================================
# DATABASE SAVE
# ============================================================

def save_configuration(
    health_area_id: str,
    health_area_name: str,
    domain_id: str,
    domain_name: str,
    associated_jobs: dict[str, Any],
) -> None:
    json_text = json.dumps(
        associated_jobs,
        separators=(",", ":"),
    )

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
            ON TRIM(TO_VARCHAR(TARGET.HEALTH_AREA_ID)) =
               SOURCE.HEALTH_AREA_ID
           AND TRIM(TO_VARCHAR(TARGET.DOMAIN_ID)) =
               SOURCE.DOMAIN_ID

        WHEN MATCHED THEN
            UPDATE SET
                TARGET.HEALTH_AREA_NAME = SOURCE.HEALTH_AREA_NAME,
                TARGET.DOMAIN_NAME = SOURCE.DOMAIN_NAME,
                TARGET.ASSOCIATED_JOBS = SOURCE.ASSOCIATED_JOBS,
                TARGET.UPDATED_ON = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN
            INSERT
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
                str(health_area_id).strip(),
                str(health_area_name).strip(),
                str(domain_id).strip(),
                str(domain_name).strip(),
                json_text,
            ],
        ).collect()

        session.sql("COMMIT").collect()

    except Exception:
        session.sql("ROLLBACK").collect()
        raise


# ============================================================
# INITIAL DATA LOAD
# ============================================================

try:
    health_domains_df = load_health_domains()
    jobs_df = load_jobs()

except Exception as error:
    st.error("Unable to load Snowflake configuration tables.")
    st.exception(error)
    st.stop()


# ============================================================
# HEADER AND SIDEBAR
# ============================================================

st.markdown(
    """
    <div class="app-header">
        <div class="app-title">Health Domain Configuration</div>
        <div class="app-subtitle">
            Select a domain, add use cases and test cases, then save.
            JSON is generated automatically.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Actions")

    if st.button("Refresh Source Data", use_container_width=True):
        load_health_domains.clear()
        load_jobs.clear()
        load_tests_for_job.clear()
        load_testplan_jobid_samples.clear()
        st.rerun()

    st.caption(f"Environment ID: {ENVIRONMENT_ID}")
    st.caption(f"Page loaded: {datetime.now():%Y-%m-%d %H:%M}")


# ============================================================
# SECTION 1: DOMAIN
# ============================================================

st.subheader("1. Select Health Area and Domain")

selection_col_1, selection_col_2 = st.columns(2)

health_area_options = (
    health_domains_df[
        [
            "HEALTH_AREA_ID",
            "HEALTH_AREA_NAME",
        ]
    ]
    .drop_duplicates()
    .sort_values("HEALTH_AREA_NAME")
)

health_area_label_to_id = {
    f"{row['HEALTH_AREA_NAME']} ({row['HEALTH_AREA_ID']})":
        str(row["HEALTH_AREA_ID"])
    for _, row in health_area_options.iterrows()
}

with selection_col_1:
    selected_health_area_label = st.selectbox(
        "Health Area",
        list(health_area_label_to_id.keys()),
        index=None,
        placeholder="Select a health area",
    )

selected_health_area_id = (
    health_area_label_to_id.get(selected_health_area_label)
    if selected_health_area_label
    else None
)

if selected_health_area_id:
    selected_health_area_rows = health_domains_df[
        health_domains_df["HEALTH_AREA_ID"].astype(str)
        == str(selected_health_area_id)
    ]

    selected_health_area_name = str(
        selected_health_area_rows.iloc[0]["HEALTH_AREA_NAME"]
    )

    domain_label_to_id = {
        f"{row['DOMAIN_NAME']} ({row['DOMAIN_ID']})":
            str(row["DOMAIN_ID"])
        for _, row in selected_health_area_rows.iterrows()
    }
else:
    selected_health_area_name = ""
    domain_label_to_id = {}

with selection_col_2:
    selected_domain_label = st.selectbox(
        "Domain",
        list(domain_label_to_id.keys()),
        index=None,
        placeholder="Select a domain",
        disabled=not selected_health_area_id,
    )

selected_domain_id = (
    domain_label_to_id.get(selected_domain_label)
    if selected_domain_label
    else None
)

if selected_domain_id:
    selected_domain_row = health_domains_df[
        (
            health_domains_df["HEALTH_AREA_ID"].astype(str)
            == str(selected_health_area_id)
        )
        & (
            health_domains_df["DOMAIN_ID"].astype(str)
            == str(selected_domain_id)
        )
    ].iloc[0]

    selected_domain_name = str(
        selected_domain_row["DOMAIN_NAME"]
    )
else:
    selected_domain_name = ""

button_col_1, button_col_2, button_col_3 = st.columns([1, 1, 3])

with button_col_1:
    load_existing_button = st.button(
        "Load Existing",
        use_container_width=True,
        disabled=not (
            selected_health_area_id
            and selected_domain_id
        ),
    )

with button_col_2:
    clear_button = st.button(
        "Clear Working Copy",
        use_container_width=True,
    )

if clear_button:
    st.session_state.configuration_rows = empty_configuration_dataframe()
    st.session_state.loaded_health_area_id = None
    st.session_state.loaded_health_area_name = ""
    st.session_state.loaded_domain_id = None
    st.session_state.loaded_domain_name = ""
    st.session_state.loaded_domain_weight = 5
    st.session_state.new_usecase_name = ""
    st.session_state.new_usecase_weight = 5
    st.session_state.selected_job_label = None
    st.rerun()

if load_existing_button:
    try:
        with st.spinner("Loading existing configuration..."):
            (
                existing_domain_weight,
                existing_rows,
            ) = load_existing_configuration(
                selected_health_area_id,
                selected_domain_id,
            )

        st.session_state.configuration_rows = existing_rows
        st.session_state.loaded_health_area_id = selected_health_area_id
        st.session_state.loaded_health_area_name = selected_health_area_name
        st.session_state.loaded_domain_id = selected_domain_id
        st.session_state.loaded_domain_name = selected_domain_name
        st.session_state.loaded_domain_weight = existing_domain_weight

        st.success("Existing configuration loaded.")
        st.rerun()

    except Exception as error:
        st.error("Unable to load the existing configuration.")
        st.exception(error)


# ============================================================
# SECTION 2: DOMAIN WEIGHT
# ============================================================

st.divider()
st.subheader("2. Set Domain Weight")

domain_weight = st.slider(
    "Domain Weight",
    min_value=1,
    max_value=10,
    value=int(st.session_state.loaded_domain_weight),
    help=(
        "1 means lower importance and 10 means highest importance "
        "when calculating the Health Area Score."
    ),
)

st.session_state.loaded_domain_weight = domain_weight


# ============================================================
# SECTION 3: USE CASE AND TEST CASES
# ============================================================

st.divider()
st.subheader("3. Add a Use Case and Test Cases")

if not selected_domain_id and not st.session_state.loaded_domain_id:
    st.info("Select a Health Area and Domain first.")

else:
    effective_domain_id = (
        selected_domain_id
        or st.session_state.loaded_domain_id
    )

    generated_usecase_id = generate_next_usecase_id(
        effective_domain_id,
        st.session_state.configuration_rows,
    )

    usecase_col_1, usecase_col_2, usecase_col_3 = st.columns(
        [1, 2, 1]
    )

    with usecase_col_1:
        st.text_input(
            "Use Case ID",
            value=generated_usecase_id,
            disabled=True,
            help="Generated automatically by the application.",
        )

    with usecase_col_2:
        usecase_name = st.text_input(
            "Use Case Name",
            placeholder="Example: Members",
            key="new_usecase_name",
        )

    with usecase_col_3:
        usecase_weight = st.number_input(
            "Use Case Weight",
            min_value=1,
            max_value=10,
            value=int(st.session_state.new_usecase_weight),
            step=1,
            key="new_usecase_weight",
            help=(
                "1 means lower importance and 10 means highest importance "
                "inside this domain."
            ),
        )

    job_label_to_id = {
        f"{row['JOBNAME']} ({row['JOBID']})":
            str(row["JOBID"]).strip()
        for _, row in jobs_df.iterrows()
    }

    selected_job_label = st.selectbox(
        "Job",
        list(job_label_to_id.keys()),
        index=None,
        placeholder="Select a job",
        key="selected_job_label",
    )

    selected_job_id = (
        job_label_to_id.get(selected_job_label)
        if selected_job_label
        else None
    )

    selected_job_name = (
        selected_job_label.rsplit(" (", 1)[0]
        if selected_job_label
        else ""
    )

    selected_test_rows = pd.DataFrame()

    if selected_job_id:
        try:
            tests_df = load_tests_for_job(selected_job_id)

        except Exception as error:
            st.error("Unable to load DSIDs from DSE_TESTPLAN.")
            st.exception(error)
            tests_df = pd.DataFrame()

        if tests_df.empty:
            st.warning(
                f"No DSIDs were found in DSE_TESTPLAN for Job ID "
                f"'{selected_job_id}'."
            )

            with st.expander("Troubleshooting details", expanded=True):
                st.code(
                    f"Selected JOBID: {selected_job_id}",
                    language="text",
                )

                try:
                    st.dataframe(
                        load_testplan_jobid_samples(),
                        use_container_width=True,
                        hide_index=True,
                    )
                except Exception as error:
                    st.exception(error)

        else:
            st.markdown(
                """
                <div class="instruction-box">
                    <b>For every selected DSID:</b><br>
                    • Set <b>DSID Weight</b> from 1 to 10.
                    Use 1 for lower impact and 10 for highest impact.<br>
                    • Turn on <b>Critical</b> only when failure of that
                    DSID should strongly affect the use-case health score.
                </div>
                """,
                unsafe_allow_html=True,
            )

            dsid_search = st.text_input(
                "Search DSID or Test Description",
                placeholder="Type part of a DSID or description",
                key=f"dsid_search_{selected_job_id}",
            )

            displayed_tests_df = tests_df.copy()

            if dsid_search.strip():
                search_text = dsid_search.strip().lower()

                displayed_tests_df = displayed_tests_df[
                    displayed_tests_df["DSID"]
                    .astype(str)
                    .str.lower()
                    .str.contains(search_text, regex=False, na=False)
                    |
                    displayed_tests_df["TESTCASEDESCRIPTION"]
                    .astype(str)
                    .str.lower()
                    .str.contains(search_text, regex=False, na=False)
                ].copy()

            selection_df = displayed_tests_df.copy()
            selection_df.insert(0, "SELECTED", False)
            selection_df.insert(1, "DSID_WEIGHT", 5)
            selection_df.insert(2, "CRITICAL", False)

            edited_selection_df = st.data_editor(
                selection_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                disabled=[
                    "JOBID",
                    "DSID",
                    "TESTCASEDESCRIPTION",
                ],
                column_order=[
                    "SELECTED",
                    "DSID_WEIGHT",
                    "CRITICAL",
                    "DSID",
                    "TESTCASEDESCRIPTION",
                    "JOBID",
                ],
                column_config={
                    "SELECTED": st.column_config.CheckboxColumn(
                        "Select",
                        default=False,
                        width="small",
                        help="Select this DSID to add it.",
                    ),
                    "DSID_WEIGHT": st.column_config.NumberColumn(
                        "DSID Weight",
                        min_value=1,
                        max_value=10,
                        step=1,
                        default=5,
                        required=True,
                        width="small",
                        help="1 = lower impact; 10 = highest impact.",
                    ),
                    "CRITICAL": st.column_config.CheckboxColumn(
                        "Critical",
                        default=False,
                        width="small",
                        help=(
                            "Select when failure of this DSID should be "
                            "treated as business-critical."
                        ),
                    ),
                    "DSID": st.column_config.TextColumn(
                        "DSID",
                        disabled=True,
                        width="medium",
                    ),
                    "TESTCASEDESCRIPTION":
                        st.column_config.TextColumn(
                            "Test Case Description",
                            disabled=True,
                            width="large",
                        ),
                    "JOBID": st.column_config.TextColumn(
                        "Job ID",
                        disabled=True,
                        width="small",
                    ),
                },
                key=f"test_editor_{selected_job_id}",
            )

            selected_test_rows = edited_selection_df[
                edited_selection_df["SELECTED"] == True
            ].copy()

            st.caption(
                f"{len(selected_test_rows)} test case"
                f"{'s' if len(selected_test_rows) != 1 else ''} selected."
            )

    add_selected_button = st.button(
        "Add Selected Test Cases",
        type="primary",
        use_container_width=True,
        disabled=not selected_job_id,
    )

    if add_selected_button:
        input_errors = []

        if not usecase_name.strip():
            input_errors.append("Enter a Use Case Name.")

        if not selected_job_id:
            input_errors.append("Select a Job.")

        if selected_test_rows.empty:
            input_errors.append("Select at least one DSID.")

        if input_errors:
            for error_message in input_errors:
                st.error(error_message)

        else:
            new_rows = []

            for _, selected_test in selected_test_rows.iterrows():
                new_rows.append(
                    {
                        "USECASE_ID": generated_usecase_id,
                        "USECASE_NAME": usecase_name.strip(),
                        "USECASE_WEIGHT": int(usecase_weight),
                        "JOBID": str(selected_job_id).strip(),
                        "JOBNAME": selected_job_name,
                        "DSID": str(selected_test["DSID"]).strip(),
                        "TESTCASEDESCRIPTION": str(
                            selected_test["TESTCASEDESCRIPTION"]
                        ),
                        "DSID_WEIGHT": int(
                            selected_test["DSID_WEIGHT"]
                        ),
                        "CRITICAL": bool(
                            selected_test["CRITICAL"]
                        ),
                    }
                )

            updated_rows = pd.concat(
                [
                    st.session_state.configuration_rows,
                    pd.DataFrame(new_rows, columns=CONFIG_COLUMNS),
                ],
                ignore_index=True,
            )

            updated_rows = updated_rows.drop_duplicates(
                subset=[
                    "USECASE_ID",
                    "JOBID",
                    "DSID",
                ],
                keep="last",
            ).reset_index(drop=True)

            st.session_state.configuration_rows = updated_rows
            st.session_state.new_usecase_name = ""
            st.session_state.new_usecase_weight = 5
            st.session_state.selected_job_label = None

            st.success(
                f"Added {len(new_rows)} selected test case"
                f"{'s' if len(new_rows) != 1 else ''}."
            )
            st.rerun()


# ============================================================
# SECTION 4: REVIEW, EDIT AND SAVE
# ============================================================

st.divider()
st.subheader("4. Review, Edit and Save")

if st.session_state.configuration_rows.empty:
    st.info("No test cases have been added yet.")

else:
    edited_configuration_df = st.data_editor(
        st.session_state.configuration_rows,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_order=[
            "USECASE_ID",
            "USECASE_NAME",
            "USECASE_WEIGHT",
            "DSID_WEIGHT",
            "CRITICAL",
            "JOBID",
            "JOBNAME",
            "DSID",
            "TESTCASEDESCRIPTION",
        ],
        column_config={
            "USECASE_ID": st.column_config.TextColumn(
                "Use Case ID",
                disabled=True,
            ),
            "USECASE_NAME": st.column_config.TextColumn(
                "Use Case Name",
                required=True,
            ),
            "USECASE_WEIGHT": st.column_config.NumberColumn(
                "Use Case Weight",
                min_value=1,
                max_value=10,
                step=1,
                required=True,
            ),
            "DSID_WEIGHT": st.column_config.NumberColumn(
                "DSID Weight",
                min_value=1,
                max_value=10,
                step=1,
                required=True,
            ),
            "CRITICAL": st.column_config.CheckboxColumn(
                "Critical",
                default=False,
            ),
            "JOBID": st.column_config.TextColumn(
                "Job ID",
                disabled=True,
            ),
            "JOBNAME": st.column_config.TextColumn(
                "Job Name",
                disabled=True,
            ),
            "DSID": st.column_config.TextColumn(
                "DSID",
                disabled=True,
            ),
            "TESTCASEDESCRIPTION":
                st.column_config.TextColumn(
                    "Test Case Description",
                    disabled=True,
                    width="large",
                ),
        },
        key="working_configuration_editor",
    )

    st.session_state.configuration_rows = edited_configuration_df

    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)

    with metric_col_1:
        st.metric(
            "Use Cases",
            edited_configuration_df["USECASE_ID"].nunique(),
        )

    with metric_col_2:
        st.metric(
            "Jobs",
            edited_configuration_df["JOBID"].nunique(),
        )

    with metric_col_3:
        st.metric(
            "DSIDs",
            len(edited_configuration_df),
        )

    effective_health_area_id = (
        selected_health_area_id
        or st.session_state.loaded_health_area_id
    )

    effective_health_area_name = (
        selected_health_area_name
        or st.session_state.loaded_health_area_name
    )

    effective_domain_id = (
        selected_domain_id
        or st.session_state.loaded_domain_id
    )

    effective_domain_name = (
        selected_domain_name
        or st.session_state.loaded_domain_name
    )

    validation_errors = validate_configuration(
        st.session_state.configuration_rows
    )

    if validation_errors:
        st.error("Resolve these issues before saving:")

        for error_message in validation_errors:
            st.write(f"• {error_message}")

    elif not effective_health_area_id or not effective_domain_id:
        st.warning("Select a Health Area and Domain before saving.")

    else:
        associated_jobs = build_associated_jobs_json(
            st.session_state.loaded_domain_weight,
            st.session_state.configuration_rows,
        )

        with st.expander(
            "Technical JSON preview",
            expanded=False,
        ):
            st.caption(
                "Business users do not need to edit this JSON."
            )
            st.json(associated_jobs)

        save_button = st.button(
            "Save Configuration",
            type="primary",
            use_container_width=True,
        )

        if save_button:
            try:
                with st.spinner("Saving configuration..."):
                    save_configuration(
                        health_area_id=str(effective_health_area_id),
                        health_area_name=str(effective_health_area_name),
                        domain_id=str(effective_domain_id),
                        domain_name=str(effective_domain_name),
                        associated_jobs=associated_jobs,
                    )

                load_health_domains.clear()

                st.success(
                    "Configuration saved successfully to "
                    "DSE_HEALTH_DOMAIN."
                )

            except Exception as error:
                st.error("Unable to save the configuration.")
                st.exception(error)
