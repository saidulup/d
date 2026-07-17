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
    page_title="Health Configuration",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="collapsed",
)

session = get_active_session()

ENVIRONMENT_ID = "1"

# Use fully qualified names when required:
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
            padding: 0.95rem 1rem;
            margin: 0.5rem 0 0.85rem 0;
            line-height: 1.55;
        }

        .weight-box {
            border-left: 5px solid rgba(128, 128, 128, 0.6);
            background: rgba(128, 128, 128, 0.06);
            border-radius: 8px;
            padding: 0.85rem 1rem;
            margin: 0.45rem 0 0.85rem 0;
            line-height: 1.55;
        }

        div[data-testid="stMetric"] {
            border: 1px solid rgba(128, 128, 128, 0.25);
            border-radius: 12px;
            padding: 0.8rem;
        }

        .selection-card {
            border: 1px solid rgba(128, 128, 128, 0.25);
            border-radius: 16px;
            padding: 1rem 1.1rem;
            margin: 0.4rem 0 0.9rem 0;
            background: rgba(128, 128, 128, 0.035);
        }

        .active-card {
            border: 1px solid rgba(128, 128, 128, 0.28);
            border-radius: 16px;
            padding: 0.95rem 1.1rem;
            margin: 0.8rem 0;
            background: rgba(128, 128, 128, 0.045);
        }

        .active-title {
            font-size: 1.2rem;
            font-weight: 720;
        }

        .active-subtitle {
            opacity: 0.72;
            margin-top: 0.15rem;
        }

        .small-note {
            opacity: 0.72;
            font-size: 0.9rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# DATAFRAME STRUCTURES
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
        "usecases_df": empty_usecases_dataframe(),
        "tests_df": empty_tests_dataframe(),
        "active_health_area_id": None,
        "active_health_area_name": "",
        "active_domain_id": None,
        "active_domain_name": "",
        "domain_weight": 5,
        "selected_usecase_id": None,
        "selected_job_id": None,
        "ui_revision": 0,
        "new_usecase_form_revision": 0,
        "active_view": "home",
        "loaded_context_key": None,
        "last_dsid_usecase_id": None,
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

    normalized = str(value).strip().lower()
    return normalized in {"true", "1", "yes", "y", "t"}


def normalize_usecases(usecases_df: pd.DataFrame) -> pd.DataFrame:
    if usecases_df is None or usecases_df.empty:
        return empty_usecases_dataframe()

    rows = usecases_df.copy()

    for column_name in USECASE_COLUMNS:
        if column_name not in rows.columns:
            rows[column_name] = None

    rows["USECASE_ID"] = (
        rows["USECASE_ID"].fillna("").astype(str).str.strip()
    )
    rows["USECASE_NAME"] = (
        rows["USECASE_NAME"].fillna("").astype(str).str.strip()
    )
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
        rows[column_name] = (
            rows[column_name].fillna("").astype(str).str.strip()
        )

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

    # One configured assignment is uniquely identified by
    # Use Case + Job + DSID. Keeping only the latest row prevents
    # inflated cards and duplicate JSON entries.
    rows = rows.drop_duplicates(
        subset=["USECASE_ID", "JOBID", "DSID"],
        keep="last",
    )

    return rows[TEST_CONFIG_COLUMNS].reset_index(drop=True)


def next_ui_revision() -> None:
    st.session_state.ui_revision += 1


def weight_level(weight: int) -> str:
    numeric_weight = int(weight)

    if numeric_weight <= 2:
        return "Low"
    if numeric_weight <= 4:
        return "Below standard"
    if numeric_weight <= 6:
        return "Standard"
    if numeric_weight <= 8:
        return "High"
    return "Very high"


def safe_percentage(numerator: float, denominator: float) -> float:
    if denominator in (0, 0.0) or pd.isna(denominator):
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 1)


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
    selected_job_id = clean_text(job_id)

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


def parse_variant_object(raw_value: Any) -> dict[str, Any]:
    if raw_value is None:
        return {}

    if isinstance(raw_value, dict):
        return raw_value

    if isinstance(raw_value, str):
        return json.loads(raw_value)

    try:
        return json.loads(str(raw_value))
    except Exception as error:
        raise ValueError(
            "ASSOCIATED_JOBS could not be converted to a JSON object."
        ) from error


def load_existing_configuration(
    health_area_id: str,
    domain_id: str,
) -> tuple[int, pd.DataFrame, pd.DataFrame]:
    query = f"""
        SELECT ASSOCIATED_JOBS
        FROM {HEALTH_DOMAIN_TABLE}
        WHERE TRIM(TO_VARCHAR(HEALTH_AREA_ID)) = ?
          AND TRIM(TO_VARCHAR(DOMAIN_ID)) = ?
    """

    result = session.sql(
        query,
        params=[
            clean_text(health_area_id),
            clean_text(domain_id),
        ],
    ).collect()

    if not result or result[0]["ASSOCIATED_JOBS"] is None:
        return 5, empty_usecases_dataframe(), empty_tests_dataframe()

    configuration = parse_variant_object(result[0]["ASSOCIATED_JOBS"])
    domain_weight = int(configuration.get("domain_weight", 5))

    jobs_df = load_jobs()
    job_name_by_id = {
        clean_text(row["JOBID"]): clean_text(row["JOBNAME"])
        for _, row in jobs_df.iterrows()
    }

    usecase_records: list[dict[str, Any]] = []
    test_records: list[dict[str, Any]] = []
    description_cache: dict[str, dict[str, str]] = {}

    for usecase in configuration.get("usecases", []):
        usecase_id = clean_text(usecase.get("usecase_id"))
        usecase_name = clean_text(usecase.get("usecase_name"))
        usecase_weight = int(usecase.get("usecase_weight", 5))

        if not usecase_id:
            continue

        usecase_records.append(
            {
                "USECASE_ID": usecase_id,
                "USECASE_NAME": usecase_name,
                "USECASE_WEIGHT": usecase_weight,
            }
        )

        for job in usecase.get("jobs", []):
            job_id = clean_text(job.get("jobid"))

            if not job_id:
                continue

            if job_id not in description_cache:
                try:
                    source_tests_df = load_tests_for_job(job_id)
                    description_cache[job_id] = {
                        clean_text(row["DSID"]): clean_text(
                            row["TESTCASEDESCRIPTION"]
                        )
                        for _, row in source_tests_df.iterrows()
                    }
                except Exception:
                    description_cache[job_id] = {}

            for test in job.get("tests", []):
                dsid = clean_text(test.get("dsid"))

                if not dsid:
                    continue

                test_records.append(
                    {
                        "USECASE_ID": usecase_id,
                        "JOBID": job_id,
                        "JOBNAME": job_name_by_id.get(job_id, ""),
                        "DSID": dsid,
                        "TESTCASEDESCRIPTION": (
                            description_cache[job_id].get(dsid, "")
                        ),
                        "DSID_WEIGHT": int(test.get("weight", 5)),
                        "CRITICAL": coerce_bool(
                            test.get("critical", False)
                        ),
                    }
                )

    return (
        domain_weight,
        normalize_usecases(pd.DataFrame(usecase_records)),
        normalize_tests(pd.DataFrame(test_records)),
    )


# ============================================================
# SUMMARY AND IMPACT CALCULATIONS
# ============================================================


def build_usecase_summary(
    usecases_df: pd.DataFrame,
    tests_df: pd.DataFrame,
) -> pd.DataFrame:
    usecases = normalize_usecases(usecases_df)
    tests = normalize_tests(tests_df)

    if usecases.empty:
        return pd.DataFrame(
            columns=[
                "USECASE_ID",
                "USECASE_NAME",
                "USECASE_WEIGHT",
                "WEIGHT_LEVEL",
                "DOMAIN_INFLUENCE_PCT",
                "JOB_COUNT",
                "DSID_COUNT",
                "CRITICAL_COUNT",
            ]
        )

    total_usecase_weight = float(usecases["USECASE_WEIGHT"].sum())

    test_counts = (
        tests.groupby("USECASE_ID", dropna=False)
        .agg(
            JOB_COUNT=("JOBID", "nunique"),
            DSID_COUNT=("DSID", "count"),
            CRITICAL_COUNT=("CRITICAL", "sum"),
        )
        .reset_index()
        if not tests.empty
        else pd.DataFrame(
            columns=[
                "USECASE_ID",
                "JOB_COUNT",
                "DSID_COUNT",
                "CRITICAL_COUNT",
            ]
        )
    )

    summary = usecases.merge(
        test_counts,
        on="USECASE_ID",
        how="left",
    )

    for column_name in ["JOB_COUNT", "DSID_COUNT", "CRITICAL_COUNT"]:
        summary[column_name] = (
            pd.to_numeric(summary[column_name], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    summary["WEIGHT_LEVEL"] = summary["USECASE_WEIGHT"].apply(weight_level)
    summary["DOMAIN_INFLUENCE_PCT"] = summary["USECASE_WEIGHT"].apply(
        lambda value: safe_percentage(value, total_usecase_weight)
    )

    return summary[
        [
            "USECASE_ID",
            "USECASE_NAME",
            "USECASE_WEIGHT",
            "WEIGHT_LEVEL",
            "DOMAIN_INFLUENCE_PCT",
            "JOB_COUNT",
            "DSID_COUNT",
            "CRITICAL_COUNT",
        ]
    ]


def add_dsid_influence(
    selected_tests_df: pd.DataFrame,
    all_tests_df: pd.DataFrame,
    usecase_id: str,
) -> pd.DataFrame:
    selected_rows = selected_tests_df.copy()
    all_rows = normalize_tests(all_tests_df)

    usecase_total_weight = float(
        all_rows.loc[
            all_rows["USECASE_ID"] == clean_text(usecase_id),
            "DSID_WEIGHT",
        ].sum()
    )

    selected_rows["USECASE_INFLUENCE_PCT"] = selected_rows[
        "DSID_WEIGHT"
    ].apply(lambda value: safe_percentage(value, usecase_total_weight))

    return selected_rows


def build_review_dataframe(
    usecases_df: pd.DataFrame,
    tests_df: pd.DataFrame,
) -> pd.DataFrame:
    usecases = normalize_usecases(usecases_df)
    tests = normalize_tests(tests_df)

    if usecases.empty or tests.empty:
        return pd.DataFrame()

    usecase_summary = build_usecase_summary(usecases, tests)

    review = tests.merge(
        usecase_summary[
            [
                "USECASE_ID",
                "USECASE_NAME",
                "USECASE_WEIGHT",
                "DOMAIN_INFLUENCE_PCT",
            ]
        ],
        on="USECASE_ID",
        how="left",
    )

    usecase_dsid_weight_totals = (
        review.groupby("USECASE_ID")["DSID_WEIGHT"].transform("sum")
    )
    review["DSID_INFLUENCE_PCT"] = review.apply(
        lambda row: safe_percentage(
            row["DSID_WEIGHT"],
            usecase_dsid_weight_totals.loc[row.name],
        ),
        axis=1,
    )

    return review[
        [
            "USECASE_ID",
            "USECASE_NAME",
            "USECASE_WEIGHT",
            "DOMAIN_INFLUENCE_PCT",
            "JOBID",
            "JOBNAME",
            "DSID",
            "TESTCASEDESCRIPTION",
            "DSID_WEIGHT",
            "DSID_INFLUENCE_PCT",
            "CRITICAL",
        ]
    ].sort_values(
        ["USECASE_ID", "JOBID", "DSID"],
        kind="stable",
    )


# ============================================================
# USE CASE ID GENERATION
# ============================================================


def generate_next_usecase_id(
    domain_id: str,
    usecases_df: pd.DataFrame,
) -> str:
    clean_domain_id = re.sub(
        r"[^A-Za-z0-9]",
        "",
        clean_text(domain_id),
    )

    prefix = f"UC{clean_domain_id}-"
    highest_sequence = 0
    usecases = normalize_usecases(usecases_df)

    pattern = re.compile(
        rf"^{re.escape(prefix)}(\d+)$",
        re.IGNORECASE,
    )

    for existing_id in usecases["USECASE_ID"].tolist():
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


def validate_configuration(
    domain_weight: int,
    usecases_df: pd.DataFrame,
    tests_df: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []
    usecases = normalize_usecases(usecases_df)
    tests = normalize_tests(tests_df)

    if not 1 <= int(domain_weight) <= 10:
        errors.append("Domain Weight must be between 1 and 10.")

    if usecases.empty:
        errors.append("Create at least one Use Case.")
        return errors

    if tests.empty:
        errors.append("Add at least one DSID before saving.")

    if usecases["USECASE_NAME"].eq("").any():
        errors.append("Every Use Case must have a name.")

    if not usecases["USECASE_WEIGHT"].between(1, 10).all():
        errors.append("Every Use Case Weight must be between 1 and 10.")

    duplicate_usecase_ids = usecases.duplicated(
        subset=["USECASE_ID"],
        keep=False,
    )

    if duplicate_usecase_ids.any():
        errors.append("Use Case IDs must be unique.")

    if not tests.empty:
        if not tests["DSID_WEIGHT"].between(1, 10).all():
            errors.append("Every DSID Weight must be between 1 and 10.")

        unknown_usecases = set(tests["USECASE_ID"]) - set(
            usecases["USECASE_ID"]
        )

        if unknown_usecases:
            errors.append(
                "Some DSIDs refer to a Use Case that no longer exists."
            )

        # Prevent the same Job + DSID from being counted more than once
        # anywhere within the selected domain.
        duplicate_tests = tests.duplicated(
            subset=["JOBID", "DSID"],
            keep=False,
        )

        if duplicate_tests.any():
            errors.append(
                "The same Job ID and DSID cannot be assigned to more than "
                "one Use Case in the same domain."
            )

        dsid_counts = tests.groupby("USECASE_ID").size().to_dict()
        empty_usecase_ids = [
            usecase_id
            for usecase_id in usecases["USECASE_ID"]
            if int(dsid_counts.get(usecase_id, 0)) == 0
        ]

        if empty_usecase_ids:
            errors.append(
                "Every Use Case must contain at least one DSID. Empty: "
                + ", ".join(empty_usecase_ids)
            )

    return errors


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
            "JOBID",
            sort=False,
            dropna=False,
        ):
            test_objects = []

            for _, test_row in job_rows.iterrows():
                test_objects.append(
                    {
                        "dsid": clean_text(test_row["DSID"]),
                        "weight": int(test_row["DSID_WEIGHT"]),
                        "critical": bool(test_row["CRITICAL"]),
                    }
                )

            usecase_object["jobs"].append(
                {
                    "jobid": clean_text(job_id),
                    "tests": test_objects,
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
                clean_text(health_area_id),
                clean_text(health_area_name),
                clean_text(domain_id),
                clean_text(domain_name),
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

health_domains_df = health_domains_df.copy()
health_domains_df["HEALTH_AREA_ID"] = (
    health_domains_df["HEALTH_AREA_ID"].astype(str).str.strip()
)
health_domains_df["DOMAIN_ID"] = (
    health_domains_df["DOMAIN_ID"].astype(str).str.strip()
)

jobs_df = jobs_df.copy()
jobs_df["JOBID"] = jobs_df["JOBID"].astype(str).str.strip()
jobs_df["JOBNAME"] = jobs_df["JOBNAME"].astype(str).str.strip()

job_name_by_id = {
    clean_text(row["JOBID"]): clean_text(row["JOBNAME"])
    for _, row in jobs_df.iterrows()
}



# ============================================================
# SAVED CONFIGURATION ANALYSIS
# ============================================================


@st.cache_data(ttl=60, show_spinner=False)
def load_saved_configuration_rows() -> pd.DataFrame:
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


def configuration_counts(configuration: dict[str, Any]) -> dict[str, int]:
    raw_usecases = configuration.get("usecases", []) or []
    usecase_ids: set[str] = set()
    job_ids: set[str] = set()
    test_keys: set[tuple[str, str, str]] = set()
    critical_keys: set[tuple[str, str, str]] = set()
    tests_by_usecase: dict[str, set[tuple[str, str]]] = {}

    for usecase_index, usecase in enumerate(raw_usecases):
        usecase_id = clean_text(usecase.get("usecase_id"))
        if not usecase_id:
            usecase_id = f"__MISSING_USECASE_{usecase_index}"

        usecase_ids.add(usecase_id)
        tests_by_usecase.setdefault(usecase_id, set())

        for job in usecase.get("jobs", []) or []:
            job_id = clean_text(job.get("jobid"))
            if job_id:
                job_ids.add(job_id)

            for test in job.get("tests", []) or []:
                dsid = clean_text(test.get("dsid"))
                if not job_id or not dsid:
                    continue

                test_key = (usecase_id, job_id, dsid)
                test_keys.add(test_key)
                tests_by_usecase[usecase_id].add((job_id, dsid))

                if coerce_bool(test.get("critical", False)):
                    critical_keys.add(test_key)

    empty_usecase_count = sum(
        1
        for usecase_id in usecase_ids
        if len(tests_by_usecase.get(usecase_id, set())) == 0
    )

    return {
        "USECASE_COUNT": len(usecase_ids),
        "JOB_COUNT": len(job_ids),
        "DSID_COUNT": len(test_keys),
        "CRITICAL_COUNT": len(critical_keys),
        "EMPTY_USECASE_COUNT": empty_usecase_count,
    }

def working_configuration_counts() -> dict[str, int]:
    usecases = normalize_usecases(st.session_state.usecases_df)
    tests = normalize_tests(st.session_state.tests_df)
    dsid_counts = tests.groupby("USECASE_ID").size().to_dict() if not tests.empty else {}

    return {
        "USECASE_COUNT": len(usecases),
        "JOB_COUNT": tests["JOBID"].nunique() if not tests.empty else 0,
        "DSID_COUNT": len(tests),
        "CRITICAL_COUNT": int(tests["CRITICAL"].sum()) if not tests.empty else 0,
        "EMPTY_USECASE_COUNT": sum(
            1 for usecase_id in usecases["USECASE_ID"].tolist()
            if int(dsid_counts.get(usecase_id, 0)) == 0
        ),
    }


def configuration_status(counts: dict[str, int], json_valid: bool = True) -> str:
    if not json_valid:
        return "Invalid configuration"
    if counts["USECASE_COUNT"] == 0:
        return "Not configured"
    if counts["DSID_COUNT"] == 0 or counts["EMPTY_USECASE_COUNT"] > 0:
        return "Needs DSIDs"
    return "Ready"


def build_health_area_overview(saved_rows_df: pd.DataFrame) -> pd.DataFrame:
    area_id = clean_text(st.session_state.active_health_area_id)
    active_domain_id = clean_text(st.session_state.active_domain_id)

    area_domains = health_domains_df[
        health_domains_df["HEALTH_AREA_ID"] == area_id
    ].copy()

    saved_area_rows = saved_rows_df[
        saved_rows_df["HEALTH_AREA_ID"].astype(str).str.strip() == area_id
    ].copy()

    saved_by_domain = {
        clean_text(row["DOMAIN_ID"]): row
        for _, row in saved_area_rows.iterrows()
    }

    overview_rows: list[dict[str, Any]] = []

    for _, domain_row in area_domains.iterrows():
        domain_id = clean_text(domain_row["DOMAIN_ID"])
        domain_name = clean_text(domain_row["DOMAIN_NAME"])
        is_working_domain = domain_id == active_domain_id
        json_valid = True

        if is_working_domain:
            domain_weight = int(st.session_state.domain_weight)
            counts = working_configuration_counts()
            source = "Current working configuration"
        else:
            saved_row = saved_by_domain.get(domain_id)
            raw_configuration = saved_row["ASSOCIATED_JOBS"] if saved_row is not None else None

            if raw_configuration is None:
                configuration = {}
            else:
                try:
                    configuration = parse_variant_object(raw_configuration)
                except Exception:
                    configuration = {}
                    json_valid = False

            domain_weight = int(
                configuration.get(
                    "domain_weight",
                    domain_row.get("DOMAIN_WEIGHT", 5),
                )
            )
            counts = configuration_counts(configuration)
            source = "Saved configuration"

        overview_rows.append(
            {
                "DOMAIN_ID": domain_id,
                "DOMAIN_NAME": domain_name,
                "STATUS": configuration_status(counts, json_valid),
                "DOMAIN_WEIGHT": domain_weight,
                "HEALTH_AREA_INFLUENCE_PCT": 0.0,
                "USECASE_COUNT": counts["USECASE_COUNT"],
                "JOB_COUNT": counts["JOB_COUNT"],
                "DSID_COUNT": counts["DSID_COUNT"],
                "CRITICAL_COUNT": counts["CRITICAL_COUNT"],
                "SOURCE": source,
            }
        )

    overview_df = pd.DataFrame(overview_rows)
    if overview_df.empty:
        return overview_df

    total_weight = float(overview_df["DOMAIN_WEIGHT"].sum())
    overview_df["HEALTH_AREA_INFLUENCE_PCT"] = overview_df[
        "DOMAIN_WEIGHT"
    ].apply(lambda value: safe_percentage(value, total_weight))

    return overview_df.sort_values(
        ["DOMAIN_NAME", "DOMAIN_ID"],
        kind="stable",
    ).reset_index(drop=True)





def health_area_summary_counts(
    saved_rows_df: pd.DataFrame,
) -> dict[str, int]:
    area_id = clean_text(st.session_state.active_health_area_id)
    area_domains = health_domains_df[
        health_domains_df["HEALTH_AREA_ID"] == area_id
    ].copy()

    saved_area_rows = saved_rows_df[
        saved_rows_df["HEALTH_AREA_ID"].astype(str).str.strip() == area_id
    ].copy()
    saved_by_domain = {
        clean_text(row["DOMAIN_ID"]): row
        for _, row in saved_area_rows.iterrows()
    }

    usecase_keys: set[tuple[str, str]] = set()
    job_ids: set[str] = set()
    dsid_keys: set[tuple[str, str]] = set()
    ready_domains = 0

    for _, domain_row in area_domains.iterrows():
        domain_id = clean_text(domain_row["DOMAIN_ID"])

        if domain_id == clean_text(st.session_state.active_domain_id):
            configuration = build_associated_jobs_json(
                int(st.session_state.domain_weight),
                st.session_state.usecases_df,
                st.session_state.tests_df,
            )
        else:
            saved_row = saved_by_domain.get(domain_id)
            raw_configuration = (
                saved_row["ASSOCIATED_JOBS"]
                if saved_row is not None
                else None
            )
            try:
                configuration = (
                    parse_variant_object(raw_configuration)
                    if raw_configuration is not None
                    else {}
                )
            except Exception:
                configuration = {}

        counts = configuration_counts(configuration)
        if configuration_status(counts) == "Ready":
            ready_domains += 1

        for usecase_index, usecase in enumerate(
            configuration.get("usecases", []) or []
        ):
            usecase_id = clean_text(usecase.get("usecase_id"))
            if not usecase_id:
                usecase_id = f"__MISSING_{usecase_index}"
            usecase_keys.add((domain_id, usecase_id))

            for job in usecase.get("jobs", []) or []:
                job_id = clean_text(job.get("jobid"))
                if job_id:
                    job_ids.add(job_id)

                for test in job.get("tests", []) or []:
                    dsid = clean_text(test.get("dsid"))
                    if job_id and dsid:
                        dsid_keys.add((job_id, dsid))

    return {
        "DOMAIN_COUNT": len(area_domains),
        "READY_DOMAIN_COUNT": ready_domains,
        "USECASE_COUNT": len(usecase_keys),
        "JOB_COUNT": len(job_ids),
        "DSID_COUNT": len(dsid_keys),
    }


# ============================================================
# CLEAN PAGE NAVIGATION
# ============================================================


def go_to(page_name: str) -> None:
    st.session_state.active_view = page_name
    st.rerun()


def render_context_header(page_title: str) -> None:
    back_col, title_col = st.columns([0.8, 6.2])
    with back_col:
        if st.button("← Home", use_container_width=True, key=f"back_{page_title}"):
            go_to("home")
    with title_col:
        st.markdown(f"## {page_title}")
        st.caption(
            f"{st.session_state.active_health_area_name}  /  "
            f"{st.session_state.active_domain_name}"
        )

def open_existing_configuration(
    health_area_id: str,
    health_area_name: str,
    domain_id: str,
    domain_name: str,
) -> None:
    existing_domain_weight, existing_usecases_df, existing_tests_df = (
        load_existing_configuration(health_area_id, domain_id)
    )

    st.session_state.usecases_df = existing_usecases_df
    st.session_state.tests_df = existing_tests_df
    st.session_state.active_health_area_id = health_area_id
    st.session_state.active_health_area_name = health_area_name
    st.session_state.active_domain_id = domain_id
    st.session_state.active_domain_name = domain_name
    st.session_state.domain_weight = int(existing_domain_weight)
    st.session_state.selected_usecase_id = (
        existing_usecases_df.iloc[0]["USECASE_ID"]
        if not existing_usecases_df.empty
        else None
    )
    configured_job_ids = (
        existing_tests_df.loc[
            existing_tests_df["USECASE_ID"]
            == clean_text(st.session_state.selected_usecase_id),
            "JOBID",
        ].drop_duplicates().tolist()
        if not existing_tests_df.empty
        and st.session_state.selected_usecase_id is not None
        else []
    )
    st.session_state.selected_job_id = (
        configured_job_ids[0] if configured_job_ids else None
    )
    st.session_state.last_dsid_usecase_id = st.session_state.selected_usecase_id
    st.session_state.loaded_context_key = (
        f"{clean_text(health_area_id)}|{clean_text(domain_id)}"
    )
    st.session_state.active_view = "home"
    next_ui_revision()


def start_new_configuration(
    health_area_id: str,
    health_area_name: str,
    domain_id: str,
    domain_name: str,
    default_domain_weight: int,
) -> None:
    st.session_state.usecases_df = empty_usecases_dataframe()
    st.session_state.tests_df = empty_tests_dataframe()
    st.session_state.active_health_area_id = health_area_id
    st.session_state.active_health_area_name = health_area_name
    st.session_state.active_domain_id = domain_id
    st.session_state.active_domain_name = domain_name
    st.session_state.domain_weight = int(default_domain_weight)
    st.session_state.selected_usecase_id = None
    st.session_state.selected_job_id = None
    st.session_state.last_dsid_usecase_id = None
    st.session_state.loaded_context_key = (
        f"{clean_text(health_area_id)}|{clean_text(domain_id)}"
    )
    st.session_state.active_view = "home"
    next_ui_revision()


def render_home() -> None:
    st.markdown("# Health Configuration")

    health_area_options_df = (
        health_domains_df[["HEALTH_AREA_ID", "HEALTH_AREA_NAME"]]
        .drop_duplicates()
        .sort_values("HEALTH_AREA_NAME")
        .reset_index(drop=True)
    )
    health_area_ids = health_area_options_df["HEALTH_AREA_ID"].tolist()
    health_area_name_by_id = {
        clean_text(row["HEALTH_AREA_ID"]): clean_text(row["HEALTH_AREA_NAME"])
        for _, row in health_area_options_df.iterrows()
    }

    if not health_area_ids:
        st.info("No Health Areas found.")
        return

    selector_col_1, selector_col_2, selector_col_3 = st.columns([1.2, 1.5, 0.8])

    with selector_col_1:
        selected_health_area_id = st.selectbox(
            "Health Area",
            options=health_area_ids,
            index=0,
            format_func=lambda value: health_area_name_by_id.get(
                clean_text(value), ""
            ),
            key="health_area_selector",
        )

    selected_area_domains_df = health_domains_df[
        health_domains_df["HEALTH_AREA_ID"]
        == clean_text(selected_health_area_id)
    ].copy().sort_values("DOMAIN_NAME").reset_index(drop=True)

    selected_domain_ids = selected_area_domains_df["DOMAIN_ID"].tolist()
    domain_name_by_id = {
        clean_text(row["DOMAIN_ID"]): clean_text(row["DOMAIN_NAME"])
        for _, row in selected_area_domains_df.iterrows()
    }

    with selector_col_2:
        selected_domain_id = st.selectbox(
            "Domain",
            options=selected_domain_ids,
            index=0 if selected_domain_ids else None,
            placeholder="Select Domain",
            disabled=not selected_domain_ids,
            format_func=lambda value: domain_name_by_id.get(
                clean_text(value), ""
            ),
            key=f"domain_selector_{clean_text(selected_health_area_id)}",
        )

    with selector_col_3:
        st.text_input(
            "Domain ID",
            value=clean_text(selected_domain_id),
            disabled=True,
            placeholder="—",
        )

    if not selected_domain_id:
        return

    selected_health_area_name = health_area_name_by_id.get(
        clean_text(selected_health_area_id), ""
    )
    selected_domain_name = domain_name_by_id.get(
        clean_text(selected_domain_id), ""
    )
    selected_context_key = (
        f"{clean_text(selected_health_area_id)}|{clean_text(selected_domain_id)}"
    )

    # Selecting a Health Area or Domain automatically reloads that domain's
    # saved configuration. No Open or Refresh button is required.
    if st.session_state.loaded_context_key != selected_context_key:
        try:
            with st.spinner("Loading configuration..."):
                open_existing_configuration(
                    clean_text(selected_health_area_id),
                    selected_health_area_name,
                    clean_text(selected_domain_id),
                    selected_domain_name,
                )
            st.rerun()
        except Exception as error:
            st.error("Unable to load the selected configuration.")
            st.exception(error)
            return

    counts = working_configuration_counts()

    metric_1, metric_2, metric_3, metric_4, metric_5 = st.columns(5)
    with metric_1:
        st.metric("Domain Weight", int(st.session_state.domain_weight))
    with metric_2:
        st.metric("Use Cases", counts["USECASE_COUNT"])
    with metric_3:
        st.metric("Unique Jobs", counts["JOB_COUNT"])
    with metric_4:
        st.metric("Configured DSIDs", counts["DSID_COUNT"])
    with metric_5:
        st.metric("Critical DSIDs", counts["CRITICAL_COUNT"])

    nav_col_1, nav_col_2 = st.columns(2)
    with nav_col_1:
        if st.button(
            "Configuration",
            type="primary",
            use_container_width=True,
            help="Manage Use Cases and DSIDs together.",
        ):
            go_to("configuration")
    with nav_col_2:
        if st.button(
            "Health Analysis",
            use_container_width=True,
            help="Review the selected Health Area and save this Domain.",
        ):
            go_to("analysis")

def render_usecases(embedded: bool = False) -> None:
    if not embedded:
        render_context_header("Use Cases")

    active_area_domains_df = health_domains_df[
        health_domains_df["HEALTH_AREA_ID"]
        == clean_text(st.session_state.active_health_area_id)
    ].copy()
    active_area_domains_df["EFFECTIVE_WEIGHT"] = pd.to_numeric(
        active_area_domains_df["DOMAIN_WEIGHT"], errors="coerce"
    ).fillna(5)
    active_area_domains_df.loc[
        active_area_domains_df["DOMAIN_ID"]
        == clean_text(st.session_state.active_domain_id),
        "EFFECTIVE_WEIGHT",
    ] = int(st.session_state.domain_weight)

    domain_total_weight = float(active_area_domains_df["EFFECTIVE_WEIGHT"].sum())
    domain_share = safe_percentage(st.session_state.domain_weight, domain_total_weight)

    weight_col, influence_col = st.columns([3, 1])
    with weight_col:
        st.slider(
            "Domain Weight",
            min_value=1,
            max_value=10,
            step=1,
            key="domain_weight",
            help="Relative importance of this domain inside the selected Health Area.",
        )
    with influence_col:
        st.metric("Health Area Influence", f"{domain_share:.1f}%")

    usecases_df = normalize_usecases(st.session_state.usecases_df)
    tests_df = normalize_tests(st.session_state.tests_df)
    st.session_state.usecases_df = usecases_df
    st.session_state.tests_df = tests_df
    usecase_summary_df = build_usecase_summary(usecases_df, tests_df)

    if usecase_summary_df.empty:
        st.info("No Use Cases configured.")
    else:
        editor_df = usecase_summary_df.copy()
        editor_df.insert(0, "REMOVE", False)

        edited_df = st.data_editor(
            editor_df,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_order=[
                "REMOVE",
                "USECASE_WEIGHT",
                "DOMAIN_INFLUENCE_PCT",
                "USECASE_NAME",
                "USECASE_ID",
                "JOB_COUNT",
                "DSID_COUNT",
                "CRITICAL_COUNT",
            ],
            disabled=[
                "USECASE_ID",
                "DOMAIN_INFLUENCE_PCT",
                "JOB_COUNT",
                "DSID_COUNT",
                "CRITICAL_COUNT",
            ],
            column_config={
                "REMOVE": st.column_config.CheckboxColumn(
                    "Remove",
                    default=False,
                    width="small",
                    help="Also removes DSIDs assigned to this Use Case.",
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
                "DOMAIN_INFLUENCE_PCT": st.column_config.NumberColumn(
                    "Influence %",
                    format="%.1f%%",
                    width="small",
                    help="Calculated from all Use Case weights in this Domain.",
                ),
                "USECASE_NAME": st.column_config.TextColumn(
                    "Use Case Name", required=True, width="large"
                ),
                "USECASE_ID": st.column_config.TextColumn("Use Case ID", width="medium"),
                "JOB_COUNT": st.column_config.NumberColumn("Jobs", width="small"),
                "DSID_COUNT": st.column_config.NumberColumn("DSIDs", width="small"),
                "CRITICAL_COUNT": st.column_config.NumberColumn("Critical", width="small"),
            },
            key=f"usecase_editor_{st.session_state.ui_revision}",
        )

        if st.button(
            "Apply Changes",
            type="primary",
            use_container_width=True,
            key="apply_usecase_changes",
        ):
            edited_rows = edited_df.copy()
            edited_rows["USECASE_NAME"] = (
                edited_rows["USECASE_NAME"].fillna("").astype(str).str.strip()
            )
            edited_rows["USECASE_WEIGHT"] = pd.to_numeric(
                edited_rows["USECASE_WEIGHT"], errors="coerce"
            )

            errors: list[str] = []
            if edited_rows["USECASE_NAME"].eq("").any():
                errors.append("Use Case Name cannot be empty.")
            if (
                edited_rows["USECASE_WEIGHT"].isna().any()
                or (~edited_rows["USECASE_WEIGHT"].between(1, 10)).any()
            ):
                errors.append("Every Use Case Weight must be between 1 and 10.")

            if errors:
                for message in errors:
                    st.error(message)
            else:
                remove_ids = set(
                    edited_rows.loc[
                        edited_rows["REMOVE"] == True, "USECASE_ID"
                    ].astype(str)
                )
                kept_usecases_df = edited_rows[
                    edited_rows["REMOVE"] != True
                ][USECASE_COLUMNS].copy()
                kept_usecases_df["USECASE_WEIGHT"] = kept_usecases_df[
                    "USECASE_WEIGHT"
                ].astype(int)

                st.session_state.usecases_df = normalize_usecases(kept_usecases_df)
                st.session_state.tests_df = normalize_tests(
                    tests_df[~tests_df["USECASE_ID"].isin(remove_ids)]
                )

                if st.session_state.selected_usecase_id in remove_ids:
                    remaining_ids = st.session_state.usecases_df["USECASE_ID"].tolist()
                    st.session_state.selected_usecase_id = (
                        remaining_ids[0] if remaining_ids else None
                    )

                next_ui_revision()
                st.rerun()

    with st.expander("Add Use Case", expanded=usecase_summary_df.empty):
        next_usecase_id = generate_next_usecase_id(
            st.session_state.active_domain_id,
            st.session_state.usecases_df,
        )

        with st.form(
            key="new_usecase_form_" + str(st.session_state.new_usecase_form_revision)
        ):
            form_col_1, form_col_2, form_col_3 = st.columns([1, 2, 1])
            with form_col_1:
                st.text_input("Use Case ID", value=next_usecase_id, disabled=True)
            with form_col_2:
                new_usecase_name = st.text_input("Use Case Name")
            with form_col_3:
                new_usecase_weight = st.number_input(
                    "Weight",
                    min_value=1,
                    max_value=10,
                    value=5,
                    step=1,
                    help="Relative importance inside this Domain.",
                )

            create_button = st.form_submit_button(
                "Add Use Case", type="primary", use_container_width=True
            )

        if create_button:
            if not new_usecase_name.strip():
                st.error("Enter a Use Case Name.")
            else:
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
                st.session_state.new_usecase_form_revision += 1
                next_ui_revision()
                st.rerun()


def render_dsids(embedded: bool = False) -> None:
    if not embedded:
        render_context_header("DSID Configuration")

    usecases_df = normalize_usecases(st.session_state.usecases_df)
    tests_df = normalize_tests(st.session_state.tests_df)

    if usecases_df.empty:
        st.info("Create a Use Case before adding DSIDs.")
        return

    usecase_ids = usecases_df["USECASE_ID"].tolist()
    usecase_name_by_id = {
        clean_text(row["USECASE_ID"]): clean_text(row["USECASE_NAME"])
        for _, row in usecases_df.iterrows()
    }

    if st.session_state.selected_usecase_id not in usecase_ids:
        st.session_state.selected_usecase_id = usecase_ids[0]

    valid_job_ids = jobs_df["JOBID"].tolist()

    # When the Use Case changes, default to its first configured Job so the
    # existing DSIDs are displayed immediately.
    if (
        st.session_state.last_dsid_usecase_id
        != st.session_state.selected_usecase_id
    ):
        configured_jobs = tests_df.loc[
            tests_df["USECASE_ID"]
            == clean_text(st.session_state.selected_usecase_id),
            "JOBID",
        ].drop_duplicates().tolist()
        st.session_state.selected_job_id = (
            configured_jobs[0] if configured_jobs else None
        )
        st.session_state.last_dsid_usecase_id = (
            st.session_state.selected_usecase_id
        )

    if st.session_state.selected_job_id not in valid_job_ids:
        st.session_state.selected_job_id = None

    selector_col_1, selector_col_2 = st.columns(2)
    with selector_col_1:
        selected_usecase_id = st.selectbox(
            "Use Case",
            options=usecase_ids,
            format_func=lambda value: (
                f"{usecase_name_by_id.get(clean_text(value), '')} "
                f"({clean_text(value)})"
            ),
            key="selected_usecase_id",
        )
    with selector_col_2:
        selected_job_id = st.selectbox(
            "Job",
            options=valid_job_ids,
            index=None,
            placeholder="Select a Job",
            format_func=lambda value: (
                f"{job_name_by_id.get(clean_text(value), '')} "
                f"({clean_text(value)})"
            ),
            key="selected_job_id",
        )

    if not selected_job_id:
        return

    try:
        source_tests_df = load_tests_for_job(clean_text(selected_job_id))
    except Exception as error:
        st.error("Unable to load DSIDs for this Job.")
        st.exception(error)
        return

    source_tests_df = source_tests_df.copy()
    if not source_tests_df.empty:
        source_tests_df["JOBID"] = source_tests_df["JOBID"].astype(str).str.strip()
        source_tests_df["DSID"] = source_tests_df["DSID"].astype(str).str.strip()
        source_tests_df["TESTCASEDESCRIPTION"] = (
            source_tests_df["TESTCASEDESCRIPTION"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

    if source_tests_df.empty:
        st.warning("No DSIDs were found for this Job in DSE_TESTPLAN.")
        with st.expander("Job ID diagnostics"):
            try:
                st.dataframe(
                    load_testplan_jobid_samples(),
                    use_container_width=True,
                    hide_index=True,
                )
            except Exception as error:
                st.exception(error)
        return

    configured_tab, add_tab = st.tabs(["Configured DSIDs", "Add DSIDs"])

    current_scope_mask = (
        (tests_df["USECASE_ID"] == clean_text(selected_usecase_id))
        & (tests_df["JOBID"] == clean_text(selected_job_id))
    )
    configured_df = tests_df[current_scope_mask].copy()

    with configured_tab:
        if configured_df.empty:
            st.info("No DSIDs configured for this Use Case and Job.")
        else:
            editor_df = add_dsid_influence(
                configured_df,
                tests_df,
                clean_text(selected_usecase_id),
            )
            editor_df.insert(0, "REMOVE", False)

            edited_df = st.data_editor(
                editor_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_order=[
                    "REMOVE",
                    "DSID_WEIGHT",
                    "USECASE_INFLUENCE_PCT",
                    "CRITICAL",
                    "DSID",
                    "TESTCASEDESCRIPTION",
                    "JOBID",
                ],
                disabled=[
                    "USECASE_INFLUENCE_PCT",
                    "DSID",
                    "TESTCASEDESCRIPTION",
                    "JOBID",
                    "JOBNAME",
                    "USECASE_ID",
                ],
                column_config={
                    "REMOVE": st.column_config.CheckboxColumn(
                        "Remove", default=False, width="small"
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
                    "USECASE_INFLUENCE_PCT": st.column_config.NumberColumn(
                        "Influence %",
                        format="%.1f%%",
                        width="small",
                        help="Calculated from all DSID weights in this Use Case.",
                    ),
                    "CRITICAL": st.column_config.CheckboxColumn(
                        "Critical",
                        default=False,
                        width="small",
                        help="Use only when this test failure must be treated as business critical.",
                    ),
                    "DSID": st.column_config.TextColumn("DSID", width="medium"),
                    "TESTCASEDESCRIPTION": st.column_config.TextColumn(
                        "Test Case Description", width="large"
                    ),
                    "JOBID": st.column_config.TextColumn("Job ID", width="small"),
                },
                key=(
                    "configured_dsid_editor_"
                    f"{selected_usecase_id}_{selected_job_id}_"
                    f"{st.session_state.ui_revision}"
                ),
            )

            if st.button(
                "Apply Changes",
                type="primary",
                use_container_width=True,
                key="apply_configured_dsid_changes",
            ):
                edited_df["DSID_WEIGHT"] = pd.to_numeric(
                    edited_df["DSID_WEIGHT"], errors="coerce"
                )
                if (
                    edited_df["DSID_WEIGHT"].isna().any()
                    or (~edited_df["DSID_WEIGHT"].between(1, 10)).any()
                ):
                    st.error("Every DSID Weight must be between 1 and 10.")
                else:
                    kept_scope_df = edited_df[
                        edited_df["REMOVE"] != True
                    ][TEST_CONFIG_COLUMNS].copy()
                    kept_scope_df["DSID_WEIGHT"] = kept_scope_df[
                        "DSID_WEIGHT"
                    ].astype(int)
                    outside_scope_df = tests_df[~current_scope_mask].copy()
                    st.session_state.tests_df = normalize_tests(
                        pd.concat(
                            [outside_scope_df, kept_scope_df],
                            ignore_index=True,
                        )
                    )
                    next_ui_revision()
                    st.rerun()

    with add_tab:
        assigned_elsewhere_df = tests_df[
            (tests_df["JOBID"] == clean_text(selected_job_id))
            & (tests_df["USECASE_ID"] != clean_text(selected_usecase_id))
        ].copy()

        if not assigned_elsewhere_df.empty:
            assigned_elsewhere_df = assigned_elsewhere_df.merge(
                usecases_df[["USECASE_ID", "USECASE_NAME"]],
                on="USECASE_ID",
                how="left",
            )
            with st.expander("DSIDs used by another Use Case"):
                st.dataframe(
                    assigned_elsewhere_df[
                        [
                            "DSID",
                            "TESTCASEDESCRIPTION",
                            "USECASE_NAME",
                            "DSID_WEIGHT",
                            "CRITICAL",
                        ]
                    ].sort_values(["USECASE_NAME", "DSID"]),
                    use_container_width=True,
                    hide_index=True,
                )

        configured_dsids_for_job = set(
            tests_df.loc[
                tests_df["JOBID"] == clean_text(selected_job_id),
                "DSID",
            ].astype(str)
        )
        available_df = source_tests_df[
            ~source_tests_df["DSID"].isin(configured_dsids_for_job)
        ].copy()

        search_text = st.text_input(
            "Search",
            placeholder="DSID or test description",
            key=f"available_dsid_search_{selected_usecase_id}_{selected_job_id}",
        ).strip().lower()

        if search_text:
            available_df = available_df[
                available_df["DSID"].astype(str).str.lower().str.contains(
                    search_text, regex=False, na=False
                )
                |
                available_df["TESTCASEDESCRIPTION"]
                .astype(str)
                .str.lower()
                .str.contains(search_text, regex=False, na=False)
            ].copy()

        if available_df.empty:
            st.info("No additional DSIDs available.")
        else:
            available_editor_df = available_df.copy()
            available_editor_df.insert(0, "SELECTED", False)
            available_editor_df.insert(1, "DSID_WEIGHT", 5)
            available_editor_df.insert(2, "CRITICAL", False)

            edited_available_df = st.data_editor(
                available_editor_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_order=[
                    "SELECTED",
                    "DSID_WEIGHT",
                    "CRITICAL",
                    "DSID",
                    "TESTCASEDESCRIPTION",
                    "JOBID",
                ],
                disabled=["DSID", "TESTCASEDESCRIPTION", "JOBID"],
                column_config={
                    "SELECTED": st.column_config.CheckboxColumn(
                        "Select", default=False, width="small"
                    ),
                    "DSID_WEIGHT": st.column_config.NumberColumn(
                        "Weight",
                        min_value=1,
                        max_value=10,
                        step=1,
                        default=5,
                        required=True,
                        width="small",
                        help="Relative importance inside this Use Case.",
                    ),
                    "CRITICAL": st.column_config.CheckboxColumn(
                        "Critical",
                        default=False,
                        width="small",
                        help="Use only when this test failure must be treated as business critical.",
                    ),
                    "DSID": st.column_config.TextColumn("DSID", width="medium"),
                    "TESTCASEDESCRIPTION": st.column_config.TextColumn(
                        "Test Case Description", width="large"
                    ),
                    "JOBID": st.column_config.TextColumn("Job ID", width="small"),
                },
                key=(
                    "available_dsid_editor_"
                    f"{selected_usecase_id}_{selected_job_id}_"
                    f"{st.session_state.ui_revision}"
                ),
            )

            selected_rows = edited_available_df[
                edited_available_df["SELECTED"] == True
            ].copy()

            if st.button(
                "Add Selected DSIDs",
                type="primary",
                use_container_width=True,
                key="add_selected_dsids",
            ):
                errors: list[str] = []
                if selected_rows.empty:
                    errors.append("Select at least one DSID.")

                selected_rows["DSID_WEIGHT"] = pd.to_numeric(
                    selected_rows["DSID_WEIGHT"], errors="coerce"
                )
                if (
                    selected_rows["DSID_WEIGHT"].isna().any()
                    or (~selected_rows["DSID_WEIGHT"].between(1, 10)).any()
                ):
                    errors.append("Every selected DSID Weight must be between 1 and 10.")

                if errors:
                    for message in errors:
                        st.error(message)
                else:
                    new_rows = []
                    for _, selected_test in selected_rows.iterrows():
                        new_rows.append(
                            {
                                "USECASE_ID": clean_text(selected_usecase_id),
                                "JOBID": clean_text(selected_job_id),
                                "JOBNAME": job_name_by_id.get(
                                    clean_text(selected_job_id), ""
                                ),
                                "DSID": clean_text(selected_test["DSID"]),
                                "TESTCASEDESCRIPTION": clean_text(
                                    selected_test["TESTCASEDESCRIPTION"]
                                ),
                                "DSID_WEIGHT": int(selected_test["DSID_WEIGHT"]),
                                "CRITICAL": coerce_bool(selected_test["CRITICAL"]),
                            }
                        )

                    st.session_state.tests_df = normalize_tests(
                        pd.concat(
                            [
                                tests_df,
                                pd.DataFrame(new_rows, columns=TEST_CONFIG_COLUMNS),
                            ],
                            ignore_index=True,
                        )
                    )
                    next_ui_revision()
                    st.rerun()


def render_configuration() -> None:
    render_context_header("Configuration")

    st.markdown("### Use Cases")
    render_usecases(embedded=True)

    st.divider()
    st.markdown("### DSIDs")
    render_dsids(embedded=True)


def render_analysis() -> None:
    render_context_header("Health Analysis")

    try:
        saved_rows_df = load_saved_configuration_rows()
    except Exception as error:
        st.error("Unable to load saved configurations.")
        st.exception(error)
        return

    area_overview_df = build_health_area_overview(saved_rows_df)
    working_counts = working_configuration_counts()

    area_counts = health_area_summary_counts(saved_rows_df)

    metric_1, metric_2, metric_3, metric_4, metric_5 = st.columns(5)
    with metric_1:
        st.metric("Domains", area_counts["DOMAIN_COUNT"])
    with metric_2:
        st.metric("Ready", area_counts["READY_DOMAIN_COUNT"])
    with metric_3:
        st.metric("Use Cases", area_counts["USECASE_COUNT"])
    with metric_4:
        st.metric("Unique Jobs", area_counts["JOB_COUNT"])
    with metric_5:
        st.metric("Unique DSIDs", area_counts["DSID_COUNT"])

    st.dataframe(
        area_overview_df,
        use_container_width=True,
        hide_index=True,
        column_order=[
            "DOMAIN_NAME",
            "STATUS",
            "DOMAIN_WEIGHT",
            "HEALTH_AREA_INFLUENCE_PCT",
            "USECASE_COUNT",
            "JOB_COUNT",
            "DSID_COUNT",
            "CRITICAL_COUNT",
        ],
        column_config={
            "DOMAIN_NAME": st.column_config.TextColumn("Domain", width="medium"),
            "STATUS": st.column_config.TextColumn("Status", width="small"),
            "DOMAIN_WEIGHT": st.column_config.NumberColumn(
                "Weight",
                width="small",
                help="Relative importance inside the Health Area.",
            ),
            "HEALTH_AREA_INFLUENCE_PCT": st.column_config.NumberColumn(
                "Influence %",
                format="%.1f%%",
                width="small",
                help="Calculated from all Domain weights in the Health Area.",
            ),
            "USECASE_COUNT": st.column_config.NumberColumn("Use Cases", width="small"),
            "JOB_COUNT": st.column_config.NumberColumn("Jobs", width="small"),
            "DSID_COUNT": st.column_config.NumberColumn("DSIDs", width="small"),
            "CRITICAL_COUNT": st.column_config.NumberColumn("Critical", width="small"),
        },
    )

    st.markdown("### Selected Domain")
    selected_domain_share = 0.0
    if not area_overview_df.empty:
        selected_row = area_overview_df[
            area_overview_df["DOMAIN_ID"]
            == clean_text(st.session_state.active_domain_id)
        ]
        if not selected_row.empty:
            selected_domain_share = float(
                selected_row.iloc[0]["HEALTH_AREA_INFLUENCE_PCT"]
            )

    selected_1, selected_2, selected_3, selected_4, selected_5 = st.columns(5)
    with selected_1:
        st.metric("Domain Weight", int(st.session_state.domain_weight))
    with selected_2:
        st.metric("Influence", f"{selected_domain_share:.1f}%")
    with selected_3:
        st.metric("Use Cases", working_counts["USECASE_COUNT"])
    with selected_4:
        st.metric("Jobs", working_counts["JOB_COUNT"])
    with selected_5:
        st.metric("DSIDs", working_counts["DSID_COUNT"])

    usecases_df = normalize_usecases(st.session_state.usecases_df)
    tests_df = normalize_tests(st.session_state.tests_df)
    usecase_summary_df = build_usecase_summary(usecases_df, tests_df)
    review_df = build_review_dataframe(usecases_df, tests_df)

    if not usecase_summary_df.empty:
        table_col, chart_col = st.columns([3, 2])
        with table_col:
            st.dataframe(
                usecase_summary_df,
                use_container_width=True,
                hide_index=True,
                column_order=[
                    "USECASE_NAME",
                    "USECASE_WEIGHT",
                    "DOMAIN_INFLUENCE_PCT",
                    "JOB_COUNT",
                    "DSID_COUNT",
                    "CRITICAL_COUNT",
                ],
                column_config={
                    "USECASE_NAME": "Use Case",
                    "USECASE_WEIGHT": st.column_config.NumberColumn(
                        "Weight", width="small"
                    ),
                    "DOMAIN_INFLUENCE_PCT": st.column_config.NumberColumn(
                        "Influence %", format="%.1f%%", width="small"
                    ),
                    "JOB_COUNT": "Jobs",
                    "DSID_COUNT": "DSIDs",
                    "CRITICAL_COUNT": "Critical",
                },
            )
        with chart_col:
            st.bar_chart(
                usecase_summary_df[
                    ["USECASE_NAME", "DOMAIN_INFLUENCE_PCT"]
                ].set_index("USECASE_NAME"),
                use_container_width=True,
            )

    if not review_df.empty:
        with st.expander("DSID Details"):
            st.dataframe(
                review_df,
                use_container_width=True,
                hide_index=True,
                column_order=[
                    "USECASE_NAME",
                    "JOBNAME",
                    "DSID",
                    "TESTCASEDESCRIPTION",
                    "DSID_WEIGHT",
                    "DSID_INFLUENCE_PCT",
                    "CRITICAL",
                ],
                column_config={
                    "USECASE_NAME": "Use Case",
                    "JOBNAME": "Job",
                    "DSID": "DSID",
                    "TESTCASEDESCRIPTION": st.column_config.TextColumn(
                        "Test Case Description", width="large"
                    ),
                    "DSID_WEIGHT": st.column_config.NumberColumn(
                        "Weight", width="small"
                    ),
                    "DSID_INFLUENCE_PCT": st.column_config.NumberColumn(
                        "Influence %", format="%.1f%%", width="small"
                    ),
                    "CRITICAL": st.column_config.CheckboxColumn("Critical"),
                },
            )

    validation_errors = validate_configuration(
        int(st.session_state.domain_weight),
        usecases_df,
        tests_df,
    )

    st.divider()
    if validation_errors:
        for message in validation_errors:
            st.warning(message)
    else:
        associated_jobs = build_associated_jobs_json(
            int(st.session_state.domain_weight),
            usecases_df,
            tests_df,
        )

        if st.button(
            "Save Configuration",
            type="primary",
            use_container_width=True,
            key="save_configuration",
        ):
            try:
                with st.spinner("Saving..."):
                    save_configuration(
                        health_area_id=clean_text(
                            st.session_state.active_health_area_id
                        ),
                        health_area_name=clean_text(
                            st.session_state.active_health_area_name
                        ),
                        domain_id=clean_text(st.session_state.active_domain_id),
                        domain_name=clean_text(st.session_state.active_domain_name),
                        associated_jobs=associated_jobs,
                    )
                load_health_domains.clear()
                load_saved_configuration_rows.clear()
                st.success("Configuration saved.")
            except Exception as error:
                st.error("Unable to save the configuration.")
                st.exception(error)


# ============================================================
# ROUTER
# ============================================================

if st.session_state.active_view != "home" and not st.session_state.active_domain_id:
    st.session_state.active_view = "home"

if st.session_state.active_view == "configuration":
    render_configuration()
elif st.session_state.active_view == "analysis":
    render_analysis()
else:
    render_home()
