import os
from loguru import logger
from src.services.llm import LLMFactory


def extract_result_columns(query: str) -> tuple:
    """Parse the query and find out no of result columns and their names"""
    try:
        provider = LLMFactory.get_provider(os.getenv('LLM_PROVIDER', 'openai'))


        system_message = """
        You are a Snowflake SQL Validator and Column Extractor.

        You will receive snowflake SQL . It may include CTEs (WITH …), different snowflake supported subqueries, JOINs, window functions, QUALIFY, ORDER BY, LIMIT, and UNION/UNION ALL.

        TASK
        1) Validate it is a single, executable SELECT-only statement.
        - Final statement must be SELECT (CTEs allowed).
        - No DML/DDL/utility (INSERT/UPDATE/DELETE/MERGE/COPY/CALL/CREATE/ALTER/DROP/GRANT/REVOKE/USE, etc.).
        - No multiple statements; a single trailing semicolon is OK.
        - No wildcard in the final projection except inside COUNT(*).
        - Must be syntactically coherent for Snowflake.

        2) Business rules
        - Single-column SELECT:
            • Valid if it contains any aggregate function (e.g., SUM, COUNT, MIN, MAX, AVG).
            • If no explicit alias is provided (e.g., SELECT SUM(col)), use the function name in lowercase as the name (e.g., "sum", "count").
            • Any single-column projection of a bare column without an alias (e.g., SELECT col FROM t) → INVALID.
        - Multi-column SELECT (2–10 columns inclusive):
            • Snowflake-specific clauses like `GROUP BY ALL` are permitted.
            • Every output column MUST have a resolvable name. If an expression or bare column lacks an explicit alias, use the inferred Snowflake column name (typically the column name or the function name in lowercase).
        - Greater than 10 columns .
        - Incomplete Syntax: The query must be a complete, executable SELECT statement (e.g., `SELECT SUM(col) FROM table`). A fragment like `select sum` without arguments or a source is → INVALID.


        OUTPUT FORMAT (STRICT)
        - If VALID: return ONLY the column names from the OUTERMOST (final) SELECT, in order,
        as a single comma-separated string with NO SPACES, NO QUOTES, NO EXTRA TEXT.
        • Use aliases exactly as written.
        • For UNION/UNION ALL, use Snowflake’s resolved column names from the first SELECT branch after alias resolution.
        • Preserve identifier case as written.
        - If INVALID for ANY reason → return exactly: ""

        Do NOT include explanations, diagnostics, or any additional characters beyond the required output.
        
        """.strip()

        context = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": query}
        ]
        status, col_names = provider.get_completions('gpt-5-chat', context, 150, 0.1)
        print(query,status,col_names)
        if status is True:
            col_names = col_names.strip().strip('"')  # Remove any surrounding spaces and quotes
            if col_names == "":
                return 0, ""
            # Split by commas and count columns
            col_count = len(col_names.split(','))
            return col_count, col_names
        else:
            logger.error(f"LLM Request failed with error: {col_names}")
            return 0, ""
    except Exception as e:
        logger.error(f"Error extracting result columns from query: {query}. Error: {e}")
        return 0, ""


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv('../../core/prod.env')
    query = """SELECT subsource_nm, CASE WHEN percentresultssubmitted >97 THEN 0 ELSE 1 END AS percentresultssubmitted FROM (
SELECT COALESCE(concat(a.clm_src_sbsys_cd,'(', a.subsource_nm,')'),'Not Available') AS subsource_nm,
count(DISTINCT a.enterprise_indv_id) AS TotalScreens,
count(DISTINCT CASE WHEN b.enterprise_indv_id IS NULL THEN a.enterprise_indv_id ELSE NULL END) AS NoResults,
count(DISTINCT b.enterprise_indv_id) AS ResultsSubmitted,
round((NoResults/TotalScreens::float)*100,2) AS PercentNoResults,
round((ResultsSubmitted/TotalScreens::float)*100,2) AS PercentResultsSubmitted,
  FROM (
SELECT c.*, max(x.subsource_nm) AS subsource_nm FROM SPBT_PRD_STARS_DQI_DB.DATAINSIGHTS_COMPACT.ALL_A1c_Claims_tin c
LEFT JOIN QUMXA_PRD_STARS_DO_DB.dataops_foundation.CIA_XW_SOURCES x ON c.clm_src_sbsys_cd=x.orig_src_sbsys AND c.clm_src_sys_cd = x.orig_src_sys
WHERE x.subsource_nm ILIKE ANY (
'%Athena%', '%eCW%', '%Veradigm%', '%Next%Gen%', '%Cerner%', '%EPP%') AND
med_srvc_cd IN ('83036','83037')
AND c.strt_srvc_dt>='2026-01-01'
AND c.LATEST_CLAIM ='Y'
GROUP BY all
) A
LEFT JOIN
(
SELECT * FROM SPBT_PRD_STARS_DQI_DB.DATAINSIGHTS_COMPACT.ALL_A1c_Claims_tin c
LEFT JOIN QUMXA_PRD_STARS_DO_DB.dataops_foundation.CIA_XW_SOURCES x ON c.clm_src_sbsys_cd=x.orig_src_sbsys AND c.clm_src_sys_cd = x.orig_src_sys
WHERE cd_sys_txt ='LOINC'
AND c.strt_srvc_dt>='2026-01-01'
UNION
SELECT * FROM SPBT_PRD_STARS_DQI_DB.DATAINSIGHTS_COMPACT.ALL_A1c_Claims_tin c
LEFT JOIN QUMXA_PRD_STARS_DO_DB.dataops_foundation.CIA_XW_SOURCES x ON c.clm_src_sbsys_cd=x.orig_src_sbsys AND c.clm_src_sys_cd = x.orig_src_sys
WHERE cd_sys_txt ='CPT-CAT-II'
AND c.strt_srvc_dt>='2026-01-01'
)B
ON A.enterprise_indv_id = B.enterprise_indv_id
AND b.strt_srvc_dt BETWEEN a.strt_srvc_dt and dateadd(DAY,15,a.strt_srvc_dt)
AND year(a.strt_srvc_dt)=year(b.strt_srvc_dt)
GROUP BY ALL
)x"""
    #query = """select sum"""
    col_count, col_names = extract_result_columns(query)
    print(f"Column Count: {col_count}, Column Names: {col_names}")

Column Count: 0, Column Names: 

Process finished with exit code 0
