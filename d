WITH CONFIGURATION AS
(
    SELECT
        DSE_HEALTH_DOMAIN.HEALTH_AREA_ID,
        DSE_HEALTH_DOMAIN.HEALTH_AREA_NAME,
        DSE_HEALTH_DOMAIN.DOMAIN_ID,
        DSE_HEALTH_DOMAIN.DOMAIN_NAME,

        COALESCE(
            TRY_TO_NUMBER(
                TO_VARCHAR(
                    DSE_HEALTH_DOMAIN.ASSOCIATED_JOBS:domain_weight
                )
            ),
            1
        ) AS DOMAIN_WEIGHT,

        COALESCE(
            TO_VARCHAR(
                USECASE_CONFIG.VALUE:usecase_id
            ),
            ''
        ) AS USECASE_ID,

        COALESCE(
            TO_VARCHAR(
                USECASE_CONFIG.VALUE:usecase_name
            ),
            ''
        ) AS USECASE_NAME,

        COALESCE(
            TRY_TO_NUMBER(
                TO_VARCHAR(
                    USECASE_CONFIG.VALUE:usecase_weight
                )
            ),
            1
        ) AS USECASE_WEIGHT,

        TRY_TO_NUMBER(
            TO_VARCHAR(
                JOB_CONFIG.VALUE:jobid
            )
        ) AS JOBID,

        COALESCE(
            TO_VARCHAR(
                TEST_CONFIG.VALUE:dsid
            ),
            ''
        ) AS DSID,

        COALESCE(
            TRY_TO_NUMBER(
                TO_VARCHAR(
                    TEST_CONFIG.VALUE:weight
                )
            ),
            1
        ) AS DSID_WEIGHT,

        COALESCE(
            TRY_TO_BOOLEAN(
                TO_VARCHAR(
                    TEST_CONFIG.VALUE:critical
                )
            ),
            FALSE
        ) AS CRITICAL_IND

    FROM DSE_HEALTH_DOMAIN

    CROSS JOIN LATERAL FLATTEN
    (
        INPUT => DSE_HEALTH_DOMAIN.ASSOCIATED_JOBS:usecases
    ) USECASE_CONFIG

    CROSS JOIN LATERAL FLATTEN
    (
        INPUT => USECASE_CONFIG.VALUE:jobs
    ) JOB_CONFIG

    CROSS JOIN LATERAL FLATTEN
    (
        INPUT => JOB_CONFIG.VALUE:tests
    ) TEST_CONFIG

    WHERE DSE_HEALTH_DOMAIN.ASSOCIATED_JOBS IS NOT NULL
)
