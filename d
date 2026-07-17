UPDATE DSE_HEALTH_DOMAIN
SET ASSOCIATED_JOBS = PARSE_JSON(
'[
    {
        "jobid": 124,
        "dsids": []
    },
    {
        "jobid": 222,
        "dsids": []
    },
    {
        "jobid": 359,
        "dsids": [
            35901,
            35902,
            35903,
            35904,
            35905,
            35906,
            35907,
            35908,
            35909,
            35910,
            35911,
            35912
        ]
    }
]'
)
WHERE HEALTH_AREA_ID = '2'
  AND DOMAIN_ID = '201';
