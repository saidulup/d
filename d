UPDATE DSE_HEALTH_DOMAIN
SET ASSOCIATED_JOBS = PARSE_JSON(
'{
    "domain_weight": 10,
    "usecases": [
        {
            "usecase_id": "UC201",
            "usecase_name": "Members",
            "usecase_weight": 10,
            "jobs": [
                {
                    "jobid": 40102,
                    "tests": [
                        {
                            "dsid": 40102034,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102036,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102037,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102041,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102044,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102024,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102059,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102062,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102065,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102066,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102067,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40102070,
                            "weight": 5,
                            "critical": false
                        }
                    ]
                },
                {
                    "jobid": 40101,
                    "tests": [
                        {
                            "dsid": 40101065,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40101070,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40101069,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40101068,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40101067,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40101066,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40101075,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40101074,
                            "weight": 5,
                            "critical": false
                        },
                        {
                            "dsid": 40101073,
                            "weight": 5,
                            "critical": false
                        }
                    ]
                }
            ]
        }
    ]
}'
)
WHERE HEALTH_AREA_ID = '2'
  AND DOMAIN_ID = '201';
