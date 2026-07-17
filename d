INSERT INTO DSE_HEALTH_DOMAIN
(
    HEALTH_AREA_ID,
    HEALTH_AREA_NAME,
    DOMAIN_ID,
    DOMAIN_NAME
)
VALUES
    -- SDM Health Check
    ('1', 'SDM', '101', 'Claims'),
    ('1', 'SDM', '102', 'Members'),
    ('1', 'SDM', '103', 'Providers'),
    ('1', 'SDM', '104', 'Provider Associations'),
    ('1', 'SDM', '105', 'Gap Results'),
    ('1', 'SDM', '106', 'Incentives'),

    -- MDE Health Check
    ('2', 'MDE', '201', 'Members'),
    ('2', 'MDE', '202', 'Provider Associations'),
    ('2', 'MDE', '203', 'Incentives'),

    -- BI Reporting Health Check
    ('3', 'BI Reporting', '301', 'IMDE'),
    ('3', 'BI Reporting', '302', 'Incentive Reporting'),
    ('3', 'BI Reporting', '303', 'Part D Dashboard');
