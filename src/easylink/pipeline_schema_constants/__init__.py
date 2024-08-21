from easylink.pipeline_schema_constants import development, integration_test, pvs_census_demo

ALLOWED_SCHEMA_PARAMS = {
    "development": development.SCHEMA_PARAMS,
    "prl_demo": pvs_census_demo.SCHEMA_PARAMS,
}

TESTING_SCHEMA_PARAMS = {
    "integration": integration_test.SCHEMA_PARAMS,
}
