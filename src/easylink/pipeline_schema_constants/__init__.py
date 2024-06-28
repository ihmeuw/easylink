from easylink.pipeline_schema_constants import development, integration_test

ALLOWED_SCHEMA_PARAMS = {
    "development": development.SCHEMA_PARAMS,
}

TESTING_SCHEMA_PARAMS = {
    "integration": integration_test.SCHEMA_PARAMS,
}
