from easylink.pipeline_schema_constants import development, tests

ALLOWED_SCHEMA_PARAMS = {
    "development": development.SCHEMA_PARAMS,
}

TESTING_SCHEMA_PARAMS = {
    "integration": tests.SINGLE_STEP_SCHEMA_PARAMS,
    "combined_bad_topology": tests.BAD_COMBINED_TOPOLOGY_SCHEMA_PARAMS,
    "combined_bad_implementation_names": tests.BAD_COMBINED_TOPOLOGY_SCHEMA_PARAMS,
}
