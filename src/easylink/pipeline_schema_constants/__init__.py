from easylink.pipeline_schema_constants import development, testing

ALLOWED_SCHEMA_PARAMS = {
    "development": development.SCHEMA_PARAMS,
}

TESTING_SCHEMA_PARAMS = {
    "integration": testing.SINGLE_STEP_SCHEMA_PARAMS,
    "combined_bad_topology": testing.BAD_COMBINED_TOPOLOGY_SCHEMA_PARAMS,
    "combined_bad_implementation_names": testing.BAD_COMBINED_TOPOLOGY_SCHEMA_PARAMS,
    "nested_templated_steps": testing.NESTED_TEMPLATED_STEPS_SCHEMA_PARAMS,
}
