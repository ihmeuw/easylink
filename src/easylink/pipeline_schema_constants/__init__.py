"""
=========================
Pipeline Schema Constants
=========================

An EasyLink :class:`~easylink.pipeline_schema.PipelineSchema` is a collection of
:class:`Steps<easylink.step.Step>` that define a fully supported pipeline. This 
package defines the nodes and edges required to instantiate such ``PipelineSchemas``.

"""

from easylink.pipeline_schema_constants import development, testing

ALLOWED_SCHEMA_PARAMS = {
    "development": development.SCHEMA_PARAMS,
}

TESTING_SCHEMA_PARAMS = {
    "integration": testing.SINGLE_STEP_SCHEMA_PARAMS,
    "combine_bad_topology": testing.BAD_COMBINED_TOPOLOGY_SCHEMA_PARAMS,
    "combine_bad_implementation_names": testing.BAD_COMBINED_TOPOLOGY_SCHEMA_PARAMS,
    "nested_templated_steps": testing.NESTED_TEMPLATED_STEPS_SCHEMA_PARAMS,
    "combine_with_iteration": testing.COMBINE_WITH_ITERATION_SCHEMA_PARAMS,
    "combine_with_iteration_cycle": testing.COMBINE_WITH_ITERATION_SCHEMA_PARAMS,
    "combine_with_extra_node": testing.TRIPLE_STEP_SCHEMA_PARAMS,
}
