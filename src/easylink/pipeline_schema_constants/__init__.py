"""
=========================
Pipeline Schema Constants
=========================

An EasyLink :class:`~easylink.pipeline_schema.PipelineSchema` is a collection of
:class:`Steps<easylink.step.Step>` that define a fully supported pipeline. This 
package defines the nodes and edges required to instantiate such ``PipelineSchemas``.

"""

from easylink.pipeline_schema_constants import development, main, testing

SCHEMA_PARAMS = {
    "main": main.SCHEMA_PARAMS,
    # development and testing
    "development": development.SCHEMA_PARAMS,
    "integration": testing.SCHEMA_PARAMS_ONE_STEP,
    "output_dir": testing.SCHEMA_PARAMS_OUTPUT_DIR,
    "combine_bad_topology": testing.SCHEMA_PARAMS_BAD_COMBINED_TOPOLOGY,
    "combine_bad_implementation_names": testing.SCHEMA_PARAMS_BAD_COMBINED_TOPOLOGY,
    "nested_templated_steps": testing.SCHEMA_PARAMS_NESTED_TEMPLATED_STEPS,
    "combine_with_iteration": testing.SCHEMA_PARAMS_COMBINE_WITH_ITERATION,
    "combine_with_iteration_cycle": testing.SCHEMA_PARAMS_COMBINE_WITH_ITERATION,
    "combine_with_extra_node": testing.SCHEMA_PARAMS_THREE_STEPS,
    "looping_auto_parallel_step": testing.SCHEMA_PARAMS_LOOPING_AUTO_PARALLEL_STEP,
    "auto_parallel_cloneable_step": testing.SCHEMA_PARAMS_AUTO_PARALLEL_CLONEABLE_STEP,
    "auto_parallel_loop_step": testing.SCHEMA_PARAMS_AUTO_PARALLEL_LOOP_STEP,
    "auto_parallel_hierarchical_step": testing.SCHEMA_PARAMS_AUTO_PARALLEL_HIERARCHICAL_STEP,
}
