**0.1.21 - 6/16/25**

 - Update the "parallel" pipeline configuration key to "clones"
 - Update "ParallelStep" and "EmbarrassinglyParallelStep" class names to "CloneableStep" and "AutoParallelStep", respectively

**0.1.20 - 6/11/25**

 - Update implementation creator tool for new implementation_metadata.yaml format

**0.1.19 - 6/11/25**

 - Implement support for images hosted on zenodo

**0.1.18 - 5/14/25**

 - Refactor to use a single pipeline schema rather than multiple potential schemas

**0.1.17 - 5/14/25**

 - Support directories as step intermediates

**0.1.16 - 5/13/25**

 - Implement new cli command to simplify implementation creation

**0.1.15 - 5/5/25**

 - Fix SyntaxWarning for unescaped backslashes

**0.1.14 - 5/1/25**

 - Add support for EmbarrassinglyParallelSteps to accept sections (i.e. non-leaf steps)

**0.1.13 - 4/21/25**

 - remove graphviz from doc build install

**0.1.12 - 4/1/25**

 - Propagate embarrassingly parallel splitters and aggregators to leaf steps

**0.1.11 - 3/28/25**

 - Refactor the ImplementationGraph recursion logic for clarity

**0.1.10 - 3/25/25**

 - Make InputSlots and OutputSlots mutable

**0.1.9 - 3/14/25**

 - Refactor EmbarrassinglyParallelStep to require a Step during construction

**0.1.8 - 3/13/25**

 - Refactor subgraph logic from Step to HierarchicalStep
 - Refactor ChoiceStep so that each choice requires a single Step instead of nodes/edges
 - Standardize the passing around of configurations to be step config instead of parent config

**0.1.7 - 2/26/25**

 - Implement initial embarrassingly/auto-parallel step support

**0.1.6 - 2/21/25**

 - Move test dictionaries to yaml files

**0.1.5 - 2/20/25**

 - Fix handling of templated steps when no looping or parallelism is requested

**0.1.4 - 2/20/25**

 - Implement duplicate_template_step method on TemplatedStep class

**0.1.3 - 1/7/25**

 - Validate currently-installed python version during setup
 - Automatically update README when supported python versions change
 - Automatically extract github actions supported python version test matrix

**0.1.2 - 12/16/24**

 - Add optional arg to pass allowable schemas to the Config constructor
 - Initial documentation publication

**0.1.1 - 12/10/24**

 - Implement pipeline choice sections

**0.1.0 - 11/22/24**

 - Initial release
