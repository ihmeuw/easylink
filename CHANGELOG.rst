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
