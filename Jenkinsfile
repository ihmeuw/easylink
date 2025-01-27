@Library("vivarium_build_utils") _
reusable_pipeline(scheduled_branches: ["main"], 
                  test_types: ["unit", "integration", "e2e"], 
                  upstream_repos: ["layered_config_tree"], 
                  python_versions: ["3.11","3.12"], 
                  requires_slurm: true, 
                  use_shared_fs: true)
