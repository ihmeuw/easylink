@Library("vivarium_build_utils@pnast/feature/mic-5670-auto-python") _
reusable_pipeline(scheduled_branches: ["main"], 
                  test_types: ["unit", "integration", "e2e"], 
                  upstream_repos: ["layered_config_tree"], 
                  requires_slurm: true, 
                  use_shared_fs: true)
