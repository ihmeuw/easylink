@Library("vivarium_build_utils@ignore_dirs") _
reusable_pipeline(scheduled_branches: ["main"], 
                  test_types: ["unit", "integration", "e2e"], 
                  upstream_repos: ["layered_config_tree"], 
                  requires_slurm: true, 
                  use_shared_fs: true,
                  ignored_dirs: ["docs"])
