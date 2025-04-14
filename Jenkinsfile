// @Library("vivarium_build_utils") _

def reusable_pipeline(Map config = [:]){
  /* This is the funtion called from the repo
  Example: reusable_pipeline(job_name: JOB_NAME)
  JOB_NAME is a reserved Jenkins var
  -------------
  Configuration options:
  scheduled_branches: The branch names for which to run scheduled nightly builds.
  test_types: The tests to run. Must be subset (inclusive) of ['unit', 'integration', 'e2e']
  requires_slurm: Whether the child tasks require the slurm scheduler.
  deployable: Whether the package can be deployed by Jenkins.
  skip_doc_build: Only skips the doc build.
  use_shared_fs: Whether to use the shared filesystem for conda envs.
  upstream_repos: A list of repos to check for upstream changes.
  ignored_dirs: A list of directories to ignore; if no changes outside of these, the build is skipped.
  */

  task_node = config.requires_slurm ? 'slurm' : 'matrix-tasks'

  scheduled_branches = config.scheduled_branches ?: []
  CRON_SETTINGS = scheduled_branches.contains(BRANCH_NAME) ? 'H H(20-23) * * *' : ''

  PYTHON_DEPLOY_VERSION = "3.11"

  test_types = config.test_types ?: ['all-tests']
  // raise an error if test_types is not a subset of  ['e2e', 'unit', 'integration']
  if (!test_types.every { ['all-tests', 'e2e', 'unit', 'integration'].contains(it) }) {
    throw new IllegalArgumentException("test_types must be a subset of ['all-tests', 'e2e', 'unit', 'integration']")
  }
  // Allow for building conda env on shared fs if required
  conda_env_name = config.use_shared_fs ? "${env.JOB_NAME.replaceAll('/', '-')}-${BUILD_NUMBER}" : "${env.JOB_NAME}-${BUILD_NUMBER}"
  conda_env_dir = config.use_shared_fs ? "/mnt/team/simulation_science/priv/engineering/tests/venv" : "/tmp"

  // Define the upstream repos to check for changes
  upstream_repos = config.upstream_repos ?: []
  ignored_dirs = config.ignored_dirs ?: []

  pipeline {
    // This agent runs as svc-simsci on node simsci-ci-coordinator-01.
    // It has access to standard IHME filesystems and singularity
    agent { label "coordinator" }

    environment {
        IS_CRON = "${currentBuild.buildCauses.toString().contains('TimerTrigger')}"
        // defaults for conda and pip are a local directory /svc-simsci for improved speed.
        // In the past, we used /ihme/code/* on the NFS (which is slower)
        shared_path="/svc-simsci"
        // Get the branch being built and strip everything but the text after the last "/"
        BRANCH = sh(script: "echo ${GIT_BRANCH} | rev | cut -d '/' -f1 | rev", returnStdout: true).trim()
        TIMESTAMP = sh(script: 'date', returnStdout: true)
        // Specify the path to the .condarc file via environment variable.
        // This file configures the shared conda package cache.
        CONDARC = "${shared_path}/miniconda3/.condarc"
        CONDA_BIN_PATH = "${shared_path}/miniconda3/bin"
        // Set the Pip cache.
        XDG_CACHE_HOME = "${shared_path}/pip-cache"
        // Jenkins commands run in separate processes, so need to activate the environment every
        // time we run pip, poetry, etc.
        ACTIVATE_BASE = "source ${CONDA_BIN_PATH}/activate &> /dev/null"
    }

    options {
      // Keep 100 old builds.
      buildDiscarder logRotator(numToKeepStr: "100")
      
      // Wait 60 seconds before starting the build.
      // If another commit enters the build queue in this time, the first build will be discarded.
      quietPeriod(60)

      // Fail immediately if any part of a parallel stage fails
      parallelsAlwaysFailFast()
    }

    parameters {
      booleanParam(
        name: "SKIP_DEPLOY",
        defaultValue: false,
        description: "Whether to skip deploying on a run of the default branch."
      )
      booleanParam(
        name: "RUN_SLOW",
        defaultValue: false,
        description: "Whether to run slow tests as part of pytest suite."
      )
      string(
        name: "SLACK_TO",
        defaultValue: "",
        description: "The Slack channel to send messages to."
      )
      booleanParam(
        name: "DEBUG",
        defaultValue: false,
        description: "Used as needed for debugging purposes."
      )
    }

    triggers {
      cron(CRON_SETTINGS)
    }

    stages {
      stage("Initialization") {
        steps {
          script {
            if (ignored_dirs && !env.IS_CRON.toBoolean() && !currentBuild.rawBuild.getCauses().any { it.toString().contains('UserIdCause') }) {
              def diffOutput
              if (env.CHANGE_TARGET) {
                sh "git fetch origin ${env.CHANGE_TARGET}"
                diffOutput = sh(
                  script: "git diff --name-only origin/${env.CHANGE_TARGET}...HEAD",
                  returnStdout: true
                ).trim()
              } else {
                diffOutput = sh(
                  script: "git diff --name-only HEAD~1",
                  returnStdout: true
                ).trim()
              }
              def hasRelevantChange = diffOutput.split("\n").any { file ->
                !ignored_dirs.any { file.startsWith(it) }
              }
              if (!hasRelevantChange) {
                echo "No relevant changes outside ignored_dirs: ${ignored_dirs}. Skipping build."
                currentBuild.result = 'NOT_BUILT'
                return
              }
            }

            // Use the name of the branch in the build name
            currentBuild.displayName = "#${BUILD_NUMBER} ${GIT_BRANCH}"
            python_versions = get_python_versions(WORKSPACE, GIT_URL)
          }
        }
      }

      stage("Python Versions") {
        steps {
          script {
            def parallelPythonVersions = [:]
            
            python_versions.each { pythonVersion ->
              parallelPythonVersions["Python ${pythonVersion}"] = {
                node(task_node) {
                  def envVars = [
                    CONDA_ENV_NAME: "${conda_env_name}-${pythonVersion}",
                    CONDA_ENV_PATH: "${conda_env_dir}/${conda_env_name}-${pythonVersion}",
                    PYTHON_VERSION: pythonVersion,
                    ACTIVATE: "source /svc-simsci/miniconda3/bin/activate ${conda_env_dir}/${conda_env_name}-${pythonVersion} &> /dev/null",
                  ]
                  
                  withEnv(envVars.collect { k, v -> "${k}=${v}" }) {
                    try {
                      checkout scm
                      load_shared_files()
                      stage("Debug Info - Python ${pythonVersion}") {
                        echo "Jenkins pipeline run timestamp: ${env.TIMESTAMP}"
                        // Display parameters used.
                        echo """Parameters:
                        SKIP_DEPLOY: ${params.SKIP_DEPLOY}
                        RUN_SLOW: ${params.RUN_SLOW}
                        SLACK_TO: ${params.SLACK_TO}
                        DEBUG: ${params.DEBUG}"""

                        // Display environment variables from Jenkins.
                        echo """Environment:
                        ACTIVATE:       '${ACTIVATE}'
                        BUILD_NUMBER:   '${BUILD_NUMBER}'
                        BRANCH:         '${BRANCH}'
                        CONDARC:        '${CONDARC}'
                        CONDA_BIN_PATH: '${CONDA_BIN_PATH}'
                        CONDA_ENV_NAME: '${CONDA_ENV_NAME}'
                        CONDA_ENV_PATH: '${CONDA_ENV_PATH}'
                        GIT_BRANCH:     '${GIT_BRANCH}'
                        JOB_NAME:       '${JOB_NAME}'
                        WORKSPACE:      '${WORKSPACE}'
                        XDG_CACHE_HOME: '${XDG_CACHE_HOME}'"""
                      }

                      stage("Build Environment - Python ${pythonVersion}") {
                        // The env should have been cleaned out after the last build, but delete it again
                        // here just to be safe.
                        sh "rm -rf ${CONDA_ENV_PATH}"
                        sh "${ACTIVATE_BASE} && make create-env PYTHON_VERSION=${PYTHON_VERSION}"
                        // open permissions for test users to create file in workspace
                        sh "chmod 777 ${WORKSPACE}"
                      }

                      stage("Install Package - Python ${pythonVersion}") {
                        sh "${ACTIVATE} && make install && pip install ."
                      }
                      stage("Install Upstream Dependency Branches - Python ${pythonVersion}") {
                        sh "chmod +x install_dependency_branch.sh"
                        upstream_repos.each { repo ->
                          sh "${ACTIVATE} && ./install_dependency_branch.sh ${repo} ${GIT_BRANCH} jenkins"
                        }
                      }

                      stage("Check Formatting - Python ${pythonVersion}") {
                        sh "${ACTIVATE} && make lint"
                      }

                      stage("Run Tests - Python ${pythonVersion}") {
                        script {
                            def full_name = { test_type ->
                              if (test_type == 'e2e') {
                                  return "End-to-End"
                              } else if (test_type == 'all-tests') {
                                return "All"
                              } else {
                                  return test_type.capitalize()
                              }
                            }
                            def parallelTests = test_types.collectEntries {
                                ["${full_name(it)} Tests" : {
                                    stage("Run ${full_name(it)} Tests - Python ${pythonVersion}") {
                                        sh "${ACTIVATE} && make ${it}${(env.IS_CRON.toBoolean() || params.RUN_SLOW) ? ' RUNSLOW=1' : ''}"
                                        publishHTML([
                                          allowMissing: true,
                                          alwaysLinkToLastBuild: false,
                                          keepAll: true,
                                          reportDir: "output/htmlcov_${it}",
                                          reportFiles: "index.html",
                                          reportName: "Coverage Report - ${full_name(it)} tests",
                                          reportTitles: ''
                                        ])
                                    }
                                }]
                            }
                            parallel parallelTests
                        }
                      }

                      if (PYTHON_VERSION == PYTHON_DEPLOY_VERSION) {
                        if (config?.skip_doc_build != true) {
                            stage("Build Docs - Python ${pythonVersion}") {
                              sh "${ACTIVATE} && make build-doc"
                            }
                          }

                        stage("Build and Deploy - Python ${pythonVersion}") {
                          if ((config?.deployable == true) && 
                            !env.IS_CRON.toBoolean() &&
                            !params.SKIP_DEPLOY &&
                            (env.BRANCH == "main")) {
                            
                            stage("Tagging Version and Pushing") {
                                  sh "${ACTIVATE} && make tag-version"
                                }

                            stage("Build Package - Python ${pythonVersion}") {
                              sh "${ACTIVATE} && make build-package"
                            }

                            stage("Deploy Package to Artifactory") {
                              withCredentials([usernamePassword(
                                credentialsId: 'artifactory_simsci',
                                usernameVariable: 'PYPI_ARTIFACTORY_CREDENTIALS_USR',
                                passwordVariable: 'PYPI_ARTIFACTORY_CREDENTIALS_PSW'
                              )]) {
                                sh "${ACTIVATE} && make deploy-package-artifactory"
                              }
                            }
                            if (config?.skip_doc_build != true) {
                              stage("Deploy Docs") {
                                withEnv(["DOCS_ROOT_PATH=/mnt/team/simulation_science/pub/docs"]) {
                                  sh "${ACTIVATE} && make deploy-doc"
                                }
                              }
                            }
                          }
                        }
                      }
                    } finally {
                  // Cleanup
                      sh "${ACTIVATE} && make clean"
                      sh "rm -rf ${conda_env_path}"
                      cleanWs()
                      dir("${WORKSPACE}@tmp") {
                        deleteDir()
                      }
                    }
                  }
                }
              }
            }

            parallel parallelPythonVersions
          }
        }
      }
    }

    post {
      always {
        // Generate a message to send to Slack.
        script {
          // Run git command to get the author of the last commit
          developerID = sh(
            script: "git log -1 --pretty=format:'%an'",
            returnStdout: true
          ).trim()
          if (params.SLACK_TO) {
            channelName = params.SLACK_TO
            slackID = "channel"
          } else if (env.BRANCH == "main") {
            channelName = "simsci-ci-status"
            slackID = "channel"
          } else {
            channelName = "simsci-ci-status-test"
            slackID = github_slack_mapper(github_author: developerID)
          }
          echo "slackID to tag in slack message: ${slackID}"
          slackMessage = """
            Job: *${env.JOB_NAME}*
            Build number: #${env.BUILD_NUMBER}
            Build status: *${currentBuild.result}*
            Author: @${slackID}
            Build details: <${env.BUILD_URL}/console|See in web console>
            """.stripIndent()
        }
      }
      failure {
        echo "This build triggered by ${developerID} failed on ${GIT_BRANCH}. Sending a failure message to Slack."
        slackSend channel: "#${channelName}",
                  message: slackMessage,
                  teamDomain: "ihme",
                  tokenCredentialId: "slack"
      }
      success {
        script {
          if (params.DEBUG) {
            echo 'Debug is enabled. Sending a success message to Slack.'
            slackSend channel: "#${channelName}",
                      message: slackMessage,
                      teamDomain: "ihme",
                      tokenCredentialId: "slack"
          } else {
            echo 'Debug is not enabled. No success message will be sent to Slack.'
          }
        }
      }
      cleanup { // cleanup for outer workspace
        cleanWs()
        // manually remove @tmp dirs
        dir("${WORKSPACE}@tmp"){
          deleteDir()
        }
      }
    }  // End of post
  }  // End of pipeline
}


reusable_pipeline(scheduled_branches: ["main"], 
                  test_types: ["unit", "integration", "e2e"], 
                  upstream_repos: ["layered_config_tree"], 
                  requires_slurm: true, 
                  use_shared_fs: true,
                  ignored_dirs: ["docs"])
