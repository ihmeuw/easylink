def githubUsernameToSlackName(github_author) {
  // Add team members as necessary
  def mapping = [
    "Jim Albright": "albrja",
    "Steve Bachmeier": "sbachmei",
    "Hussain Jafari": "hjafari",
    "Patrick Nast": "pnast",
    "Rajan Mudambi": "rmudambi",
  ]
  return mapping.get(github_author, "channel")
}

pipeline_name="easylink"
conda_env_name="${pipeline_name}-${BRANCH_NAME}-${BUILD_NUMBER}"
// using /tmp for things is MUCH faster but not shared between nodes.
shared_filesystem_path="/mnt/team/simulation_science/priv/engineering/tests"
conda_env_path="${shared_filesystem_path}/venv/${conda_env_name}"
// defaults for conda and pip are a local directory /svc-simsci for improved speed.
// In the past, we used /ihme/code/* on the NFS (which is slower)
shared_jenkins_node_path="/svc-simsci"
// comma separated string list of branches to run periodic builds on
scheduled_branches = "main"
CRON_SETTINGS = scheduled_branches.split(',').collect{it.trim()}.contains(BRANCH_NAME) ? 'H H(20-23) * * *' : ''

pipeline {
  // This agent runs as svc-simsci on node simsci-ci-coordinator-01.
  // It has access to standard IHME filesystems and singularity
  agent { label "coordinator" }

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
    string(
      name: "SLACK_TO",
      defaultValue: "simsci-ci-status",
      description: "The Slack channel to send messages to."
    )
  }
  triggers {
    cron(CRON_SETTINGS)
  }
  stages {
    stage("Initialization") {
      steps {
        script {
          // Use the name of the branch in the build name
          currentBuild.displayName = "#${BUILD_NUMBER} ${GIT_BRANCH}"
          // Tell BitBucket that a build has started.
          notifyBitbucket()
        }
      }
    }

    stage("Python version matrix") {
      // we don't want to go to deployment if any of the matrix branches fail
      failFast true
      matrix {
        // customWorkspace setting must be ran within a node
        agent {
          node {
            // Run child tasks on slurm jenkins node.
              label "slurm"
          }
        }
        axes {
          axis {
              // parallelize by python minor version
              name 'PYTHON_VERSION'
              values "3.11", "3.12"
          }
        }

        environment {
          // Get the branch being built and strip everything but the text after the last "/"
          BRANCH = sh(script: "echo ${GIT_BRANCH} | rev | cut -d '/' -f1 | rev", returnStdout: true).trim()
          TIMESTAMP = sh(script: 'date', returnStdout: true)
          // Specify the path to the .condarc file via environment variable.
          // This file configures the shared conda package cache.
          CONDARC = "${shared_jenkins_node_path}/miniconda3/.condarc"
          CONDA_BIN_PATH = "${shared_jenkins_node_path}/miniconda3/bin"
          // Specify conda env by build number so that we don't have collisions if builds from
          // different branches happen concurrently.
          CONDA_ENV_NAME = "${conda_env_name}"
          CONDA_ENV_PATH = "${conda_env_path}_${PYTHON_VERSION}"
          // Set the Pip cache.
          XDG_CACHE_HOME = "${shared_jenkins_node_path}/pip-cache"
          // Jenkins commands run in separate processes, so need to activate the environment every
          // time we run pip, poetry, etc.
          ACTIVATE = "source ${CONDA_BIN_PATH}/activate ${CONDA_ENV_PATH} &> /dev/null"
        }

        stages {
          stage("Debug Info") {
            steps {
              echo "Jenkins pipeline run timestamp: ${TIMESTAMP}"
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
              // display env
              sh "env | sort"
            }
          }

          stage("Build Environment") {
            environment {
              // Command for activating the base environment. Activating the base environment sets
              // the correct path to the conda binary which is used to create a new conda env.
              ACTIVATE_BASE = "source ${CONDA_BIN_PATH}/activate &> /dev/null"
            }
            steps {
              // The env should have been cleaned out after the last build, but delete it again
              // here just to be safe.
              sh "rm -rf ${CONDA_ENV_PATH}"
              sh "${ACTIVATE_BASE} && make build-env"
              // open permissions for test users to create file in workspace
              sh "chmod 777 ${WORKSPACE}"
            }
          }

          stage("Install Package") {
            steps {
              // NOTE: If you're having issues with the env not being found, it's possible
              // that 'make install' is generating symlinks. Try adding
              // '&& pip install .' to install a second time w/ realpaths.
              sh "${ACTIVATE} && make install && pip install ."
            }
          }

          stage("Quality Checks") {
            stages {
              stage("Format") {
                steps {
                  sh "${ACTIVATE} && make format"
                }
              }

              stage("Lint") {
                steps {
                  sh "${ACTIVATE} && make lint"
                }
              }

              // TODO: Add a typecheck stage
            }
          }  // End of quality checks

          stage("Test and Build Docs") {
            stages {

              stage("Build Docs") {
                steps {
                  sh "${ACTIVATE} && make build-doc"
                }
              }

              stage("Run Unit Tests") {
                steps {
                  sh "${ACTIVATE} && make unit"
                  publishHTML([
                    allowMissing: true,
                    alwaysLinkToLastBuild: false,
                    keepAll: true,
                    reportDir: "output/htmlcov_unit",
                    reportFiles: "index.html",
                    reportName: "Coverage Report - Unit Tests",
                    reportTitles: ''
                  ])
                }
              }

              stage("Run Integration Tests") {
                steps {
                  sh "${ACTIVATE} && make integration"
                  publishHTML([
                    allowMissing: true,
                    alwaysLinkToLastBuild: false,
                    keepAll: true,
                    reportDir: "output/htmlcov_integration",
                    reportFiles: "index.html",
                    reportName: "Coverage Report - Integration Tests",
                    reportTitles: ''
                  ])
                }
              }
              
              stage("Run End-to-End Tests") {
                steps {
                  sh "${ACTIVATE} && make e2e"
                  publishHTML([
                    allowMissing: true,
                    alwaysLinkToLastBuild: false,
                    keepAll: true,
                    reportDir: "output/htmlcov_e2e",
                    reportFiles: "index.html",
                    reportName: "Coverage Report - E2E Tests",
                    reportTitles: ''
                  ])
                }
              }
            }  
          }  // End of test and build docs stage
        }  // End of matrix stages

        post {
          cleanup {  // cleanup for python matrix workspaces
            sh "${ACTIVATE} && make clean"
            sh "rm -rf ${CONDA_ENV_PATH}"
            cleanWs()
            // manually remove @tmp dirs
            dir("${WORKSPACE}@tmp"){
              deleteDir()
            }
          }
        }  // End of matrix post actions
      }  // End of matrix
    }  // End of matrix stage
  }  // End of outer stages

  post {
    failure {
      script {
        if (env.BRANCH == "main") {
          channelName = "simsci-ci-status"
        } else {
          channelName = "simsci-ci-status-test"
        }
        // Run git command to get the author of the last commit
        developerID = sh(
          script: "git log -1 --pretty=format:'%an'",
          returnStdout: true
        ).trim()
        slackID = githubUsernameToSlackName(developerID)
        slackMessage = """
          Job: *${env.JOB_NAME}*
          Build number: #${env.BUILD_NUMBER}
          Build status: *${currentBuild.result}*
          Author: @${slackID}
          Build details: <${env.BUILD_URL}/console|See in web console>
        """.stripIndent()
      }
      slackSend channel: "#${params.SLACK_TO}", 
                message: slackMessage,
                teamDomain: "ihme",
                tokenCredentialId: "slack"
    }
    cleanup { // cleanup for outer workspace
      cleanWs()
      // manually remove @tmp dirs
      dir("${WORKSPACE}@tmp"){
        deleteDir()
      }
    }
  }
}  // End of pipeline