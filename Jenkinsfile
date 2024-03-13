def pipeline_name="linker"
def env_name="$pipeline_name-$BUILD_NUMBER"
// def conda_env_path="/mnt/team/simulation_science/priv/engineering/venv/svc-ccomp/$env_name"
def conda_env_path="/mnt/share/homes/sbachmei/venv/$env_name"

pipeline {
  // This agent runs as svc-ccomp on node cc-slurm-sbuild-p01.
  // It has access to standard IHME filesystems, and it doesn't have access to Docker.
  agent { label "svc-ccomp" }

  options {
    // Keep 100 old builds.
    buildDiscarder logRotator(numToKeepStr: "100")

    // Wait 60 seconds before starting the build.
    // If another commit enters the build queue in this time, the first build will be discarded.
    quietPeriod(60)

    // Fail immediately if any part of a parallel stage fails
    parallelsAlwaysFailFast()
  }

  // This trigger enables Bitbucket integration.
  triggers {
    pollSCM ""
    parameterizedCron 'H H(0-6) * * * %IS_CRON=true'
  }

  parameters {
    booleanParam(
      name: "IS_CRON",
      defaultValue: true,
      description: "Inidicates a recurring build. Used to skip deployment steps."
    )
  }

  environment {
    // Get the branch being built and strip everything but the text after the last "/"
    BRANCH = sh(script: "echo ${GIT_BRANCH} | rev | cut -d '/' -f1 | rev", returnStdout: true).trim()

    // Specify the path to the .condarc file via environment variable.
    // This file configures the shared conda package cache.
    CONDARC = "/ihme/code/svc-ccomp/miniconda3/.condarc"

    // Specify conda env by build number so that we don't have collisions if builds from
    // different branches happen concurrently.
    CONDA_ENV_NAME = "$conda_env_name"
    CONDA_ENV_PATH = "$conda_env_path"

    // Path to conda binaries.
    CONDA_BIN_PATH = "/ihme/code/svc-ccomp/miniconda3/bin"

    // Jenkins commands run in separate processes, so need to activate the environment every
    // time we run pip, poetry, etc.
    ACTIVATE = "source ${CONDA_BIN_PATH}/activate ${CONDA_ENV_PATH} &> /dev/null"

    // Set the Pip cache.
    XDG_CACHE_HOME = "/ihme/code/svc-ccomp/pip-cache"

    // Timestamp this build.
    TIMESTAMP = sh(script: 'date', returnStdout: true)
  }

  stages {
    stage("Initialization") {
      steps {
        script {
          // Use the name of the branch in the build name
          currentBuild.displayName = "#${BUILD_NUMBER} ${GIT_BRANCH}"
        }
      }
    }

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
        // open permissions for cctest users to create file in workspace
        sh "chmod 777 ${WORKSPACE}"
      }
    }

    stage("Install Package") {
      steps {
        // HACK: Note we REINSTALL the current package (".") because we want a
        // "portable" environment that has all the files copied into it. By default we
        // get a reference back to the current working directory, but that will be in
        // the Jenkins VM, and we need an environment that slurm tasks can run on
        // (remotely).
        sh "${ACTIVATE} && make install && pip install ."
      }
    }

    stage("Quality Checks") {
      parallel {
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

        stage("Type Check") {
          steps {
            sh "${ACTIVATE} && make typecheck"
          }
        }
      }
    }

    stage("Test") {
      // removable, if passwords can be exported to env. securely without bash indirection
      parallel {
        stage("Run e2e tests") {
          steps {
            sh "${ACTIVATE} && make unit"  // TODO: PUT BACK TO E2E 
          }
        }

        stage("Run unit tests") {
          steps {
            sh "${ACTIVATE} && make unit"
          }
        }
      }
    }
  }

  post {
    always {
      publishHTML([
        allowMissing: true,
        alwaysLinkToLastBuild: false,
        keepAll: true,
        reportDir: 'output/htmlcov_e2e_',
        reportFiles: 'index.html',
        reportName: 'Coverage Report - e2e Tests',
        reportTitles: ''
      ])
      publishHTML([
        allowMissing: true,
        alwaysLinkToLastBuild: false,
        keepAll: true,
        reportDir: 'output/htmlcov_unit_',
        reportFiles: 'index.html',
        reportName: 'Coverage Report - Unit tests',
        reportTitles: ''
      ])
      junit([
        testResults: "**/*_test_report.xml",
        allowEmptyResults: true
      ])

      // Run any cleanup steps specified in the makefile.
      sh "${ACTIVATE} && make clean"

      // Delete the conda environment used in this build.
      // sh "rm -rf ${CONDA_ENV_PATH}"  //TODO: UNCOMMEND THIS OUT!!

      // Delete the workspace directory.
      deleteDir()
    }
    failure {
      slackSend channel: '#simsci-test-ci-status', 
                message: ":x: JOB FAILURE: $JOB_NAME - #$BUILD_ID\n\n$BUILD_URL/console\n\n<!channel>",
                teamDomain: 'ihme',
                tokenCredentialId: 'eafd508b-f614-460d-bce5-3a5a43b7aa68'
    }
  }
}