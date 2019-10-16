    dir("core") {
    pipeline {
        options {
            timeout(time: 1, unit: 'HOURS')
        }
        agent {
            docker {
                image 'teambitflow/python-docker:3.7-stretch'
                args '-v /var/run/docker.sock:/var/run/docker.sock'
            }
        }
        environment {
            registry = 'teambitflow/python-bitflow'
            registryCredential = 'dockerhub'
            dockerImage = '' // Variable must be declared here to allow passing an object between the stages.
        }
        stages {
            stage('Test') {
                steps {
                    sh 'pip install pytest pytest-cov'
                    sh 'make init'
                    sh 'make jenkins-test'
                }
                post {
                    always {
                        junit 'tests/test-report.xml'
                        archiveArtifacts 'tests/*-report.xml'
                    }
                }
            }
            stage('Git') {
                steps {
                    script {
                        env.GIT_COMMITTER_EMAIL = sh(
                            script: "git --no-pager show -s --format='%ae'",
                            returnStdout: true
                            ).trim()
                    }
                }
            }
            stage('SonarQube') {
                steps {
                    script {
                        // sonar-scanner which don't rely on JVM
                        def scannerHome = tool 'sonar-scanner-linux'
                        withSonarQubeEnv('CIT SonarQube') {
                            sh """
                                ${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=python-bitflow -Dsonar.branch.name=$BRANCH_NAME \
                                    -Dsonar.sources=bitflow -Dsonar.tests=tests/. \
                                    -Dsonar.inclusions="**/*.py" -Dsonar.exclusions="bitflow/Bitflow*.py" \
                                    -Dsonar.python.coverage.reportPaths=tests/coverage-report.xml \
                                    -Dsonar.test.reportPath=tests/test-report.xml
                            """
                        }
                    }
                    timeout(time: 30, unit: 'MINUTES') {
                        waitForQualityGate abortPipeline: true
                    }
                }
            }
            stage('Docker build') {
                steps {
                    script {
                        dockerImage = docker.build registry + ':$BRANCH_NAME-build-$BUILD_NUMBER'
                    }
                }
            }
            stage('Docker push') {
                when {
                    branch 'master'
                }
                steps {
                    script {
                        docker.withRegistry('', registryCredential) {
                            dockerImage.push("build-$BUILD_NUMBER")
                            dockerImage.push("latest")
                        }
                    }
                }
            }
        }
        post {
            success {
                withSonarQubeEnv('CIT SonarQube') {
                    slackSend channel: '#jenkins-builds-all', color: 'good',
                        message: "Build ${env.JOB_NAME} ${env.BUILD_NUMBER} was successful (<${env.BUILD_URL}|Open Jenkins>) (<${env.SONAR_HOST_URL}|Open SonarQube>)"
                }
            }
            failure {
                slackSend channel: '#jenkins-builds-all', color: 'danger',
                    message: "Build ${env.JOB_NAME} ${env.BUILD_NUMBER} failed (<${env.BUILD_URL}|Open Jenkins>)"
            }
            fixed {
                withSonarQubeEnv('CIT SonarQube') {
                    slackSend channel: '#jenkins-builds', color: 'good',
                        message: "Thanks to ${env.GIT_COMMITTER_EMAIL}, build ${env.JOB_NAME} ${env.BUILD_NUMBER} was successful (<${env.BUILD_URL}|Open Jenkins>) (<${env.SONAR_HOST_URL}|Open SonarQube>)"
                }
            }
            regression {
                slackSend channel: '#jenkins-builds', color: 'danger',
                    message: "What have you done ${env.GIT_COMMITTER_EMAIL}? Build ${env.JOB_NAME} ${env.BUILD_NUMBER} failed (<${env.BUILD_URL}|Open Jenkins>)"
            }
        }
    }
}
