pipeline {
    options {
        timeout(time: 1, unit: 'HOURS')
    }
    environment {
        registry = 'bitflowstream/bitflow-pipeline-python'
        registryCredential = 'dockerhub'
        dockerImage = '' // Variable must be declared here to allow passing an object between the stages.
        dockerImageARM32 = ''
    }
    agent {
        docker {
            image 'bitflowstream/python-build:debian'
            args '-v /var/run/docker.sock:/var/run/docker.sock'
        }
    }
    stages {
        stage('Git') {
            steps {
                script {
                    env.GIT_COMMITTER_EMAIL = sh(script: "git --no-pager show -s --format='%ae'", returnStdout: true).trim()
                }
            }
        }
        stage('Test') {
            steps {
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
        stage('SonarQube') {
            steps {
                script {
                    def scannerHome = tool 'sonar-scanner-linux'
                    withSonarQubeEnv('CIT SonarQube') {
                        sh """
                            ${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=python-bitflow -Dsonar.branch.name=$BRANCH_NAME \
                                -Dsonar.sources=bitflow -Dsonar.tests=tests/. \
                                -Dsonar.inclusions="**/*.py" \
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
                    dockerImage = docker.build registry + ':$BRANCH_NAME-build-$BUILD_NUMBER', '-f build/alpine.Dockerfile .'
                    sh "./build/test-image.sh $BRANCH_NAME-build-$BUILD_NUMBER"

                    dockerImageARM32 = docker.build registry + ':$BRANCH_NAME-build-$BUILD_NUMBER-arm32v7', '-f build/arm32v7.Dockerfile .'
                    sh "./build/test-image.sh $BRANCH_NAME-build-$BUILD_NUMBER-arm32v7"
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
                        dockerImage.push("latest-amd64")
                        dockerImageARM32.push("build-$BUILD_NUMBER-arm32v7")
                        dockerImageARM32.push("latest-arm32v7")
                    }
                }
                withCredentials([[
                    $class: 'UsernamePasswordMultiBinding',
                    credentialsId: 'dockerhub', usernameVariable: 'DOCKERUSER', passwordVariable: 'DOCKERPASS'
                ]]) {
                    // Dockerhub Login
                    sh 'echo "$DOCKERPASS" | docker login -u "$DOCKERUSER" --password-stdin'
                    sh "docker manifest create ${registry}:latest ${registry}:latest-amd64 ${registry}:latest-arm32v7"
                    sh "docker manifest annotate ${registry}:latest ${registry}:latest-arm32v7 --os linux --arch arm"
                    sh "docker manifest push --purge ${registry}:latest"
                }
            }
        }
    }
    post {
        success {
            node('master') {
                withSonarQubeEnv('CIT SonarQube') {
                    slackSend channel: '#jenkins-builds-all', color: 'good',
                        message: "Build ${env.JOB_NAME} ${env.BUILD_NUMBER} was successful (<${env.BUILD_URL}|Open Jenkins>) (<${env.SONAR_HOST_URL}|Open SonarQube>)"
                }
            }
        }
        failure {
            node('master') {
                slackSend channel: '#jenkins-builds-all', color: 'danger',
                    message: "Build ${env.JOB_NAME} ${env.BUILD_NUMBER} failed (<${env.BUILD_URL}|Open Jenkins>)"
            }
        }
        fixed {
            node('master') {
                withSonarQubeEnv('CIT SonarQube') {
                    slackSend channel: '#jenkins-builds', color: 'good',
                        message: "Thanks to ${env.GIT_COMMITTER_EMAIL}, build ${env.JOB_NAME} ${env.BUILD_NUMBER} was successful (<${env.BUILD_URL}|Open Jenkins>) (<${env.SONAR_HOST_URL}|Open SonarQube>)"
                }
            }
        }
        regression {
            node('master') {
                slackSend channel: '#jenkins-builds', color: 'danger',
                    message: "What have you done ${env.GIT_COMMITTER_EMAIL}? Build ${env.JOB_NAME} ${env.BUILD_NUMBER} failed (<${env.BUILD_URL}|Open Jenkins>)"
            }
        }
    }
}
