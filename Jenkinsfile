pipeline {
    agent {
        docker {
            image 'teambitflow/python-docker:3.7-stretch'
            args '-v /var/run/docker.sock:/var/run/docker.sock'
        }
    }
    environment {
        registry = 'teambitflow/python-bitflow'
        registryCredential = 'dockerhub'
    }
    stages {
        stage('Test') { 
            steps {
                sh 'pip install pytest pytest-cov'
                sh 'pip install -r requirements.txt'
                sh 'py.test --junitxml test-report.xml --cov-report xml:coverage-report.xml --cov=bitflow tests.py'
            }
            post {
                always {
                    junit 'test-report.xml'
                    archiveArtifacts '*-report.xml'
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
            when {
                branch 'master'
            }
            steps {
                script {
                    // sonar-scanner which don't rely on JVM
                    def scannerHome = tool 'sonar-scanner-linux'
                    withSonarQubeEnv('CIT SonarQube') {
                        sh """
                            ${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=python-bitflow \
                                -Dsonar.sources=bitflow -Dsonar.tests=tests.py \
                                -Dsonar.inclusions="**/*.py" -Dsonar.exclusions="bitflow/Bitflow*.py" \
                                -Dsonar.python.coverage.reportPaths=coverage-report.xml \
                                -Dsonar.test.reportPath=test-report.xml
                        """
                    }
                }
                timeout(time: 30, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }
        stage('Docker') {
            when {
                branch 'master'
            }
            steps {
                script {
                    dockerImage = docker.build registry + ':build-$BUILD_NUMBER'
                    docker.withRegistry('', registryCredential ) {
                        dockerImage.push()
                        dockerImage.push('latest')
                    }
                }
                sh "docker rmi $registry:build-$BUILD_NUMBER"
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

