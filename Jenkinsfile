pipeline {
    agent {
        docker {
            image 'python:3.7-stretch'
        }
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
                                -Dsonar.sources=bitflow/ -Dsonar.tests=tests.py \
                                -Dsonar.inclusions="**/*.py" \
                                -Dsonar.exclusions="**/Bitflow*.py" \
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
        stage('Notify Slack') {
            steps {
                sh 'true'
            }
            post {
                success {
                    withSonarQubeEnv('CIT SonarQube') {
                        slackSend color: 'good', message: "Build ${env.JOB_NAME} ${env.BUILD_NUMBER} was successful (<${env.BUILD_URL}|Open Jenkins>) (<${env.SONAR_HOST_URL}|Open SonarQube>)"
                    }
               }
               failure {
                    slackSend color: 'danger', message: "Build ${env.JOB_NAME} ${env.BUILD_NUMBER} failed (<${env.BUILD_URL}|Open Jenkins>)"
               }
            }
        }
    }
}

