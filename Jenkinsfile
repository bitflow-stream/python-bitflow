pipeline {
    agent {
        docker {
            image 'python:3.7-stretch'
        }
    }
    stages {
        stage('Test') { 
            steps {
                sh '''
                pip install pytest
                pip install -r requirements.txt
                '''
                sh 'py.test --junitxml test.xml tests.py'
            }
            post {
                always {
                    junit "test.xml"
                }
            }
        }
        stage('SonarQube') {
            steps {
                script {
                    // sonar-scanner which don't rely on JVM
                    def scannerHome = tool 'sonar-scanner-linux'
                    withSonarQubeEnv('CIT SonarQube') {
                        sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=python-bitflow"
                    }
                }
            }
        }
        stage("Quality Gate") {
            steps {
                timeout(time: 30, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }
        stage('Notify Slack') {
            steps {
                script {
                    sh 'true'
                }
            }
            post {
                success {
                    withSonarQubeEnv('CIT SonarQube') {
                        slackSend color: 'good', message: "Build ${env.JOB_NAME} ${env.BUILD_NUMBER} was successful (<${env.BUILD_URL}|Open Jenkins>) (<${env.SONAR_HOST_URL}|Open SonarQube>)"
                    }
               }
               failure {
                    withSonarQubeEnv('CIT SonarQube') {
                        slackSend color: 'danger', message: "Build ${env.JOB_NAME} ${env.BUILD_NUMBER} failed (<${env.BUILD_URL}|Open Jenkins>)"
                    }
               }
            }
        }
    }
}

