pipeline {
    agent any
        environment {
            projectName = 'queueclient'
            VIRTUAL_ENV = "${env.WORKSPACE}/venv"
    }
    stages {
        stage("requirements") {
            steps {
                sh """
                    echo ${SHELL}
                    [ -d venv ] && rm -rf venv
                    virtualenv venv -p python2
                    #. venv/bin/activate
                    export PATH=${VIRTUAL_ENV}/bin:${PATH}
                    pip install --upgrade pip
                    pip install -r requirements.txt -r dev-requirements.txt
                    python setup.py install
                """
            }
        }
        stage("test") {
            steps {
                sh """
                #. venv/bin/activate
                export PATH=${VIRTUAL_ENV}/bin:${PATH}
                tox
                """
            }
            post {
                always {
                    junit keepLongStdio: true, testResults: 'report/*.xml'
                    publishHTML target: [
                        reportDir: 'report/coverage',
                        reportFiles: 'index.html',
                        reportName: 'Coverage Report - Unit Test'
                    ]
                }
            }
        }
        stage ('clean') {
            steps {
                sh 'rm -rf venv'
            }
        }
    }
}