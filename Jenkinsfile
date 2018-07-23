#! /usr/bin/env groovy

pipeline {
    agent { label 'docker' }

    environment {
        COMPOSE_FILE = 'docker-compose.ci.yml'
        COMPOSE_PROJECT_NAME = "${env.JOB_NAME}-${env.BUILD_ID}"
    }

    stages {
        stage('Build Image') {
            steps {
                sh "docker-compose build --pull"
            }
        }

        stage('Install') {
            steps {
                sh "docker-compose run --rm -T test go install ./..."
            }
        }

        stage('Vet') {
            steps {
                echo 'Running `go vet`…'
                sh "docker-compose run --rm -T test go vet ./..."
            }
        }

        stage('Fmt') {
            steps {
                echo 'Running `go fmt`…'
                sh "docker-compose run --rm -T test /bin/sh -c 'gofmt -e -d . | tee /dev/stderr > bad_files.txt && test ! -s bad_files.txt'"
            }
        }

        stage('Test') {
            steps {
                sh "docker-compose run --rm -T test go test ./..."
            }
        }
    }

    post {
        cleanup {
            sh "docker-compose down --remove-orphans --rmi=all --volumes"
        }
    }
}
