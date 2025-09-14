properties([
    parameters([
        string(name: 'runTime', defaultValue: '0-06:00:00', description: 'Run job time limit'),
        string(name: 'runCPUs', defaultValue: '2', description: 'CPUs to allocate for run'),
        string(name: 'runMem', defaultValue: '8GB', description: 'Memory to allocate for run'),
        string(name: 'logDir', defaultValue: '/vast/wlp9800/logs', description: 'Log directory path'),
        string(name: 'envs', defaultValue: 'AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_DEFAULT_REGION', description: 'Comma-separated list of environment variables to pass through'),
        string(name: 'queueName', defaultValue: 'willyp', description: 'SLURM queue name'),
        string(name: 'maxJobs', defaultValue: '1950', description: 'Maximum number of concurrent jobs allowed'),
        string(name: 'pollInterval', defaultValue: '30', description: 'Polling interval in seconds'),
        string(name: 'awsRegion', defaultValue: 'us-east-1', description: 'AWS Region'),
        string(name: 'execHost', defaultValue: 'greene.hpc.nyu.edu', description: 'Execution host for SSH commands'),
        string(name: 'sshUser', defaultValue: 'wlp9800', description: 'SSH username'),
        booleanParam(name: 'isOffline', defaultValue: false, description: 'Run in offline mode')
    ])
])
pipeline {
    agent any
    stages {
        stage('Checkout Scripts') {
            steps {
                checkout([
                    $class: 'GitSCM',
                    branches: [[name: '*/main']],
                    userRemoteConfigs: [[url: 'https://github.com/thewillyP/clearml_to_slurm.git']]
                ])
            }
        }
        stage('Run Agent') {
            steps {
                sshagent(['greene-ssh-key']) {
                    withCredentials([[
                        $class: 'AmazonWebServicesCredentialsBinding',
                        credentialsId: 'aws-credentials',
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
                    ]]) {
                        script {
                            def finalQueueName = params.isOffline ? "${params.queueName}_offline" : params.queueName
                            def clearmlOfflineMode = params.isOffline ? 1 : 0
                            def finalEnvs = params.isOffline ? "${params.envs},CLEARML_OFFLINE_MODE" : params.envs
                            
                            sh """
                                ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${params.sshUser}@${params.execHost} '
                                export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}";
                                export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}";
                                export AWS_DEFAULT_REGION="${params.awsRegion}";
                                export CLEARML_OFFLINE_MODE=${clearmlOfflineMode};
                                bash -s "${params.runTime}" "${params.runCPUs}" "${params.runMem}" "${params.logDir}" "${finalEnvs}" "${finalQueueName}" "${params.maxJobs}" "${params.pollInterval}"
                                ' < clearml_to_slurm.sh
                            """
                        }
                    }
                }
            }
        }
    }
    post {
        success {
            echo 'ClearML SLURM Glue agent started successfully!'
        }
        failure {
            echo 'Failed to start ClearML SLURM Glue agent.'
        }
        always {
            echo 'Pipeline completed.'
        }
    }
}