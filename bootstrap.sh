#! /bin/bash

print_usage () {
  echo 'to start airflow run source ./bootstrap.sh start'
  echo 'to stop airflow run source ./bootstrap.sh stop'
}

start_airflow () {
  source venv/bin/activate
  echo which venv/bin/activate
  nohup airflow scheduler > ./logs/scheduler.log & echo $!> ./pids/scheduler_pid.txt
  nohup airflow webserver > ./logs/webserver.log & echo $!> ./pids/webserver_pid.txt
}

stop_airflow() {
  kill -9 $(cat ./pids/scheduler_pid.txt)
  kill -9 $(cat ./pids/webserver_pid.txt)
  source deactivate
}

if [[ $# -eq 0 ]] ; then
    print_usage
    exit 0
fi

case "$1" in
    start) echo 'starting airflow...'
    start_airflow
    ;;
    stop) echo 'stopping airflow...'
    stop_airflow
    ;;
    *) print_usage ;;
esac


# venv/bin/activate