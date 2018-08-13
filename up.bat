echo off

docker-compose -f docker-compose-LocalExecutor.yml up -d   
REM sleep 20, wait db init then set variable 
rem echo %DATE%-%TIME%
ping 127.0.0.1 -n 20 > nul
docker exec -it sqlg-with-airflow_webserver_1 airflow variables -s sql_path "/usr/local/airflow/sql"
rem echo %DATE%-%TIME%
echo on 