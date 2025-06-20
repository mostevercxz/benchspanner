go build -o benchmark.exe main.go
REM set VUS environment variable
set VUS=1
set ZONES=10
set RECORDS=80
set STRATTRCNT=10
set INTATTRCNT=10
set ZONE_START=2
set ZONES_TOTAL=1
set RECORDS_PER_ZONE=12
set EDGES_PER_RELATION=1
@REM 正式账号
set GOOGLE_APPLICATION_CREDENTIALS_FILE=llm-ai-460009-88264e702318.json
set PROJECT_ID=llm-ai-460009
set INSTANCE_ID=mlbench
set DATABASE_ID=mtbench
@REM benchmark.exe -test=write-vertex
@REM benchmark.exe -test=write-edge -batch-num 1
@REM benchmark.exe -test=truncate
benchmark.exe -test=relation
@REM benchmark.exe -test=setup
pause