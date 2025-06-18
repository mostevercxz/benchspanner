go build -o benchmark.exe main.go
REM set VUS environment variable
set VUS=10  
set ZONES=10
set RECORDS=80
set STRATTRCNT=10
set INTATTRCNT=10
benchmark.exe -test=write-vertex
pause