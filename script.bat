@echo off
set kvsWorkers=1
set flameWorkers=1

javac -cp "lib/*;." --source-path src -d bin src/cis5550/tools/*.java
javac -cp "lib/*;." --source-path src -d bin src/cis5550/jobs/*.java
javac -cp "lib/*;." --source-path src -d bin src/cis5550/test/*.java

REM Package the Crawler class into a JAR file
jar cvf crawler.jar -C bin cis5550/jobs/Crawler.class

REM Launch kvs coordinator
(
    echo cd %cd%
    echo java -cp bin;lib\webserver.jar;lib\kvs.jar;lib\flame.jar cis5550.kvs.Coordinator 8000
) > kvscoordinator.bat

REM Verify kvs coordinator batch file content
type kvscoordinator.bat

REM Launch kvs coordinator
start cmd.exe /k kvscoordinator.bat

REM Enable delayed expansion
setlocal enabledelayedexpansion

REM Launch kvs workers
for /l %%i in (1,1,%kvsWorkers%) do (
    set dir=worker%%i
    if not exist !dir! mkdir !dir!
    (
        echo cd %cd%\!dir!
        echo java -cp ..\bin;..\lib\webserver.jar;..\lib\kvs.jar;..\lib\flame.jar cis5550.kvs.Worker 800%%i !dir! localhost:8000
    ) > kvsworker%%i.bat
    start cmd.exe /k kvsworker%%i.bat
)

REM Launch flame coordinator
(
    echo cd %cd%
    echo java -cp bin;lib\webserver.jar;lib\kvs.jar;lib\flame.jar cis5550.flame.Coordinator 9000 localhost:8000
) > flamecoordinator.bat
start cmd.exe /k flamecoordinator.bat
timeout /t 2

REM Launch flame workers
for /l %%i in (1,1,%flameWorkers%) do (
    (
        echo cd %cd%
        echo java -cp "bin;lib\webserver.jar;lib\kvs.jar;lib\flame.jar" cis5550.flame.Worker 900%%i localhost:9000
    ) > flameworker%%i.bat
    start cmd.exe /k flameworker%%i.bat
)


REM Submit the job using FlameSubmit simple crawl test
(
    echo cd %cd%
    echo java -cp bin;lib\webserver.jar;lib\kvs.jar;lib\flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler "https://www.nytimes.com/"
) > flamesubmit.bat
start cmd.exe /k flamesubmit.bat

@REM REM debug test case
@REM (
@REM     echo cd %cd%
@REM     echo java -cp bin;lib\webserver.jar;lib\kvs.jar;lib\flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler http://advanced.crawltest.cis5550.net:80/nocrawl/c8Rhi6R/Crl64tEu3mJ.html 
@REM ) > flameDebug.bat
@REM start cmd.exe /k flameDebug.bat