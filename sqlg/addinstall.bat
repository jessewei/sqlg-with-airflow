@ECHO OFF

set pathToInsert=
IF '%1' == '' GOTO SETPATH
SET pathToInsert=%1
GOTO PROCESS


:SETPATH
set /p pathToInsert=Dir to install Addin[Default %APPDATA%\Microsoft\AddIns\] 
rem echo %pathToInsert%
if "%pathToInsert%" == "" set pathToInsert=%APPDATA%\Microsoft\AddIns\



:PROCESS
rem echo %pathToInsert%
rem set pathToInsert="%pathToInsert%"
@powershell  -executionpolicy bypass -file .\Inst-addin.ps1 -DEST_DIR %pathToInsert%
rem debug @powershell  -executionpolicy bypass -file .\cp-test.ps1 -DEST_DIR %pathToInsert%