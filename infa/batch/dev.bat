@ECHO OFF
SETLOCAL

CALL "%~dp0dev.env"

pushd "C:\Informatica\10.4.1\clients\PowerCenterClient\CommandLineUtilities\PC\server\bin"

PmCmd.exe pingservice -sv %INFA_SERVER_DEV% -d %INFA_DOMAIN_DEV%