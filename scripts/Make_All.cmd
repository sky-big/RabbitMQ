ECHO on

del ..\ebin\*.beam

 cd /d %0/..

set MAKEFILE_FILE_CMD=%~dp0

cd /d %MAKEFILE_FILE_CMD%

copy Emakefile Emakefile.win

escript.exe erl_make.erl

pause
