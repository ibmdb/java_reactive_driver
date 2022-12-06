@echo off

set path=%cd%\bin;%path%

set cp=.;%cd%

:: Add all dependent jars to envvar cp
for %%i in (lib\*.jar extlib\*.jar) do call :append %%i
:: echo Environment variable CP is set to - 
:: echo %cp%
:: echo Use CP for classpath

set CLASSPATH=%cp%;%CLASSPATH%

goto :end

:append
set file=%1
set file=%file:"=%
set cp=%cp%;%cd%\%file%

:end
