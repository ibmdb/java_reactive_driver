@echo off

if %1. == . goto usage

set MAVEN_REPO=https://repo1.maven.org/maven2

if exist lib goto :lib
mkdir lib

:lib
cd lib
curl -O %MAVEN_REPO%/com/ibm/db2/db2-r2dbc/%1/db2-r2dbc-%1.jar
curl -O %MAVEN_REPO%/com/ibm/db2/db2-r2dbc/%1/db2-r2dbc-%1-javadoc.jar
cd ..

if exist swidtag goto :swidtag
mkdir swidtag

:swidtag
cd swidtag
curl -O %MAVEN_REPO%/com/ibm/db2/db2-r2dbc/%1/ibm.com_IBM_Db2_Java_Reactive_Driver-%1.swidtag
cd ..

goto done

:usage
echo     Error: expecting arguments
echo:
echo     Usage: 
echo     get_driver ^<version^> 
echo         version - driver version number, example 1.1.0
echo:

:done
