@echo on

copy %INSTALL_DIR%\config\* %INSTALL_DIR%\classes\.
copy %INSTALL_DIR%\tests\resources\* %INSTALL_DIR%\classes\.

if %1. == . goto runall

java -cp "%cp%;resources" -Dfile.encoding=UTF8 org.junit.runner.JUnitCore %1
goto done

:runall
java -cp "%cp%;resources" -Dfile.encoding=UTF8 org.junit.runner.JUnitCore com.ibm.db2.r2dbc.TestSuite

:done