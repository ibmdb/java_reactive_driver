@echo on

copy %INSTALL_DIR%\config\* %INSTALL_DIR%\classes\.
copy %INSTALL_DIR%\tests\resources\* %INSTALL_DIR%\classes\.

java -cp "%cp%;resources" -Dfile.encoding=UTF8 com.ibm.db2.r2dbc.RunTest