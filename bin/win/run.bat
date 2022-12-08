@echo on

copy %INSTALL_DIR%\config\* %INSTALL_DIR%\classes\.
copy %INSTALL_DIR%\tests\resources\* %INSTALL_DIR%\classes\.

java -cp %cp% -Dfile.encoding=UTF8 %1%
