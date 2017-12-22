
@echo off
set CUR_DIR=%cd%
echo %CUR_DIR%
java -cp OFTE.jar com.ofte.services.MetaDataCreations %*
