@echo off
setlocal
REM written thanks to ChatGPT !

REM Determine the directory of the batch file
set "script_dir=%~dp0"

REM Specify the relative or absolute path to your Python script
set "python_script=main.py"

REM Run the Python script using the determined script path
python "%script_dir%%python_script%"

REM Prevent the command prompt window from closing automatically
pause

endlocal
