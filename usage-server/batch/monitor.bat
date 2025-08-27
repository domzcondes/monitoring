@echo off
REM Activate virtual environment and run monitoring script

set "VENV_DIR=update this"
set "SCRIPT_PATH=update this monitor.py"

REM Activate the virtual environment
call "%VENV_DIR%\Scripts\activate.bat"

REM Run the Python script
python "%SCRIPT_PATH%"

REM Deactivate the venv (only needed if this is a console session)
call "%VENV_DIR%\Scripts\deactivate.bat"

echo Monitoring script executed.