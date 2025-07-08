@echo off
setlocal

REM Set virtual environment folder name
set VENV_DIR=venv

REM Check if Python is installed
python --version >nul 2>nul
IF %ERRORLEVEL% NEQ 0 (
    echo Python is not installed. Please install it from https://www.python.org/downloads/.
    pause
    exit /b
)

REM Create virtual environment if it doesn't exist
IF NOT EXIST %VENV_DIR% (
    echo Creating virtual environment...
    python -m venv %VENV_DIR%
)

REM Activate virtual environment
call %VENV_DIR%\Scripts\activate.bat

REM Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip

REM Install dependencies
echo Installing dependencies from requirements.txt...
pip install -r requirements.txt

REM Run the Scrapy spider
title Real Estate Redfin Scraper Runner
echo Running the 'Redfin' scraper...
scrapy crawl Redfin

REM Keep the console open after execution
echo.
echo Scraper execution completed. Press any key to exit.
pause
endlocal
