@echo off

REM Check if Python is installed
python --version >nul 2>nul
IF %ERRORLEVEL% NEQ 0 (
    echo Python is not installed. Please install Python from https://www.python.org/downloads/.
    pause
    exit /b
)

REM Create a virtual environment in the 'venv' folder
echo Creating a virtual environment...
python -m venv venv

REM Activate the virtual environment
echo Activating the virtual environment...
call venv\Scripts\activate

REM Install dependencies from requirements.txt
echo Installing dependencies...
pip install -r requirements.txt

REM Navigate to the project directory
cd matweb_materials_scraper

title Materials Spider Runner

REM Prompt the user for spider selection
echo Select the spider(s) to run:
echo 1 - Make It From
echo 2 - MatDat Materials Databases and Services for Engineers and Scientists
echo 3 - MatWeb Material Property Data
echo 4 - TPSX Materials Properties Database
echo 5 - ASM International
echo 0 - Run all spiders
set /p spiders="Enter spider numbers (e.g. 1 2 3 for multiple spiders, or 0 for all): "

REM Check if the user wants to run all spiders
IF "%spiders%"=="0" (
    echo Running all spiders...
    echo Scraper 'Make It From' is currently running...
    start /wait cmd /c "title Make It From && scrapy crawl Make_It_From"

    echo Scraper 'MatDat Materials Databases and Services for Engineers and Scientists' is currently running...
    start /wait cmd /c "title MatDat Materials Databases and Services for Engineers and Scientists && scrapy crawl Mat_Dat"

    echo Scraper 'MatWeb Material Property Data' is currently running...
    start /wait cmd /c "title MatWeb Material Property Data && scrapy crawl MatWeb"

    echo Scraper 'TPSX Materials Properties Database' is currently running...
    start /wait cmd /c "title TPSX Materials Properties Database && scrapy crawl TPSX"

    echo Scraper 'ASM International' is currently running...
    start /wait cmd /c "title ASM International && scrapy crawl ASM"
) ELSE (
    REM Loop through the selected spider numbers and run the corresponding spiders in new cmd windows
    FOR %%i IN (%spiders%) DO (
        IF %%i==1 (
            echo Scraper 'Make It From' is currently running...
            start /wait cmd /c "title Make It From && scrapy crawl Make_It_From"
        )

        IF %%i==2 (
            echo Scraper 'MatDat Materials Databases and Services for Engineers and Scientists' is currently running...
            start /wait cmd /c "title MatDat Materials Databases and Services for Engineers and Scientists && scrapy crawl Mat_Dat"

        )

        IF %%i==3 (
            echo Scraper 'MatWeb Material Property Data' is currently running...
            start /wait cmd /c "title MatWeb Material Property Data && scrapy crawl MatWeb"
        )

        IF %%i==4 (
            echo Scraper 'TPSX Materials Properties Database' is currently running...
            start /wait cmd /c "title TPSX Materials Properties Database && scrapy crawl TPSX"
        )

        IF %%i==5 (
            echo Scraper 'ASM International' is currently running...
            start /wait cmd /c "title ASM International && scrapy crawl ASM"
        )
    )
)

REM Keep the main window open until all spiders finish
echo All selected spiders have completed. Press any key to exit the main window.
pause
