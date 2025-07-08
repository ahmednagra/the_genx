@echo off

REM Check if Python is installed
python --version >nul 2>nul
IF %ERRORLEVEL% NEQ 0 (
    echo Python is not installed. Please install Python from https://www.python.org/downloads/.
    pause
    exit /b
)

REM Navigate to the project directory
cd Ecommerce_Websites_Scraper

REM Create a virtual environment in the 'venv' folder
echo Creating a virtual environment...
python -m venv venv

REM Activate the virtual environment
echo Activating the virtual environment...
call venv\Scripts\activate

REM Install dependencies from requirements.txt
echo Installing dependencies...
pip install -r requirements.txt

title Scrapy Spider Runner

REM Prompt the user for spider selection
echo Select the spider(s) to run:
echo 1 - Bath and Body
echo 2 - Blooming Dales
echo 3 - Diesel
echo 4 - Farfetch
echo 5 - Luxury Closet
echo 6 - MAje
echo 7 - New Balance
echo 8 - Sandro
echo 9 - Ted Baker
echo 0 - Run all spiders
set /p spiders="Enter spider numbers (e.g. 1 2 3 for multiple spiders, or 0 for all): "

REM Check if the user wants to run all spiders
IF "%spiders%"=="0" (
    echo Running all spiders...
    echo Scraper 'Bath and Body' is currently running...
    start /wait cmd /c "title Bath and Body && scrapy crawl BathandBody"

    echo Scraper 'Blooming Dales' is currently running...
    start /wait cmd /c "title Blooming Dales && scrapy crawl BloomingDales"

    echo Scraper 'Diesel' is currently running...
    start /wait cmd /c "title Diesel && scrapy crawl Diesel"

    echo Scraper 'Farfetch' is currently running...
    start /wait cmd /c "title Farfetch && scrapy crawl FarFetch"

    echo Scraper 'Luxury Closet' is currently running...
    start /wait cmd /c "title Luxury Closet && scrapy crawl luxuryCloset"

    echo Scraper 'MAje' is currently running...
    start /wait cmd /c "title MAje && scrapy crawl Maje"

    echo Scraper 'New Balance' is currently running...
    start /wait cmd /c "title New Balance && scrapy crawl NewBalance"

    echo Scraper 'Sandro' is currently running...
    start /wait cmd /c "title Sandro && scrapy crawl Sandro"

    echo Scraper 'Ted Baker' is currently running...
    start /wait cmd /c "title Ted Baker && scrapy crawl TedBaker"
) ELSE (
    REM Loop through the selected spider numbers and run the corresponding spiders in new cmd windows
    FOR %%i IN (%spiders%) DO (
        IF %%i==1 (
            echo Scraper 'Bath and Body' is currently running...
            start /wait cmd /c "title Bath and Body && scrapy crawl BathandBody"
        )

        IF %%i==2 (
            echo Scraper 'Blooming Dales' is currently running...
            start /wait cmd /c "title Blooming Dales && scrapy crawl BloomingDales"
        )

        IF %%i==3 (
            echo Scraper 'Diesel' is currently running...
            start /wait cmd /c "title Diesel && scrapy crawl Diesel"
        )

        IF %%i==4 (
            echo Scraper 'Farfetch' is currently running...
            start /wait cmd /c "title Farfetch && scrapy crawl FarFetch"
        )

        IF %%i==5 (
            echo Scraper 'Luxury Closet' is currently running...
            start /wait cmd /c "title Luxury Closet && scrapy crawl luxuryCloset"
        )

        IF %%i==6 (
            echo Scraper 'MAje' is currently running...
            start /wait cmd /c "title MAje && scrapy crawl Maje"
        )

        IF %%i==7 (
            echo Scraper 'New Balance' is currently running...
            start /wait cmd /c "title New Balance && scrapy crawl NewBalance"
        )

        IF %%i==8 (
            echo Scraper 'Sandro' is currently running...
            start /wait cmd /c "title Sandro && scrapy crawl Sandro"
        )

        IF %%i==9 (
            echo Scraper 'Ted Baker' is currently running...
            start /wait cmd /c "title Ted Baker && scrapy crawl TedBaker"
        )
    )
)

REM Keep the main window open until all spiders finish
echo All selected spiders have completed. Press any key to exit the main window.
pause
