#!/bin/bash
set -e

# Set virtual environment directory
VENV_DIR="venv"

# Check if Python is installed
if ! command -v python3 &> /dev/null
then
    echo "Python3 is not installed. Please install it from https://www.python.org/downloads/mac-osx/"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate the virtual environment
echo "Activating the virtual environment..."
source "$VENV_DIR/bin/activate"

# Install dependencies
echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt



# Run the Scrapy spider
echo "Running the 'Redfin' scraper..."
scrapy crawl Redfin

# Keep the terminal open after execution (optional for GUI terminals like iTerm or Terminal.app)
echo
read -p "Scraper execution completed. Press Enter to exit."