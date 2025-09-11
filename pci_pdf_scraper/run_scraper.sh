#!/bin/bash

# Create and activate virtual environment
echo "Setting up virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Run the scraper
echo "Starting PCI Standards document scraper..."
./venv/bin/python pci_scraper.py

echo "Scraping completed!"