#!/bin/bash
# Auto-update script for GitHub Actions
# This fetches the latest edition, rebuilds data, and cleans it

set -e

echo "Fetching latest edition..."
python build/scraper.py --update --api-key "$ANTHROPIC_API_KEY" || true

echo "Checking if clean_data.py exists..."
if [ -f "build/clean_data.py" ]; then
    echo "Cleaning data..."
    python build/clean_data.py

    # Move cleaned file to correct location
    if [ -f "data/companies.json.cleaned" ]; then
        mv data/companies.json.cleaned data/companies_cleaned.json
    fi
else
    echo "No clean_data.py found, skipping cleaning step"
fi

echo "Update complete!"
