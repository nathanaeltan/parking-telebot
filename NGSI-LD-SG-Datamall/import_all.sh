#!/bin/bash
echo "Activating virtual environment..."
source venv/bin/activate

echo "Running Python script..."
python import_ura_parking.py
python import_weather.py
python import_hdb_parking.py

read -p "Press any key to continue..."