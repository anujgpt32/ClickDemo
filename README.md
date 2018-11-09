# ClickDemo
Demo Project implemented Click for Python to create CLI tools

This project uses Click==7.0.0 to create own Command Line Interface Tools.
This project reads CSV, XLS/XLSX, TSV and imports its data to the MongoDB collection using importdb. Not only, it imports the data to the
database, but also exports it to the CSV, XLS/XLSX, TSV, JSON using exportdb.

# Basic Requirements:
1. Python 3.5.4.
2. Mongo DB Shell 4.0.3
3. Virtual Environment

# Setup:
1. Create virtual environment
2. pip install -r requirements.txt

# Help:
python main.py --help

python main.py importdb --help

python main.py exportdb --help
