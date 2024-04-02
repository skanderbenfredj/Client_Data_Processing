# Client Data Processing with PySpark

This script processes client data and financial data using PySpark, a Python library for distributed data processing. The purpose of this script is to collate information about clients from two separate datasets, filter clients based on specified countries, and prepare the data for a new marketing push by a fictional company called KommatiPara.

## Features
- Reads client data and financial data from CSV files.
- Filters client data based on countries specified as "United Kingdom" or "Netherlands".
- Removes credit card numbers from financial data.
- Joins the filtered client data with financial data using the client identifier.
- Renames columns for easier readability.
- Outputs the processed data to a Parquet file in the client_data/ directory.

## Usage
To run the script, execute the following command:

bash
spark-submit data_processing.py <financial_file_path> <country1,country2>

<financial_file_path> : the path to the financial data file (dataset_two.csv) 
<country1,country2> : comma-separated list of countries (example : "United Kingdom,Netherlands")
