import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Set up logging
logging.basicConfig(filename='client_data_processing.log', level=logging.INFO)


# Function to rename columns
def rename_columns(df):
    return df.withColumnRenamed("id", "client_identifier") \
             .withColumnRenamed("email", "client_email") \
             .withColumnRenamed("btc_a", "bitcoin_address") \
             .withColumnRenamed("cc_t", "credit_card_type")

# function to process data
def process_data(client_file_path, financial_file_path, countries):
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
                            .appName("Client Data Processing") \
                            .getOrCreate()
        
        # Read datasets
        client_data = spark.read.option("header", True).csv(client_file_path)
        financial_data = spark.read.option("header", True).csv(financial_file_path)
        
        # filter data based on UK and the Netherlands
        client_data_filtered = client_data.filter((col("country") == "United Kingdom") | (col("country") == "Netherlands"))
        
        # Remove credit card number from financial data
        financial_data_without_ccn = financial_data.drop("cc_n")
        
        # Joining the datasets
        joined_data = client_data_filtered.join(financial_data_without_ccn, "client_identifier", "inner")
        
        # Rename columns
        renamed_data = rename_columns(joined_data)
        
        # Write output
        output_path = "client_data/"
        renamed_data.write.mode("overwrite").parquet(output_path)
        logging.info("Processed data successfully saved to {}".format(output_path))
        
    except Exception as e:
        logging.error("An error occurred: {}".format(str(e)))
        raise

if __name__ == "__main__":
    
    client_file_path = "dataset_one.csv"  
    financial_file_path = "dataset_two.csv" 
    countries = sys.argv[3].split(",")
    
    process_data(client_file_path, financial_file_path, countries)
