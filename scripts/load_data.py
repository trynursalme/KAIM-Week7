# script: load_data.py
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
import re  # Import the 're' module for regular expressions

# Load environment variables from .env file
load_dotenv()

# Fetch database connection parameters from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def load_data_from_postgres(query):
    """
    Connects to the PostgreSQL database and loads data based on the provided SQL query.
    
    :param query: SQL query to execute.
    :return: DataFrame containing the results of the query.
    """
    try:
        # Establish a connection to the database
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        # Load the data using pandas
        df = pd.read_sql_query(query, connection)
        
        # Close the database connection
        connection.close()
        
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def export_data_to_postgres(df, table_name):
    """
    Exports a DataFrame to the PostgreSQL database, handling both text and image data.
    
    :param df: DataFrame to export.
    :param table_name: Name of the table where data will be inserted/updated.
    :return: None
    """
    try:
        # Create a SQLAlchemy engine
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        # Export the DataFrame to the database (append mode)
        df.to_sql(table_name, engine, if_exists='append', index=False)
        
        print(f"Data successfully exported to {table_name} table in the database.")
        
    except Exception as e:
        print(f"An error occurred during export: {e}")

def process_and_store_data(file_path, table_name):
    """
    Processes the data, including text and image paths, and stores it in PostgreSQL.
    
    :param file_path: Path to the CSV file containing data.
    :param table_name: Name of the PostgreSQL table where data will be stored.
    """
    # Step 1: Read the CSV file
    df = pd.read_csv(file_path)

    # Step 2: Data cleaning (remove emojis, duplicates, etc.)
    def remove_emojis(text):
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   "]+", flags=re.UNICODE)
        return emoji_pattern.sub(r'', text)

    df['Message'] = df['Message'].apply(remove_emojis)
    
    # Remove duplicates
    df.drop_duplicates(subset=['ID'], inplace=True)

    # Step 3: Validate the media paths (ensure image files exist)
    df['Media Path'] = df['Media Path'].apply(lambda x: x if os.path.isfile(x) else None)
    
    # Step 4: Store cleaned data (with image paths) in PostgreSQL
    export_data_to_postgres(df, table_name)

# Example usage
if __name__ == "__main__":
    # Example CSV file path
    file_path = "C:/Users/NurselamHussen-ZOAEt/Downloads/New folder/10 Academy-project/Week-7/notebooks/df_cleaned_full.csv"
    
    # Define the PostgreSQL table name
    table_name = "cleandata"
    
    # Process and store the data
    process_and_store_data(file_path, table_name)
