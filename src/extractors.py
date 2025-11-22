import pandas as pd
import logging
import sys
from .settings import LOG_PATH, RAW_STAGING_DIR

# setup logging (write on both log file and terminal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH), 
        logging.StreamHandler(sys.stdout)    
    ]
)

def extract_table(db_connection, table_name, query):
    
    output_path = f"{RAW_STAGING_DIR}/{table_name}.parquet"

    try:
        # read table from the database
        table = pd.read_sql(query, db_connection)
        
        # store table into parquet file
        table.to_parquet(output_path, index=False)

        logging.info(f"Table {table_name} successfully extracted.")
    
    except Exception as e:
        logging.error(f"Error extracting {table_name}: {e}")
        raise e
    
def extract_csv():
    pass
    