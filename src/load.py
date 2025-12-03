import logging
import sys
import warnings
from sqlalchemy import text
from src.settings import LOG_PATH

# setup logging (write on both log file and terminal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH), 
        logging.StreamHandler(sys.stdout)    
    ]
)

# ignore false positive warning
warnings.filterwarnings(
    "ignore", 
    message="The provided table name", 
    category=UserWarning
)

def clean_target_tables(dw_connection):
    """
    Deletes data from target tables in specific order to respect FKs.
    Order: facts first, then dimensions.
    """
    logging.info("Cleaning target tables (DELETE mode)...")
    
    # sequence of tables to clean
    tables_to_clean = [
        "LOGBOOKREPORTING",      
        "AIRCRAFTUTILIZATION",   
        "PEOPLEDIMENSION",       
        "AIRCRAFTDIMENSION",     
        "TEMPORALDIMENSION",     
        "MONTHS"                 
    ]

    with dw_connection.connect() as conn:
        for table in tables_to_clean:
            try:
                logging.info(f"Deleting data from {table}...")
                conn.execute(text(f"DELETE FROM {table}"))
                conn.commit()
            except Exception as e:
                logging.error(f"Failed to clean {table}: {e}")
                raise e
                
    logging.info("Target tables successfully cleaned.")

def prepare_for_oracle(df):
    """
    Converts dataframe column names to uppercase to match Oracle format.
    """
    df.columns = [c.upper() for c in df.columns]
    return df

def load_table(dw_connection, df, table_name):
    """
    Loads dataframe into Oracle DB table.
    """
    df = prepare_for_oracle(df)

    try:
        df.to_sql(
            name=table_name,
            con=dw_connection,
            if_exists='append',  
            index=False,         
            chunksize=1000,      
            method=None          
        )
        logging.info(f"Successfully loaded data into {table_name}.")

    except Exception as e:
        logging.error(f"Failed to load table {table_name}: {e}")
        raise e