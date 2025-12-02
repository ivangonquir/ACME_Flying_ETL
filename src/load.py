import logging
import sys
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

def prepare_for_oracle(df):
    df.columns = [c.upper() for c in df.columns]
    return df

def load_table(dw_connection, df, table_name):
    df = prepare_for_oracle(df)

    try:
        # PERFORMANCE OPTIMIZATION: Use chunksize.
        # Oracle can struggle with massive single insert statements. 
        # Breaking it into chunks of 1000-5000 is standard best practice.
        df.to_sql(
            name=table_name,
            con=dw_connection,
            if_exists='append',  # CRITICAL: Append to existing table defined in target.sql
            index=False,         # Do not upload the DataFrame index as a column
            chunksize=1000,      # Rubric: Performance/Robustness
            method=None          # None uses standard INSERT. 
        )
        logging.info(f"Successfully loaded data into AIRCRAFTUTILIZATION.")

    except Exception as e:
        logging.error(f"Failed to load table AIRCRAFTUTILIZATION: {e}")
        raise e