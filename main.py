import logging
import sys
from src.settings import LOG_PATH, CONFIG_PATH
from src.db_connection import DBConnector
from src.extractors import extract_table, extract_csv
from src.queries import AIMS_EXTRACTION, AMOS_EXTRACTION, CSV_EXTRACTION

# setup logging (write on both log file and terminal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH), 
        logging.StreamHandler(sys.stdout)    
    ]
)

def main():
    logging.info("ETL process started")

    # initialization of DB connections
    logging.info("Initializating DB connections...")
    dbc = DBConnector(config_path=CONFIG_PATH)
    aims_connection = dbc.get_connection("aims")
    logging.info("Connection with AIMS database established.")
    amos_connection = dbc.get_connection("amos")
    logging.info("Connection with AMOS database established.")

    # extract data from AIMS database: iterate over each table query
    logging.info("Extracting data from AIMS database...")
    for table_name, query in AIMS_EXTRACTION.items():
        extract_table(aims_connection, table_name, query)

    # extract data from AMOS database: iterate over each table query
    logging.info("Extracting data from AMOS database...")
    for table_name, query in AMOS_EXTRACTION.items():
        extract_table(amos_connection, table_name, query)

    # extract data from CSV files
    logging.info("Extracting data from CSV files...")
    for table_name, config in CSV_EXTRACTION.items():
        extract_csv(table_name, config)

    # data validation
    logging.info("Validating data...")

if __name__=='__main__':
    main()
    