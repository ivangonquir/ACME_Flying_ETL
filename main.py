import logging
import sys
from src.connectors import DBConnector

# setup logging (only write on log file)
# logging.basicConfig(filename='logs/etl_execution.log', level=logging.INFO, 
#                     format='%(asctime)s:%(levelname)s:%(message)s')

# setup logging (write on both log file and terminal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/etl_execution.log"), 
        logging.StreamHandler(sys.stdout)    
    ]
)

CONFIG_PATH = "config/db_config.yaml"

def main():
    logging.info("ETL process started")

    # initialization of DB connector
    dbc = DBConnector(config_path=CONFIG_PATH)

    # extract data
    logging.info("Extracting data...")


if __name__=='__main__':
    main()
    