import logging
import sys
import os
import argparse
import pandas as pd
from src.settings import LOG_PATH, CONFIG_PATH, RAW_STAGING_DIR, TRANSFORMED_STAGING_DIR
from src.db_connection import DBConnector
from src.extract import extract_table, extract_csv
from src.queries import AIMS_EXTRACTION, AMOS_EXTRACTION, CSV_EXTRACTION
from src.transform_dims import create_aircraft_dim, create_people_dim, create_temporal_dims
from src.transform_facts import create_aircraft_utilization_fact, create_logbook_reporting_fact
from src.load import load_table, clean_target_tables

# setup logging (write on both log file and terminal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH), 
        logging.StreamHandler(sys.stdout)    
    ]
)

def run_extraction(dbc):
    logging.info("Starting extraction process...")

    # AIMS and AMOS connection
    aims_connection = dbc.get_connection("aims")
    amos_connection = dbc.get_connection("amos")

    # extract data from AIMS database: iterate over each table query
    for table_name, query in AIMS_EXTRACTION.items():
        extract_table(aims_connection, table_name, query)

    # extract data from AMOS database: iterate over each table query
    for table_name, query in AMOS_EXTRACTION.items():
        extract_table(amos_connection, table_name, query)

    # extract data from CSV files
    for table_name, config in CSV_EXTRACTION.items():
        extract_csv(table_name, config)
    
    logging.info("Data successfully extracted and stored into staging area.")

def run_validation():
    logging.info("Starting validation process...")
    
    # Helper to safely load parquet files or return empty DF if not found
    def safe_read(filename):
        path = f'{RAW_STAGING_DIR}/{filename}.parquet'
        if os.path.exists(path):
            return pd.read_parquet(path)
        logging.warning(f"File {filename}.parquet not found. Skipping related validations.")
        return pd.DataFrame()

    # Load data needed for validation
    # Adjust filenames to match exactly what your 'extract_table' functions output
    flights = safe_read('flights')
    logbook = safe_read('technicallogbookorders') # Assumed filename based on query dict key
    maintenance = safe_read('maintenanceevents')
    op_interruption = safe_read('operationinterruption')
    work_packages = safe_read('workpackages')
    work_orders = safe_read('workorders')
    attachments = safe_read('attachments')

def run_transformation():
    logging.info("Starting data transformation - Dimensions creation...")

    # read cleaned data (!! CHANGE RAW DIR BY CLEANED DIR !!)
    flights = pd.read_parquet(f'{RAW_STAGING_DIR}/flights.parquet')
    logbook = pd.read_parquet(f'{RAW_STAGING_DIR}/technicallogbookorders.parquet')
    mant_event = pd.read_parquet(f'{RAW_STAGING_DIR}/maintenanceevents.parquet')
    aircraft_lookup = pd.read_parquet(f'{RAW_STAGING_DIR}/aircraft_lookup.parquet')
    personnel_lookup = pd.read_parquet(f'{RAW_STAGING_DIR}/personnel_lookup.parquet')

    # create and store dimensions
    aircraft_dim = create_aircraft_dim('AircraftDimension', flights, logbook, aircraft_lookup)
    people_dim = create_people_dim('PeopleDimension', logbook, personnel_lookup)
    temp_dim, months_dim = create_temporal_dims('TemporalDimension', 'Months', logbook, mant_event, flights)

    logging.info("Data transformation - Fact tables creation...")

    # create and store fact tables
    create_aircraft_utilization_fact('AircraftUtilization', flights, mant_event, aircraft_dim, temp_dim)
    create_logbook_reporting_fact('LogBookReporting', logbook, aircraft_dim, months_dim, people_dim)

    logging.info("Data transformed and dimensions/fact tables created and stored into staging area successfully.")

def run_loading(dbc):
    logging.info("Starting Fact and Dimension loading into target SQL tables (DW)...")

    # read transformed dimensions and facts
    aircraft_dim = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/AircraftDimension.parquet')
    months_dim = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/Months.parquet')
    temp_dim = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/TemporalDimension.parquet')
    people_dim = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/PeopleDimension.parquet')
    aircraft_utilization_fact = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/AircraftUtilization.parquet')
    logbook_reporting_fact = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/LogBookReporting.parquet')

    # get DW oracle connection
    logging.info("Connecting to Oracle DB...")
    dw_connection = dbc.get_connection("dw")
    logging.info("Connection to Oracle DB established.")

    # delete tables before inserting (to allow rerun ETL)
    # clean_target_tables(dw_connection)

    # load dimensions and facts into oracle database (DW)
    loading_sequence = [
        (aircraft_dim, "AIRCRAFTDIMENSION"),
        (months_dim, "MONTHS"),
        (temp_dim, "TEMPORALDIMENSION"),
        (people_dim, "PEOPLEDIMENSION"),
        (aircraft_utilization_fact, "AIRCRAFTUTILIZATION"),
        (logbook_reporting_fact, "LOGBOOKREPORTING")
    ]

    for df, table_name in loading_sequence:
        load_table(dw_connection, df, table_name)

    logging.info("Facts and dimensions successfully loaded into the DW.")


def main():
    # setup arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--extract', action='store_true', help='Run extraction')
    parser.add_argument('--validate', action='store_true', help='Run validation')
    parser.add_argument('--transform', action='store_true', help='Run transformation')
    parser.add_argument('--load', action='store_true', help='Run loading')
    args = parser.parse_args()

    if not any([args.extract, args.validate, args.transform, args.load]):
        run_all = True
    else:
        run_all = False

    logging.info("ETL process started")

    # initialization of DB connections
    dbc = None
    if run_all or args.extract or args.load:
        dbc = DBConnector(config_path=CONFIG_PATH)

    if run_all or args.extract:
        run_extraction(dbc)
    
    # if run_all or args.validate:
    #     run_validation()
    
    if run_all or args.transform:
        run_transformation()
    
    if run_all or args.load:
        run_loading(dbc)
    
    logging.info("ETL process finished")
    

if __name__=='__main__':
    main()
    