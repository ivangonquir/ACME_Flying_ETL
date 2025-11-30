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

def create_aircraft_dim(flights, logbook, aircraft_lookup):
    logging.info('Starting aircraft dimension creation...')
    
    # extract aircraft IDs from flights and technical logbook
    aims_aircraft = flights[['aircraftregistration']].rename({'aircraftregistration': 'ID'}, axis=1)
    amos_aircraft = logbook[['aircraftregistration']].rename({'aircraftregistration': 'ID'}, axis=1)

    # union: combine IDs and remove duplicates
    all_aircraft = pd.concat([aims_aircraft, amos_aircraft]).drop_duplicates().reset_index(drop=True)

    # join with aircraft lookup to obtain model and manufacturer
    dim_aircraft = pd.merge(
        all_aircraft,
        aircraft_lookup,
        left_on='ID',
        right_on='aircraft_reg_code',
        how='left'
    )

    # logging of aircrafts that were not found in aircraft lookup
    no_matches_ids = dim_aircraft[dim_aircraft.manufacturer.isna()].ID.values
    for id in no_matches_ids:
        logging.warning(f'Warning: Aircraft {id} was not found in aircraft lookup')

    # set model and manufacturer of aircrafts that were not found in aircraft lookup as unknown
    dim_aircraft.manufacturer.fillna('Unknown')
    dim_aircraft.aircraft_model.fillna('Unknown')

    logging.info("Aircraft dimension sucessfully created.")
    return dim_aircraft[['ID', 'aircraft_model', 'manufacturer']].rename({'aircraft_model': 'model'}, axis=1)

if __name__ == '__main__':
    flights = pd.read_parquet(f'{RAW_STAGING_DIR}/flights.parquet')
    logbook = pd.read_parquet(f'{RAW_STAGING_DIR}/technicallogbookorders.parquet')
    aircraft_lookup = pd.read_parquet(f'{RAW_STAGING_DIR}/aircraft_lookup.parquet')

    aircraft_dim = create_aircraft_dim(flights, logbook, aircraft_lookup)
    print(aircraft_dim.head())


