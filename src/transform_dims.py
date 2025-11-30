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

    # left join (all aircrafts from flights and logbook with aircraft lookup)
    # keep aircrafts even if they do not appear in the lookup
    dim_aircraft = pd.merge(
        all_aircraft,
        aircraft_lookup,
        left_on='ID',
        right_on='aircraft_reg_code',
        how='left'
    )

    # logging of aircrafts not found in aircraft lookup
    no_matches_ids = dim_aircraft[dim_aircraft.manufacturer.isna()].ID.values
    if len(no_matches_ids) > 0:
        logging.warning(f'Warning: {len(no_matches_ids)} aircrafts not found in lookup. IDs: {no_matches_ids}')

    # set model and manufacturer of aircrafts not found in lookup as unknown
    dim_aircraft.manufacturer.fillna('UNK')
    dim_aircraft.aircraft_model.fillna('UNK')

    # keep relevant columns and rename them 
    dim_aircraft = dim_aircraft[['ID', 'aircraft_model', 'manufacturer']].rename({'aircraft_model': 'model'}, axis=1)

    logging.info("Aircraft dimension sucessfully created.")
    return dim_aircraft

def create_people_dim(logbook, personnel_lookup):
    logging.info('Starting people dimension creation...')

    # get unique reporteurs from logbook table
    logbook_people = logbook[['reporteurclass', 'reporteurid']].drop_duplicates()

    # left join (technical logbook with personnel lookup): 
    # keep reporteurs even if they do not appear in the lookup
    dim_people = pd.merge(
        logbook_people, 
        personnel_lookup,
        on='reporteurid',
        how='left'
    )

    # logging of reporteurs not found in personnel lookup
    no_matches_ids = dim_people[dim_people.airport.isna()].reporteurid.values
    if len(no_matches_ids) > 0:
        logging.warning(f'Warning: {len(no_matches_ids)} reporteurs not found in lookup. IDs: {no_matches_ids}')

    # set airports of reporteurs not found in lookup as unknown
    dim_people['airport'] = dim_people['airport'].fillna('UNK')

    # transform reporteur class format
    dim_people['reporteurclass'] = dim_people['reporteurclass'].map({
        'MAREP': 'M',
        'PIREP': 'P'
    })

    # reorder and rename columns
    dim_people = dim_people[['reporteurid', 'airport', 'reporteurclass']].rename(columns={
        'reporteurid': 'ID',
        'reporteurclass': 'role'
    })

    logging.info("People dimension sucessfully created.")
    return dim_people

if __name__ == '__main__':
    flights = pd.read_parquet(f'{RAW_STAGING_DIR}/flights.parquet')
    logbook = pd.read_parquet(f'{RAW_STAGING_DIR}/technicallogbookorders.parquet')
    aircraft_lookup = pd.read_parquet(f'{RAW_STAGING_DIR}/aircraft_lookup.parquet')
    personnel_lookup = pd.read_parquet(f'{RAW_STAGING_DIR}/personnel_lookup.parquet')

    aircraft_dim = create_aircraft_dim(flights, logbook, aircraft_lookup)
    print(aircraft_dim.head())

    people_dim = create_people_dim(logbook, personnel_lookup)
    print(people_dim.head())

