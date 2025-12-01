import pandas as pd
import logging
import sys
from .settings import LOG_PATH, TRANSFORMED_STAGING_DIR

# setup logging (write on both log file and terminal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH), 
        logging.StreamHandler(sys.stdout)    
    ]
)

def create_aircraft_dim(dim_name, flights, logbook, aircraft_lookup):
    logging.info("Creating Aircraft dimension...")

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

    # load dimension df into staging area
    output_path = f'{TRANSFORMED_STAGING_DIR}/{dim_name}.parquet'
    dim_aircraft.to_parquet(output_path, index=False)

    logging.info("Aircraft dimension successfully created and staged.")

def create_people_dim(dim_name, logbook, personnel_lookup):
    logging.info("Creating People dimension...")

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

    # load dimension df into staging area
    output_path = f'{TRANSFORMED_STAGING_DIR}/{dim_name}.parquet'
    dim_people.to_parquet(output_path, index=False)

    logging.info("People dimension successfully created and staged.")

def get_date_agg(df, column_name, agg_type):
    """
    Returns the max/min date in the column column_name of the df
        agg: min or max
    """
    series_tmp = pd.to_datetime(df[column_name])
    res = series_tmp.agg(agg_type)
    return res

def create_temporal_dims(temp_dim_name, months_dim_name, logbook, mant_event, flights):
    logging.info("Creating Temporal and Months dimensions...")

    # get min/max logbook reporting dates
    min_logbook = get_date_agg(logbook, 'reportingdate', 'min')
    max_logbook = get_date_agg(logbook, 'reportingdate', 'max')

    # get min/max maintenance dates
    mant_event['endtime'] = mant_event['starttime'] + mant_event['duration']
    min_maint = get_date_agg(mant_event, 'starttime', 'min')
    max_maint = get_date_agg(mant_event, 'endtime', 'max')

    # get min/max flight dates (scheduled and actual)
    min_scheduled_flight = get_date_agg(flights, 'scheduleddeparture', 'min')
    max_scheduled_flight = get_date_agg(flights, 'scheduledarrival', 'max')
    min_actual_flight = get_date_agg(flights, 'actualdeparture', 'min')
    max_actual_flight = get_date_agg(flights, 'actualarrival', 'max')

    # define global min and max date
    global_min_date = pd.to_datetime(pd.Series([min_logbook, min_maint, min_scheduled_flight, min_actual_flight]).min()).floor('D')
    global_max_date = pd.to_datetime(pd.Series([max_logbook, max_maint, max_scheduled_flight, max_actual_flight]).max()).ceil('D')
    logging.info(f'Temporal dimensions - Minimum date: {global_min_date}.')
    logging.info(f'Temporal dimensions - Maximum date: {global_max_date}.')

    # create day range
    day_range = pd.date_range(
        start=global_min_date,
        end=global_max_date,
        freq='D'
    )

    # create TemporalDimension (daily granularity)
    temporal_dim = pd.DataFrame({'ID': day_range})
    temporal_dim['monthID'] = temporal_dim['ID'].dt.strftime('%Y-%m')

    # create Months dimension (monthly granularity)
    months_dim = temporal_dim[['monthID']].drop_duplicates().copy()
    months_dim = months_dim.rename({'monthID': 'ID'}, axis=1)
    months_dim['y'] = pd.to_datetime(months_dim['ID']).dt.year

    # load dimensions df into staging area
    temp_output_path = f'{TRANSFORMED_STAGING_DIR}/{temp_dim_name}.parquet'
    months_output_path = f'{TRANSFORMED_STAGING_DIR}/{months_dim_name}.parquet'
    temporal_dim.to_parquet(temp_output_path, index=False)
    months_dim.to_parquet(months_output_path, index=False)

    logging.info("Temporal and Months dimensions successfully created and staged.")