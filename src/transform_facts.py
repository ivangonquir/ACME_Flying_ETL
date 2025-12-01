import pandas as pd
import logging
import sys
from .settings import LOG_PATH, RAW_STAGING_DIR, TRANSFORMED_STAGING_DIR

# setup logging (write on both log file and terminal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH), 
        logging.StreamHandler(sys.stdout)    
    ]
)

def create_aircraft_utilization_fact(flights, mant_event, aircraft_dim, temp_dim):
    logging.info("Creating AircraftUtilization fact table...")

    # -- Process flights --

    # ensure dates
    flights['actualdeparture'] = pd.to_datetime(flights['actualdeparture'])
    flights['actualarrival'] = pd.to_datetime(flights['actualarrival'])
    flights['scheduleddeparture'] = pd.to_datetime(flights['scheduleddeparture'])
    flights['scheduledarrival'] = pd.to_datetime(flights['scheduledarrival'])

    # define the day for KPI calculations 
    # BR15: All the hours of a flight are imputed to the date of its scheduled Departure
    flights['timeID'] = pd.to_datetime(flights['scheduleddeparture']).dt.floor('D')

    # KPI 1: Flight Hours
    flight_seconds = (flights['actualarrival'] - flights['actualdeparture']).dt.total_seconds()
    flights['FlightHours'] = flight_seconds.fillna(0) / 3600

    # KPI 2: Flight Cycles
    flights['FlightCycles'] = flights.apply(lambda x: 0 if x['cancelled'] else 1, axis=1)
    
    # KPI 3: Cancellations
    flights['Cancelations'] = flights.apply(lambda x: 1 if x['cancelled'] else 0, axis=1)

    # KPI 4: Delay
    flights['Delays'] = flights.apply(lambda x: 0 if pd.isna(x['delaycode']) else 1, axis=1)

    # KPI 5: Delayed minutes
    difference_seconds = (flights['actualdeparture'] - flights['scheduleddeparture']).dt.total_seconds()
    flights['DelayedMinutes'] = difference_seconds / 60
    # if delaycode is NA then set delayedminutes to 0
    flights['DelayedMinutes'] = flights.apply(lambda x: 0 if pd.isna(x['delaycode']) else x['DelayedMinutes'], axis=1)

    # aggregate flights by day and aircraft
    flights_daily = flights.groupby(['aircraftregistration', 'timeID']).agg({
        'FlightHours': 'sum',
        'FlightCycles': 'sum',
        'Delays': 'sum',
        'Cancelations': 'sum',
        'DelayedMinutes': 'sum'
    }).reset_index().rename({'aircraftregistration': 'aircraftID'}, axis=1)

    print(flights_daily.head())

    # -- Process Maintenance Events --

    # ensure date 
    mant_event['starttime'] = pd.to_datetime(mant_event['starttime'])

    # define the day for KPI calculations
    mant_event['timeID'] = mant_event['starttime'].dt.floor('D')

    # KPI 6: Scheduled out of service
    scheduled_ous_types = ['Maintenance', 'Revision']
    mant_event['ScheduledOutOfService'] = mant_event.apply(lambda x: 1 if x['kind'] in scheduled_ous_types else 0, axis=1)

    # KPI 7: Unscheduled out of service
    unscheduled_ous_types = ['Delay', 'AircraftOnGround', 'Safety']
    mant_event['UnscheduledOutOfService'] = mant_event.apply(lambda x: 1 if x['kind'] in unscheduled_ous_types else 0, axis=1)

    # aggregate maintenance by day and aircraft
    maint_daily = mant_event.groupby(['aircraftregistration', 'timeID']).agg({
        'ScheduledOutOfService': 'sum',
        'UnscheduledOutOfService': 'sum'
    }).reset_index().rename({'aircraftregistration': 'aircraftID'}, axis=1)
    print()
    print(maint_daily.head())
    
    # CONTINUE!!

def create_logbook_reporting_fact():
    logging.info("Creating LogbookReporting fact table...")

    logging.info("LogbookReporting fact table successfully created.")

if __name__ == '__main__':
    # tables
    flights = pd.read_parquet(f'{RAW_STAGING_DIR}/flights.parquet')
    logbook = pd.read_parquet(f'{RAW_STAGING_DIR}/technicallogbookorders.parquet')
    mant_event = pd.read_parquet(f'{RAW_STAGING_DIR}/maintenanceevents.parquet')
    aircraft_lookup = pd.read_parquet(f'{RAW_STAGING_DIR}/aircraft_lookup.parquet')
    personnel_lookup = pd.read_parquet(f'{RAW_STAGING_DIR}/personnel_lookup.parquet')

    # dimensions
    aircraft_dim = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/AircraftDimension.parquet')
    people_dim = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/PeopleDimension.parquet')
    months_dim = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/Months.parquet')
    temp_dim = pd.read_parquet(f'{TRANSFORMED_STAGING_DIR}/TemporalDimension.parquet')

    create_aircraft_utilization_fact(flights, mant_event, aircraft_dim, temp_dim)