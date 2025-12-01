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

def process_flights_kpis(flights):
    """
    Step 1: Calculate daily flight statistics (Hours, Cycles, Delays).
    """
    logging.info("Processing Flight KPIs...")

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
    flights['flightHours'] = flight_seconds.fillna(0) / 3600

    # KPI 2: Flight Cycles
    flights['flightCycles'] = flights.apply(lambda x: 0 if x['cancelled'] else 1, axis=1)
    
    # KPI 3: Cancellations
    flights['cancellations'] = flights.apply(lambda x: 1 if x['cancelled'] else 0, axis=1)

    # KPI 4: Delay
    flights['delays'] = flights.apply(lambda x: 0 if pd.isna(x['delaycode']) else 1, axis=1)

    # KPI 5: Delayed minutes
    difference_seconds = (flights['actualdeparture'] - flights['scheduleddeparture']).dt.total_seconds()
    flights['delayedMinutes'] = difference_seconds / 60
    # if delaycode is NA then set delayedminutes to 0
    flights['delayedMinutes'] = flights.apply(lambda x: 0 if pd.isna(x['delaycode']) else x['delayedMinutes'], axis=1)

    # aggregate flights by day and aircraft
    flights_daily = flights.groupby(['aircraftregistration', 'timeID']).agg({
        'flightHours': 'sum',
        'flightCycles': 'sum',
        'delays': 'sum',
        'cancellations': 'sum',
        'delayedMinutes': 'sum'
    }).reset_index().rename({'aircraftregistration': 'aircraftID'}, axis=1)

    logging.info("Flight KPIs successfully processed.")
    return flights_daily

def process_maintenances_kpis(mant_event):
    """
    Step 2: Calculate daily maintenance statistics (Scheduled/Unscheduled Out of Service).
    """
    logging.info("Processing Maintenance KPIs...")

    # ensure date 
    mant_event['starttime'] = pd.to_datetime(mant_event['starttime'])

    # define the day for KPI calculations
    mant_event['timeID'] = mant_event['starttime'].dt.floor('D')

    # KPI 6: Scheduled out of service
    scheduled_ous_types = ['Maintenance', 'Revision']
    mant_event['scheduledOutOfService'] = mant_event.apply(lambda x: 1 if x['kind'] in scheduled_ous_types else 0, axis=1)

    # KPI 7: Unscheduled out of service
    unscheduled_ous_types = ['Delay', 'AircraftOnGround', 'Safety']
    mant_event['unScheduledOutOfService'] = mant_event.apply(lambda x: 1 if x['kind'] in unscheduled_ous_types else 0, axis=1)

    # aggregate maintenance by day and aircraft
    maint_daily = mant_event.groupby(['aircraftregistration', 'timeID']).agg({
        'scheduledOutOfService': 'sum',
        'unScheduledOutOfService': 'sum'
    }).reset_index().rename({'aircraftregistration': 'aircraftID'}, axis=1)

    logging.info("Maintenance KPIs successfully processed.")
    return maint_daily

def validate_ids_integrity(fact_df, aircraft_dim, temp_dim):
    """
    Step 4: Check if FKs exist in dimensions.
    """
    logging.info("Validating IDs integrity...")

    valid_aircraft_ids = set(aircraft_dim['ID'])
    valid_time_ids = set(temp_dim['ID'])

    # check aircraft ids
    unknown_aircrafts = set(fact_df['aircraftID']) - valid_aircraft_ids
    if unknown_aircrafts:
        logging.error(f"Integrity Error: {len(unknown_aircrafts)} aircrafts in fact not found in dimension.")

    # check time ids
    unknown_dates = set(fact_df['timeID']) - valid_time_ids
    if unknown_dates:
        logging.error(f"Integrity Error: {len(unknown_dates)} dates in fact not found in dimension.")

    logging.info("IDs integrtity validation finished.")

def enforce_schema_constraints(aircraft_utilization):
    """
    Step 5: rounding, clipping, and type conversion to match target SQL.
    """
    logging.info("Enforcing data to match target SQL schema constraints...")

    # round flight hours to fit target schema (NUMBER(2) is integer)
    aircraft_utilization['flightHours'] = aircraft_utilization['flightHours'].round(0)
    
    # handle overflow for NUMBER(2) columns
    for col in ['flightHours', 'flightCycles', 'delays', 'cancellations', 
                'scheduledOutOfService', 'unScheduledOutOfService']:
        if (aircraft_utilization[col] > 99).any():
            logging.warning(f"Clipping values > 99 in {col} to fit NUMBER(2) constraint.")
        
        # clip and convert to Int
        aircraft_utilization[col] = aircraft_utilization[col].clip(upper=99).astype(int)

    # handle overflow for NUMBER(3) column
    if (aircraft_utilization['delayedMinutes'] > 999).any():
        count = (aircraft_utilization['delayedMinutes'] > 999).sum()
        logging.warning(f"clipping {count} records in delayedMinutes > 999 to fit NUMBER(3).")

    aircraft_utilization['delayedMinutes'] = aircraft_utilization['delayedMinutes'].round(0).clip(upper=999).astype(int)
    
    # reorder columns to match target SQL
    target_cols = [
        'aircraftID', 'timeID', 
        'scheduledOutOfService', 'unScheduledOutOfService', 
        'flightHours', 'flightCycles', 
        'delays', 'delayedMinutes', 'cancellations'
    ]
    aircraft_utilization = aircraft_utilization[target_cols]

    logging.info("Data enforced to match target SQL schema constraints.")
    return aircraft_utilization

def create_aircraft_utilization_fact(fact_name, flights, mant_event, aircraft_dim, temp_dim):
    logging.info("Creating AircraftUtilization fact table...")

    # 1: process KPIs for flights
    flights_daily = process_flights_kpis(flights)

    # 2: process KPIs for maintenances
    maint_daily = process_maintenances_kpis(mant_event)

    # 3: outer join: union of flights and maintenances KPIs
    aircraft_utilization = pd.merge(
        flights_daily,
        maint_daily,
        on=['aircraftID', 'timeID'],
        how="outer"
    )

    # fill NaN values with 0 (e.g. a day with maintenance but no flights = NaN FlightHours)
    aircraft_utilization = aircraft_utilization.fillna(0)
    
    # 4: check that ids (FKs) in fact table exist in dimension tables
    validate_ids_integrity(aircraft_utilization, aircraft_dim, temp_dim) # raise error???

    # 5: final formatting
    aircraft_utilization = enforce_schema_constraints(aircraft_utilization)

    # load into staging area
    output_path = f'{TRANSFORMED_STAGING_DIR}/{fact_name}.parquet'
    aircraft_utilization.to_parquet(output_path, index=False)

    logging.info(f"AircraftUtilization fact table successfully created.")

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

    create_aircraft_utilization_fact('AircraftUtilization', flights, mant_event, aircraft_dim, temp_dim)