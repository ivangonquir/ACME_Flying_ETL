import pandas as pd
import numpy as np
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

# ==========================================
# SHARED UTILITY FUNCTION
# ==========================================

def validate_and_filter_integrity(fact_df, valid_ids, col_name, table_name):
    """
    Generic function to check and filter rows violating referential integrity.
    Returns: cleaned dataframe
    """
    logging.info(f"Referential integrity validation: checking that all {col_name} in {table_name} are present in dimension...")
    initial_count = len(fact_df)
    
    # Filter: Keep only rows where the FK exists in the Valid IDs set
    fact_df_clean = fact_df[fact_df[col_name].isin(valid_ids)].copy()
    
    dropped_count = initial_count - len(fact_df_clean)
    
    if dropped_count > 0:
        logging.warning(f"Integrity Action: Dropped {dropped_count} rows in {table_name} fact due to missing {col_name} in Dimensions.")
        rejected = fact_df[~fact_df[col_name].isin(valid_ids)]
        rejected.to_csv(f"data/rejected/{table_name}_{col_name}_errors.csv")
    
    logging.info(f"Referential integrity validation of {col_name} in {table_name} finished.")
    return fact_df_clean


# ==========================================
# AIRCRAFT UTILIZATION LOGIC
# ==========================================

def process_flights_kpis(flights):
    """
    Calculate daily flight KPIs for each aircraft (hours, cycles, delays).
    """
    logging.info("Processing Flight KPIs for AircraftUtilization...")

    # ensure dates
    date_cols = ['actualdeparture', 'actualarrival', 'scheduleddeparture', 'scheduledarrival']
    for col in date_cols:
        flights[col] = pd.to_datetime(flights[col])

    # define the day for KPI calculations 
    # BR15: All the hours of a flight are imputed to the date of its scheduled Departure
    flights['timeID'] = pd.to_datetime(flights['scheduleddeparture']).dt.floor('D')

    # KPI 1: Flight Hours
    flight_seconds = (flights['actualarrival'] - flights['actualdeparture']).dt.total_seconds()
    flights['flightHours'] = flight_seconds.fillna(0) / 3600

    # KPI 2 and 3: Flight Cycles and Cancellations
    flights['flightCycles'] = np.where(flights['cancelled'], 0, 1)
    flights['cancellations'] = np.where(flights['cancelled'], 1, 0)

    # KPI 4: Delay
    flights['delays'] = flights['delaycode'].notna().astype(int)

    # KPI 5: Delayed minutes
    delayed_minutes = (flights['actualdeparture'] - flights['scheduleddeparture']).dt.total_seconds() / 60
    flights['delayedMinutes'] = np.where(
        flights['delaycode'].notna(), 
        delayed_minutes, # if delaycode exists, set delayed minutes
        0                # if delaycode not exists, set 0
    )

    # aggregate flights by day and aircraft
    flights_daily = flights.groupby(['aircraftregistration', 'timeID']).agg({
        'flightHours': 'sum',
        'flightCycles': 'sum',
        'delays': 'sum',
        'cancellations': 'sum',
        'delayedMinutes': 'sum'
    }).reset_index().rename({'aircraftregistration': 'aircraftID'}, axis=1)

    logging.info("Flight KPIs for AircraftUtilization successfully processed.")
    return flights_daily

def process_maintenances_kpis(mant_event):
    """
    Calculate daily maintenance statistics (Scheduled/Unscheduled out of service).
    """
    logging.info("Processing Maintenance KPIs for AircraftUtilization...")

    # ensure date 
    mant_event['starttime'] = pd.to_datetime(mant_event['starttime'])

    # define the day for KPI calculations
    mant_event['timeID'] = mant_event['starttime'].dt.floor('D')

    # KPI 6: Scheduled out of service
    scheduled_ous_types = ['Maintenance', 'Revision']
    mant_event['scheduledOutOfService'] = mant_event['kind'].isin(scheduled_ous_types).astype(int)

    # KPI 7: Unscheduled out of service
    unscheduled_ous_types = ['Delay', 'AircraftOnGround', 'Safety']
    mant_event['unScheduledOutOfService'] = mant_event['kind'].isin(unscheduled_ous_types).astype(int)

    # aggregate maintenance by day and aircraft
    maint_daily = mant_event.groupby(['aircraftregistration', 'timeID']).agg({
        'scheduledOutOfService': 'sum',
        'unScheduledOutOfService': 'sum'
    }).reset_index().rename({'aircraftregistration': 'aircraftID'}, axis=1)

    logging.info("Maintenance KPIs for AircraftUtilization successfully processed.")
    return maint_daily

def enforce_aircraft_utilization_schema(aircraft_utilization):
    """
    Rounding, clipping, and type conversion to match target SQL.
    """
    logging.info("Enforcing AircraftUtilization data to match target SQL schema constraints...")

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
        logging.warning(f"Clipping {count} records in delayedMinutes > 999 to fit NUMBER(3).")

    aircraft_utilization['delayedMinutes'] = aircraft_utilization['delayedMinutes'].round(0).clip(upper=999).astype(int)
    
    # reorder columns to match target SQL
    target_cols = [
        'aircraftID', 'timeID', 
        'scheduledOutOfService', 'unScheduledOutOfService', 
        'flightHours', 'flightCycles', 
        'delays', 'delayedMinutes', 'cancellations'
    ]
    aircraft_utilization = aircraft_utilization[target_cols]

    logging.info("AircraftUtilization data enforced to match target SQL schema constraints.")
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
    
    # 4: check that ids (FKs) in fact table exist in dimension tables (referential integrity)
    valid_aircraft_ids = set(aircraft_dim['ID'])
    valid_time_ids = set(temp_dim['ID'])

    aircraft_utilization = validate_and_filter_integrity(
        aircraft_utilization, valid_aircraft_ids, 'aircraftID', 'AircraftUtilization'
    )
    aircraft_utilization = validate_and_filter_integrity(
        aircraft_utilization, valid_time_ids, 'timeID', 'AircraftUtilization'
    )

    # 5: final formatting
    aircraft_utilization = enforce_aircraft_utilization_schema(aircraft_utilization)

    # 6: load into staging area
    output_path = f'{TRANSFORMED_STAGING_DIR}/{fact_name}.parquet'
    aircraft_utilization.to_parquet(output_path, index=False)

    logging.info(f"AircraftUtilization fact table successfully created.")


# ==========================================
# LOGBOOK REPORTING LOGIC
# ==========================================

def process_logbook_kpi(logbook):
    """
    Aggregation logic for logbook.
    """
    logging.info("Processing LogbookReporting KPI...")

    # ensure date and convert it to month
    logbook['reportingdate'] = pd.to_datetime(logbook['reportingdate']).dt.strftime("%Y-%m")

    # aggregation (count entries for each (aircraft, month, person))
    logbook['counter'] = 1
    logbook_reporting = logbook.groupby(['aircraftregistration', 'reportingdate', 'reporteurid']).agg({
        'counter': 'sum'
    }).reset_index()

    # rename columns to match target SQL schema
    logbook_reporting = logbook_reporting.rename({
        'aircraftregistration': 'aircraftID',
        'reportingdate': 'monthID',
        'reporteurid': 'personID'
    }, axis=1)

    logging.info("LogbookReporting KPI successfully processed.")
    return logbook_reporting

def enforce_logbook_reporting_schema(logbook_reporting):
    """
    Rounding, clipping, and type conversion to match target SQL.
    """
    logging.info("Enforcing LogbookReporting data to match target SQL schema constraints...")

    if (logbook_reporting['counter'] > 99).any():
        count = (logbook_reporting['counter'] > 99).sum()
        logging.warning(f"Clipping {count} records in counter > 99 to fit NUMBER(2).")

    logbook_reporting['counter'] = logbook_reporting['counter'].round(0).clip(upper=99).astype(int)

    logging.info("LogbookReporting data enforced to match target SQL schema constraints.")
    return logbook_reporting

def create_logbook_reporting_fact(fact_name, logbook, aircraft_dim, months_dim, people_dim):
    logging.info("Creating LogbookReporting fact table...")

    # 1: process KPI (counter): each logbook entry
    logbook_reporting = process_logbook_kpi(logbook)

    # 2: check that ids (FKs) in fact table exist in dimension tables (referential integrity)
    valid_aircraft = set(aircraft_dim['ID'])
    valid_months = set(months_dim['ID'])
    valid_people = set(people_dim['ID'])

    logbook_reporting = validate_and_filter_integrity(logbook_reporting, valid_aircraft, 'aircraftID', 'LogBook')
    logbook_reporting = validate_and_filter_integrity(logbook_reporting, valid_months, 'monthID', 'LogBook')
    logbook_reporting = validate_and_filter_integrity(logbook_reporting, valid_people, 'personID', 'LogBook')
    
    # 3: enforce SQL schema constraints: counter NUMBER(2) column
    logbook_reporting = enforce_logbook_reporting_schema(logbook_reporting)

    # 4: load into staging area
    output_path = f'{TRANSFORMED_STAGING_DIR}/{fact_name}.parquet'
    logbook_reporting.to_parquet(output_path, index=False)

    logging.info("LogbookReporting fact table successfully created.")