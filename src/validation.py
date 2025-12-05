import pandas as pd
import logging
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def validate_identifiers(work_packages, work_orders, maintenance_events, attachments, flights):
    """
    Validates BR1, BR2, BR3, BR4, BR11 (Uniqueness of Primary Keys)
    """
    logging.info("--- Validating Identifiers (BR1-BR4, BR11) ---")
    
    checks = [
        (work_packages, 'workPackageID', 'BR1'),
        (work_orders, 'workOrderID', 'BR2'),
        (maintenance_events, 'maintenanceID', 'BR3'),
        (attachments, 'file', 'BR4'),
        (flights, 'flightID', 'BR11')
    ]

    for df, col, rule in checks:
        if col in df.columns:
            if not df[col].is_unique:
                duplicates = df[col].duplicated().sum()
                df.drop_duplicates(subset = [col], inplace=True)
                logging.error(f"[{rule}] VIOLATION: {col} is not unique. {duplicates} duplicates removed.")
            else:
                logging.info(f"[{rule}] Passed: {col} is unique.")
        else:
            logging.warning(f"[{rule}] Column {col} not found in dataframe.")

    
    return work_packages, work_orders, maintenance_events, attachments, flights

    

def validate_domains_and_nulls(logbook, maintenance_events):
    """
    Validates BR5, BR6, BR7 (Allowed values and Non-Nulls)
    """
    logging.info("--- Validating Domains & Nulls (BR5-BR7) ---")

    # BR5: ReportKind values
    if 'reporteurclass' in logbook.columns:
        valid_kinds = ['PIREP', 'MAREP']
        invalid_mask = ~logbook['reporteurclass'].isin(valid_kinds)
        if invalid_mask.any():
            logbook = logbook[~invalid_mask]
            logging.error(f"[BR5] VIOLATION: Found invalid ReportKind values: {logbook[invalid_mask]['reporteurclass'].unique()}")
        else:
            logging.info("[BR5] Passed: ReportKind values are valid.")

    # BR6: MELCategory
    # Adjust 'MEL_category' to the actual column name in your DF
    mel_col = 'mel' 
    if mel_col in maintenance_events.columns:
        valid_mel = ['A', 'B', 'C', 'D']
        # Filter out NaNs if they are allowed, otherwise remove .dropna()
        invalid_mel = maintenance_events[~maintenance_events[mel_col].isin(valid_mel) & maintenance_events[mel_col].notna()]
        if not invalid_mel.empty:
             maintenance_events = maintenance_events[~invalid_mel]
             logging.error(f"[BR6] VIOLATION: Invalid MEL Categories found: {invalid_mel[mel_col].unique()}")
        else:
            logging.info("[BR6] Passed: MEL Categories are valid.")



    # BR7: Airport in MaintenanceEvents must have a value
    if 'airport' in maintenance_events.columns:
        missing_airport = maintenance_events['airport'].isna().sum()
        if missing_airport > 0:
            maintenance_events.dropna(subset = ['airport'], inplace = True)
            logging.error(f"[BR7] VIOLATION: {missing_airport} MaintenanceEvents are missing 'airport'.")
        else:
            logging.info("[BR7] Passed: All MaintenanceEvents have airports.")
    return logbook, maintenance_events

def validate_maintenance_logic(op_interruption, maintenance_events, flights):
    """
    Validates BR8, BR9, BR10
    """
    logging.info("--- Validating Maintenance Logic (BR8-BR10) ---")

    # BR8: OpInterruption departure matches flightID date
    # flightID structure: Date(6)-Origin(3)... e.g., 230101-LHR...
    if 'flightID' in op_interruption.columns and 'departure' in op_interruption.columns:
        # Extract date string from flightID (first 6 chars)
        # Convert to datetime (assuming format YYMMDD)
        op_interruption['flightID_date'] = pd.to_datetime(op_interruption['flightID'].astype(str).str[:6], format='%d%m%y', errors='coerce').dt.date
        op_interruption['dep_date'] = pd.to_datetime(op_interruption['departure']).dt.date

        mismatches = op_interruption['flightID_date'] != op_interruption['dep_date']
        if not mismatches.empty:
             op_interruption[mismatches]['departure'] = op_interruption[mismatches]['flightID_date']
             logging.error(f"[BR8] VIOLATION: {sum(mismatches)} departure records were replaced by the flightID date.")
        else:
            logging.info("[BR8] Passed: OpInterruption dates match flightID.")

    # BR9: Flight in OpInterruption must exist in Flights and be Delayed
    if 'flightID' in op_interruption.columns:
        # Merge to check existence and status
        merged = pd.merge(op_interruption[['flightID']], 
                          flights[['flightID', 'delayCode']], 
                          on='flightID', 
                          how='left', 
                          indicator=True)
        
        # Check 1: Must exist in Flights
        missing_flights = merged[merged['_merge'] == 'left_only'] # This means that only appears in op_interruption
        wrong_flights = op_interruption['flightID'] == merged['flightID']
        op_interruption = op_interruption[~wrong_flights]

        if not missing_flights.empty:
            logging.error(f"[BR9] VIOLATION: {len(missing_flights)} OpInterruptions refer to non-existent flights.")
        
        # Check 2: Must have delayCode
        # Look at records that exist (both) but have null delayCode
        existing = merged[merged['_merge'] == 'both']
        not_delayed = existing[existing['delayCode'].isna()]
        if not_delayed.empty:
            op_interruption.dropna(subset=['delayCode'], inplace=True)
            logging.info("[BR9] Passed: All interrupted flights exist and are delayed.")
        else:
            logging.error(f"[BR9] VIOLATION: {len(not_delayed)} interrupted flights exist but are NOT marked as delayed.")
    
  

    # BR10: Maintenance Duration Logic
    # Requires 'kind' and 'duration' columns. Duration assumed to be Timedelta.
    if 'kind' in maintenance_events.columns and 'duration' in maintenance_events.columns:
        # Define logic (Simplified for demonstration)
        # Delay -> Minutes (e.g., < 2 hours)
        # Maintenance -> Hours to max 1 day
        # Revision -> Days to 1 month
        
        # Check for "Maintenance" type
        inv_maints = maintenance_events['kind'] == 'Maintenance' & maintenance_events['duration'] > pd.Timedelta(days = 1)
        
        if not inv_maints.empty:
            maintenance_events = maintenance_events[~inv_maints]
            logging.warning(f"[BR10] WARNING: {len(inv_maints)} 'Maintenance' events lasted longer than 1 day.")
        else:
            logging.info("[BR10] Passed (partial): Maintenance duration looks reasonable.")

         # Check for "Delay" type
        inv_delays = maintenance_events['kind'] == 'Delay' & maintenance_events['duration'] > pd.Timedelta(hours = 1)
        
        if not inv_delays.empty:
            maintenance_events = maintenance_events[~inv_delays]
            logging.warning(f"[BR10] WARNING: {len(inv_delays)} 'Delay' events lasted longer than 1 hour.")
        else:
            logging.info("[BR10] Passed (partial): Delay duration looks reasonable.")


         # Check for "Safety" type
        """
         TO-DO: How to check for Safety type
        safety_evs = maintenance_events[maintenance_events['kind'] == 'Safety']
        long_safety = safety_evs[safety_evs['duration'] < pd.Timedelta(hours=1)]
        
        if not long_safety.empty:
            logging.warning(f"[BR10] WARNING: {len(long_safety)} 'Safety' events lasted longer than 1 hour.")
        else:
            logging.info("[BR10] Passed (partial): Safety duration looks reasonable.")
        """

        # Check for "AircraftOnGround" type
        inv_grounds = maintenance_events['kind'] == 'AircraftOnGround' & (maintenance_events['duration'] > pd.Timedelta(hours = 24) or maintenance_events['duration'] < pd.Timedelta(hours = 1)) 

        if not inv_grounds.empty:
            maintenance_events = maintenance_events[~inv_grounds]
            logging.warning(f"[BR10] WARNING: {len(inv_grounds)} 'Air' events lasted longer than 24 hours.")
        else:
            logging.info("[BR10] Passed (partial): Air duration looks reasonable.")

        # Check for "Revision" type
        inv_revisions = maintenance_events['kind'] == 'Revision' & (maintenance_events['duration'] < pd.Timedelta(days = 1) or maintenance_events['duration'] > pd.Timedelta(months = 1))
        
        if not inv_revisions.empty:
            maintenance_events = maintenance_events[~inv_revisions]
            logging.warning(f"[BR10] WARNING: {len(inv_revisions)} 'Revision' events lasted longer than 24 hours.")
        else:
            logging.info("[BR10] Passed (partial): Revision duration looks reasonable.")

    return op_interruption, maintenance_events, flights


def validate_flight_logic(flights):
    """
    Validates BR12, BR13, BR14, BR16, BR17, BR18
    """
    logging.info("--- Validating Flight Logic (BR12-BR18) ---")
    
    # BR12: flightID format validation
    # Regex: 6 digits (date), 3 chars (orig), 3 chars (dest), 4 digits (num), 6 chars (reg)
    # Note: Regex adjusted based on common IATA/ICAO lengths provided in description
    # Pattern: Date(6)-Origin(3)-Dest(3)-FlightNum(4)-Reg(6)
    #pattern = re.compile(r'^\d{6}-[A-Z]{3}-[A-Z]{3}-\d{4}-[A-Z0-9]{6}$') (this was the one proposed by Claude)
    pattern = re.compile(r'^\d{6}-[A-Z]{3}-[A-Z]{3}-\d{4}-[A-Z]{2}-[A-Z]{3}$')
    
    invalid_ids = ~flights['flightID'].astype(str).str.match(pattern)
    if not invalid_ids.empty:
        flights = flights[~invalid_ids]
        logging.error(f"[BR12] VIOLATION: {len(invalid_ids)} flightIDs do not match the required format.")
    else:
        logging.info("[BR12] Passed: flightID format valid.")

    # BR13 & BR18: Arrival > Departure (Scheduled and Actual)  # To check: should we work with flights or slots??????
    # Scheduled
    if 'scheduledarrival' in flights.columns and 'scheduleddeparture' in flights.columns:
        bad_sched = flights['scheduledarrival'] <= flights['scheduleddeparture']
        if not bad_sched.empty:
            flights = flights[~bad_sched]
            logging.error(f"[BR13] VIOLATION: {len(bad_sched)} flights arrive before they depart (Scheduled).")
        else:
            logging.info("[BR13] Passed: Scheduled Arrival > Departure.")
            
    # Actual
    if 'actualarrival' in flights.columns and 'actualdeparture' in flights.columns:
        # Filter out NaNs (cancelled flights might not have actuals)
        flights.dropna(subset=['actualarrival', 'actualdeparture'], inplace = True)
        bad_act = flights['actualarrival'] <= flights['actualdeparture']
        if not bad_act.empty:
            flights = flights[~bad_act]
            logging.error(f"[BR18] VIOLATION: {len(bad_act)} flights arrive before they depart (Actual).")
        else:
            logging.info("[BR18] Passed: Actual Arrival > Departure.")

    # BR14: Flight duration < 24 hours
    # Assuming duration is calculated or diff between arr and dep
    if 'scheduledarrival' in flights.columns:
        flights['calc_duration'] = flights['scheduledarrival'] - flights['scheduleddeparture']
        long_flights = flights['calc_duration'] > pd.Timedelta(hours=24)
        if not long_flights.empty:
             flights = flights[~long_flights]
             logging.error(f"[BR14] VIOLATION: {len(long_flights)} flights differ by more than 24 hours.")
        else:
             logging.info("[BR14] Passed: No flights exceed 24 hours.")

    # BR16: No overlapping slots for same aircraft
    # We sort by aircraft and departure time
    if 'aircraftregistration' in flights.columns:
        # Check this: should we sort by aircraft??
        df_sorted = flights.sort_values(by=['aircraftregistration', 'scheduleddeparture'])
        
        # Shift creates a new column with the *previous* row's arrival time. QUESTION: Maybe we don't have to group by??????
        df_sorted['prev_arrival'] = df_sorted.groupby('aircraftregistration')['scheduledarrival'].shift(1) # Each row of this new column has the value of the previous row in scheduledarrival
        
        # Overlap exists if Current Departure < Previous Arrival
        overlaps = df_sorted[df_sorted['scheduleddeparture'] < df_sorted['prev_arrival']]
        idx_overlaps = flights['scheduleddeparture'] == overlaps['scheduleddeparture'] & flights['aircraftregistration'] == overlaps['aircraftregistration']
        
        if not overlaps.empty:
            flights = flights[~idx_overlaps]
            logging.error(f"[BR16] VIOLATION: {len(overlaps)} overlapping flight slots found for the same aircraft.")
            # Optional: print sample
            # print(overlaps[['flightID', 'aircraftregistration', 'scheduleddeparture', 'prev_arrival']].head())
        else:
            logging.info("[BR16] Passed: No overlapping slots.")

    # BR17: Origin/Dest match flightID
    # flightID structure: Date-Origin-Dest-...
    if 'departureairport' in flights.columns and 'arrivalairport' in flights.columns:
        # Extract from ID
        # Split string by '-'
        # ID parts: [0]Date, [1]Origin, [2]Dest, ...
        id_parts = flights['flightID'].str.split('-', expand=True)
        if id_parts.shape[1] >= 3:
            flights['id_origin'] = id_parts[1]
            flights['id_dest'] = id_parts[2]
            
            # Check Origin
            bad_orig = flights[flights['departureairport'] != flights['id_origin']]
            # Check Destination (Allowing for diversion? Rule says "unless diverted". 
            # If you have a 'diverted' flag, add: & (flights['diverted'] == False))
            bad_dest = flights[flights['arrivalairport'] != flights['id_dest']]
            
            if not bad_orig.empty or not bad_dest.empty:
                flights = flights[~bad_orig]
                flights = flights[~bad_dest]
                logging.warning(f"[BR17] WARNING: {len(bad_orig) + len(bad_dest)} flights have airports mismatching their ID (Check for diversions).")
            else:
                logging.info("[BR17] Passed: Airports match flightID.")
    return flights

# --- Wrapper Function to Run All ---
def run_all_validations(dfs_dict):
    """
    Expects a dictionary of dataframes:
    {
        'flights': df,
        'logbook': df,
        'maintenance': df,
        'op_interruption': df,
        'work_pkg': df,
        ...
    }
    """
    work_packages=dfs_dict.get('work_pkg', pd.DataFrame())
    work_orders=dfs_dict.get('work_orders', pd.DataFrame())
    maintenance_events=dfs_dict.get('maintenance', pd.DataFrame())
    attachments=dfs_dict.get('attachments', pd.DataFrame())
    flights=dfs_dict.get('flights', pd.DataFrame())
    logbook=dfs_dict.get('logbook', pd.DataFrame())
    op_interruption=dfs_dict.get('op_interruption', pd.DataFrame())
    logging.info("STARTING DATA QUALITY VALIDATION")
    
    work_packages, work_orders, maintenance_events, attachments, flights = validate_identifiers(
       work_packages, work_orders, maintenance_events, attachments, flights
    )
    
    logbook, maintenance_events = validate_domains_and_nulls(logbook, maintenance_events)
    
    op_interruption, maintenance_events, flights = validate_maintenance_logic(op_interruption, maintenance_events, flights)
    
    flights = validate_flight_logic(flights)
    
    logging.info("VALIDATION COMPLETE")

