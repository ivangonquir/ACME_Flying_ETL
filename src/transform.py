import pandas as pd
from datetime import timedelta

def transform_aims_amos_data(aims_flights, amos_operations, aims_slots, aircraft_lookup, personnel_lookup):
    # 1. Join AIMS and AMOS Data
    # Join Flights with OperationInterruption on flightID
    merged_flights = pd.merge(aims_flights, amos_operations, on="flightid", how="left")

    # Join Slots with MaintenanceEvents on aircraftRegistration
    merged_flights = pd.merge(merged_flights, aims_slots, on="aircraftregistration", how="left")
    
    # 2. Lookup Aircraft Manufacturer Information
    merged_flights = pd.merge(merged_flights, aircraft_lookup, left_on="aircraftregistration", right_on="aircraft_reg_code", how="left")
    
    # 3. Lookup Maintenance Personnel and Airport Information
    merged_flights = pd.merge(merged_flights, personnel_lookup, left_on="reporteurid", right_on="reporteurid", how="left")

    # 4. Calculate Derived Metrics
    # Calculate Flight Hours (FH)
    merged_flights['flight_hours'] = (pd.to_datetime(merged_flights['actualarrival']) - pd.to_datetime(merged_flights['actualdeparture'])).dt.total_seconds() / 3600
    
    # Calculate Flight Cycles (TO) (only non-cancelled flights)
    merged_flights['flight_cycles'] = merged_flights.apply(lambda row: 1 if row['cancelled'] == 0 else 0, axis=1)
    
    # Calculate Delayed Minutes
    merged_flights['delayed_minutes'] = (pd.to_datetime(merged_flights['actualdeparture']) - pd.to_datetime(merged_flights['scheduleddeparture'])).dt.total_seconds() / 60
    
    # 5. Clean Data
    # Drop any rows with missing critical information
    merged_flights.dropna(subset=['flightid', 'aircraftregistration', 'actualdeparture', 'actualarrival'], inplace=True)
    
    # 6. Ensure data consistency (e.g., handle missing values, duplicates)
    merged_flights = merged_flights.drop_duplicates()
    
    return merged_flights


def load_csv_data(file_path, columns, sep = ','):
    return pd.read_csv(file_path, usecols=columns, sep = sep)

aims_flights = pd.read_parquet('data/raw_staging/flights.parquet')
amos_operations = pd.read_parquet('data/raw_staging/operationinterruption.parquet')
aims_slots = pd.read_parquet('data/raw_staging/slots.parquet')
aircraft_lookup = load_csv_data('data/input/aircraft-manufaturerinfo-lookup.csv', ['aircraft_reg_code', 'aircraft_model', 'manufacturer'], sep = ',')
personnel_lookup = load_csv_data('data/input/maintenance-personnel-airport-lookup.csv', ['reporteurid', 'airport'], sep = ';')

# Apply transformation
transformed_data = transform_aims_amos_data(aims_flights, amos_operations, aims_slots, aircraft_lookup, personnel_lookup)

# Saving the transformed data (You may save it as parquet or insert into the Data Warehouse)
transformed_data.to_parquet('data/clean_staging/transformed_flights.parquet', index=False)
