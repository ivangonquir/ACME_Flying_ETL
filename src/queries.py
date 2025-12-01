AIMS_EXTRACTION = {
    "flights": """
        SELECT 
            aircraftregistration, 
            scheduleddeparture,
            scheduledarrival,
            flightid,
            departureairport,
            arrivalairport,
            actualdeparture, 
            actualarrival, 
            cancelled,
            delaycode
        FROM flights
    """,
    "slots": """ 
        SELECT 
            aircraftregistration, 
            scheduleddeparture,
            scheduledarrival
        FROM slots
    """
}

AMOS_EXTRACTION = {
    "maintenanceevents": """
        SELECT 
            maintenanceid, 
            aircraftregistration, 
            starttime,
            duration,
            kind
        FROM maintenanceevents
    """,
    "technicallogbookorders": """ 
        SELECT 
            workorderid, 
            aircraftregistration,          
            reporteurclass,
            reporteurid,
            reportingdate
        FROM technicallogbookorders
    """
}

CSV_EXTRACTION = {
    "aircraft_lookup": {
        "source": "aircraft-manufaturerinfo-lookup.csv",
        "sep": ",",
        "cols": ["aircraft_reg_code", "aircraft_model", "manufacturer"]
    },
    "personnel_lookup": {
        "source": "maintenance-personnel-airport-lookup.csv",
        "sep": ";",
        "cols": ["reporteurid", "airport"]
    }
}