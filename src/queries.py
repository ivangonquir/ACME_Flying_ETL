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
    #"maintenance": """ """, # not used for dw
    "slots": """ 
        SELECT 
            aircraftregistration, 
            scheduleddeparture,
            scheduledarrival
        FROM slots
    """
}

AMOS_EXTRACTION = {
    "attachments": """ 
        SELECT file::text AS file
        FROM attachments
    """,
    "forecastedorders": """ 
        SELECT workorderid
        FROM forecastedorders
    """,
    "maintenanceevents": """
        SELECT 
            maintenanceid, 
            aircraftregistration, 
            airport,
            starttime, 
            duration, 
            kind
        FROM maintenanceevents
    """,
    "operationinterruption": """ 
        SELECT 
            maintenanceid, 
            flightid, 
            departure, 
            delaycode
        FROM operationinterruption
    """,
    "technicallogbookorders": """ 
        SELECT 
            workorderid, 
            aircraftregistration, 
            executiondate,            
            reporteurclass,
            reporteurid,
            mel,
            reportingdate
        FROM technicallogbookorders
    """,
    "workorders": """ 
        SELECT workorderid
        FROM workorders
    """,
    "workpackages": """ 
        SELECT workpackageid
        FROM workpackages
    """
}

CSV_EXTRACTION = {
    "aircraft_lookup": {
        "source": "aircraft-manufaturerinfo-lookup.csv",
        "sep": ",",
        "cols": ["aircraft_reg_code", "aircraft_model", "manufacturer"]
    },
    "personnel-lookup": {
        "source": "maintenance-personnel-airport-lookup.csv",
        "sep": ";",
        "cols": ["reporteurid", "airport"]
    }
}