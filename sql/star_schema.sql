-- Dimension Tables
CREATE TABLE AircraftDimension (
    aircraftID SERIAL PRIMARY KEY,
    model VARCHAR(100),
    manufacturer VARCHAR(100),
);

CREATE TABLE PeopleDimension (
    personID SERIAL PRIMARY KEY,
    role VARCHAR(50),
    airport VARCHAR(100)
);

CREATE TABLE TemporalDimension (
    timeID SERIAL PRIMARY KEY,
    monthID INT REFERENCES Months(monthID)
);

CREATE TABLE Months (
    monthID INT PRIMARY KEY,
    year INT
)

-- Fact Tables
CREATE TABLE AicraftUtilization (
    timeID INT REFERENCES TemporalDimension(timeID),
    aircraftID INT REFERENCES  AircraftDimension(aircraftID),
    FlightHours NUMERIC,
    FlightCycles INT,
    ScheduledOutOfService INT,
    UnscheduledOutOfService INT,
    Delays INT,
    Cancellations INT,
    DelayedMinutes INT,
    PRIMARY KEY (timeID, aircraftID)
);

CREATE TABLE LogbookReporting (
    monthID INT REFERENCES Months(monthID),
    personID INT REFERENCES PeopleDimension(personID),
    aircraftID INT REFERENCES AircraftDimension(aircraftID),
    counter INT,
    PRIMARY KEY (monthID, personID, aircraftID)
);
