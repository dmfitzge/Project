USE flightsDB; 
DROP TABLE IF EXISTS flights; 
CREATE TABLE flights(id integer auto_increment primary key,
YEAR INTEGER,
MONTH INTEGER,
DAY INTEGER,
DAY_OF_WEEK INTEGER,
AIRLINE VARCHAR(256),
FLIGHT_NUMBER INTEGER,
TAIL_NUMBER VARCHAR(256),
ORIGIN_AIRPORT VARCHAR(256),
DESTINATION_AIRPORT VARCHAR(256),
SCHEDULED_DEPARTURE INTEGER,
DEPARTURE_TIME INTEGER,
DEPARTURE_DELAY INTEGER,
TAXI_OUT INTEGER,
WHEELS_OFF INTEGER,
SCHEDULED_TIME INTEGER,
ELAPSED_TIME INTEGER,
AIR_TIME INTEGER,
DISTANCE INTEGER,
WHEELS_ON INTEGER,
TAXI_IN INTEGER,
SCHEDULED_ARRIVAL INTEGER,
ARRIVAL_TIME INTEGER,
ARRIVAL_DELAY INTEGER,
DIVERTED INTEGER,
CANCELLED INTEGER,
CANCELLATION_REASON VARCHAR(256),
AIR_SYSTEM_DELAY INTEGER,
SECURITY_DELAY INTEGER,
AIRLINE_DELAY INTEGER,
LATE_AIRCRAFT_DELAY INTEGER,
WEATHER_DELAY INTEGER);


LOAD DATA LOCAL INFILE '/media/sf_SharedFolder/flights.csv' INTO TABLE flights  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES(YEAR,MONTH,DAY,DAY_OF_WEEK,AIRLINE,FLIGHT_NUMBER,TAIL_NUMBER,ORIGIN_AIRPORT,DESTINATION_AIRPORT,SCHEDULED_DEPARTURE,DEPARTURE_TIME,DEPARTURE_DELAY,TAXI_OUT,WHEELS_OFF,SCHEDULED_TIME,ELAPSED_TIME,AIR_TIME,DISTANCE,WHEELS_ON,TAXI_IN,SCHEDULED_ARRIVAL,ARRIVAL_TIME,ARRIVAL_DELAY,DIVERTED,CANCELLED,CANCELLATION_REASON,AIR_SYSTEM_DELAY,SECURITY_DELAY,AIRLINE_DELAY,LATE_AIRCRAFT_DELAY,WEATHER_DELAY) ; 








