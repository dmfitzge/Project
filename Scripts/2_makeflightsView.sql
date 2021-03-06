DROP TABLE IF EXISTS flightsView; 
CREATE TABLE flightsView(id integer auto_increment primary key,
DAY_OF_WEEK INTEGER,
AIRLINE VARCHAR(256),
ORIGIN_AIRPORT VARCHAR(256),
DEPARTURE_DELAY INTEGER,
AIR_TIME INTEGER,
DISTANCE INTEGER,
ARRIVAL_DELAY INTEGER,
CANCELLED INTEGER);


INSERT INTO flightsView(AIRLINE,ORIGIN_AIRPORT,DEPARTURE_DELAY,ARRIVAL_DELAY)
SELECT AIRLINE,ORIGIN_AIRPORT,DEPARTURE_DELAY,ARRIVAL_DELAY FROM flights;


