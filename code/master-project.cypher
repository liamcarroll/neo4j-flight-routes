
\\\Loading in Nodes
----------------
\\\Countries
\\\Setting constraints to make indexes

CREATE CONSTRAINT ON (c:Country) ASSERT c.Country_ID IS UNIQUE;

\\\Importing countries data

LOAD CSV WITH HEADERS FROM 'file:///nodes_countries.csv' AS row
MERGE (c:Country {Country_ID: toInteger(row.Country_ID), 
                  Name: row.Name})
RETURN count(c)


\\Airports
\\\Constraint

CREATE CONSTRAINT ON (a:Airport) ASSERT a.Airport_ID IS UNIQUE;

\\\Importing Airports

LOAD CSV WITH HEADERS FROM 'file:///nodes_airports.csv' AS row
MERGE (a:Airport {Airport_ID: toInteger(row.Airport_ID), 
                  Name: row.Name,
                  City: row.City,
                  IATA_code: row.IATA_code,
                  Latitude: toFloat(row.Lat),
                  Longitude: toFloat(row.Long)})
RETURN count(a)

\\\Airlines
\\\Constraint
CREATE CONSTRAINT ON (al:Airline) ASSERT al.Airline_ID IS UNIQUE;

\\\Importing Airlines

LOAD CSV WITH HEADERS FROM 'file:///nodes_airlines.csv' AS row
MERGE (al:Airline {Airline_ID: toInteger(row.Airline_ID), 
                   Name: row.Name,
                   IATA_code: row.IATA_code})
RETURN count(al)

\\\Routes
\\\Constraint
CREATE CONSTRAINT ON (r:Route) ASSERT r.Route_ID IS UNIQUE;

\\\Importing Routes

LOAD CSV WITH HEADERS FROM 'file:///nodes_routes.csv' AS row
MERGE (r:Route {Route_ID: toInteger(row.Route_ID), 
                Airline_ID: toInteger(row.Airline_ID),
                Airline_IATA: row.Airline_IATA,
                Source_Airport_IATA: row.Source_Airport,
                Dest_Airport_IATA: row.Dest_Airport})
RETURN count(r)


Loading in relationships
------------------------
\\\Airlines based in countries

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///rel_airline_basedin_country.csv' AS row
WITH toInteger(row.Airline_ID) AS airlineId, row.Country as country
MATCH (al:Airline {Airline_ID: airlineId})
MATCH (c:Country {Name: country})
MERGE (al)-[rel:BASED_IN]->(c)
RETURN count(rel)

\\\Airline operates route

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///rel_airline_operates_route.csv' AS row
WITH toInteger(row.Route_ID) AS routeId, toInteger(row.Airline_ID) as airlineId
MATCH (r:Route {Route_ID: routeId})
MATCH (al:Airline {Airline_ID: airlineId})
MERGE (al)-[rel:OPERATES]->(r)
RETURN count(rel)

\\\Airport is destination of route

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///rel_airport_destof_flight.csv' AS row
WITH toInteger(row.Route_ID) AS routeId, row.Dest_Airport as dest
MATCH (r:Route {Route_ID: routeId})
MATCH (ap:Airport {IATA_code: dest})
MERGE (r)-[rel:DESTINATION]->(ap)
RETURN count(rel)

\\\Airport is the source of route

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///rel_airport_sourceof_flight.csv' AS row
WITH toInteger(row.Route_ID) AS routeId, row.Source_Airport as source
MATCH (r:Route {Route_ID: routeId})
MATCH (ap:Airport {IATA_code: source})
MERGE (ap)-[rel:SOURCE_OF]->(r)
RETURN count(rel)

\\\Airport is in country

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM 'file:///rel_airport_isin_country.csv' AS row
WITH toInteger(row.Airport_ID) AS airportId, row.Country as country
MATCH (c:Country {Name: country})
MATCH (ap:Airport {Airport_ID: airportId})
MERGE (ap)-[rel:LOCATED_IN]->(c)
RETURN count(rel)