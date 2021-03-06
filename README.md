elasticsearch Geotools Datastore
================================

First stab on elasticsearch GeoTools datastore.

Based on the tutorial for a CSV DataStore in the GeoTools guide.

Installation in Geoserver 2.1.1
-------------------------------

There is a problem with the different logging implementations in ES and GeoServer (the binary distribution)
As a workaround I upgraded to SLF4J 1.6.1 in both {GEOSERVER_HOME}/lib and in {GEOSERVER_HOME}/webapps/geoserver/WEB-INF/lib.

{GEOSERVER_HOME}/lib

Remove

-   log4j-1.2.14.jar
-   slf4j-simple-1.0.1.jar

Add

-   log4j-1.2.16.jar
-   slf4j-api-1.6.1.jar
-   slf4j-simple-1.6.1.jar

{GEOSERVER_HOME}/webapps/geoserver/WEB-INF/lib

Remove

-   log4j-1.2.14.jar
-   slf4j-api-1.5.8.jar
-   slf4j-log4j12-1.4.2.jar

Add

-   log4j-1.2.16.jar
-   slf4j-api-1.6.1.jar
-   slf4j-log4j12-1.6.1.jar

Dependencies
------------

mvn dependency:copy-dependencies

cd target/dependency

cp lucene-* {GEOSERVER_HOME}/webapps/geoserver/WEB-INF/lib/

cp elasticsearch-0.17.4.jar {GEOSERVER_HOME}/webapps/geoserver/WEB-INF/lib/



