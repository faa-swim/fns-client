# SWIM FNS JMS Reference Implementation (FnsClient)
## Overview

The System Wide Information Service (SWIM) Federal NOTAM System (FNS) Java Messaging Service (JMS) Reference Implementation (FnsClient) provides an example implementation on how to establish and maintain a local instance of the FNS NOTAM Database through the use of the FNS Initial Load (FIL) and SWIM FNS JMS services. FIL provides all active NOTAMS, via SFTP, that is required to initialize a NOTAM database and the SWIM JMS service provides, via JMS, NOTAM updates to keep the NOTAM database current. FIL also provides for re-initialization of a NOTAM database in the case of JMS service interruption.

![FnsClient Diagram](https://github.com/faa-swim/fns-client/blob/v1.0/FnsClient%20Diagram.png?raw=true)

## Contents

This repository includes the java source code for the FnsClient which consists of the following classes:

  - **FnsClient:** Main entry for the application. 
  - **FilClient:** Obtains the FIL file via SFTP.
  - **NotamDb:** Provides all methods to create, put, and query the NOTAM database; supports H2 and PostgreSQL.
  - **FnsJmsMessageWorker:** Implementation of a JMS Message Worker used to process FNS Messages received from the SWIM AIM FNS JMS service and load into the NOTAM Database.
  - **FnsMessage:** Provides methods to marshal and unmarshal AIXM NOTAMs into a workable java object.
  - **FnsRestApi:** Implementation of a basic REST API to query the NOTAM Database.

## Prerequisites

A SWIM subscription to the AIM FNS JMS service and credentials to access the AIM FIL service are required to run the FnsClient. These can be obtained via the SWIM Cloud Distribution Service (SCDS) by visiting [scds.faa.gov](https://scds.faa.gov), creating an account, and requesting a subscription for the AIM FNS service. Once the subscription has been approved you will receive an email with instructions on how to request FIL credentials.
  - Built using JDK 11 and Maven

## Building and Running

  1. Clone this repository including submodules
  	 - git clone --recurse-submodules https://github.com/faa-swim/fns-client
  2. Install submodule dependencies to your local maven repo
     - mvn clean install /aixm-5.1
     - mvn clean install /jms-client
     - mvn clean install /swim-utilities
  3. Run mvn clean package
  4. Change to the target directory; cd target/FnsClient
  5. Modify the fnsClient.conf file and add the SWIM AIM FNS JMS and FIL connection details
    - FIL Cert needs to be in RSA (aka pem) format; conversion can been done via: ssh-keygen -p -N "" -m pem -f /path/to/key’
  6. Run the FnsClient; java -jar FnsClient.jar

Once the FnsClient has started and initialized, NOTAMS can be queried directly from the NOTAM database, via calling the rest api, or by the web ui as localhost:8080

## Rest API

The FnsClient includes a basic REST API that can be used to query NOTAMS from the database. The API includes the following methods:

  - Query by Location Designator; e.g. ATL | wget http://localhost:8080/locationDesignator/{id}
  - Query by Classification; e.g. DOM | wget http://localhost:8080/classification/{classification}
  - Query by Delta Time | wget http://localhost:8080/delta/{YYYY-mm-DD HH:MM:SS}
  - Query by Time Range | wget http://localhost:8080/timerange/{YYYY-mm-DD HH:MM:SS}/{YYYY-mm-DD HH:MM:SS}
  - Query All NOTAMS | wget http://localhost:8080/allNotams

