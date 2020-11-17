# Europeana Mongo exporter

The aim of this reporitory is to export every record 
from a MongoDB into files as each line contain
a single full record serialized as JSON.
The application first reads all the identifiers from
a database then via an external web service it retrieves
and stores the records into files. This second step
runs paralell in multiple threads in order to speed 
up the process. When expoorted the number of 
records are counted and the files are gzipped and
archived.

This tool has been created for exporting from Europeana, 
but with some modification it can be used for other
databases. The specifics of Europeana's database
* contains 60 million records
* the total size of exported file are 1.2 TB
* the collection contains a main database (records),
and a number of connected databases

Traditional tools are either too slow, or can 
export properly the connected documents, that's why
this tool was developed.

## Usage

 0. `mvn clean install`
 1. `cp set-variables.sh.template set-variables.sh`
 2. replace <placeholders> to real values inside set-variables.sh
 3. `nohup ./run-all.sh [version] > logs/run-all-[version].log &` (where `version` is something like `v2020-09`)
