# Aims and objectives

## Ingesting data

* Ingest (addrecs) from SPSS file (done)
	* From separate data frame and list (CSV/json files)
	* Add single variable to to existing table (addvars)
* Updates scripts?
* Ingesting directly from SIR

## Checks and testing

* On ingest - check number of rows, and a mean for each variable
	* Also do the same for a random 10% sample (to check for rows switching)

## Retrievals

At the moment, we build queries using R programs that include using a data frame for a list of the tables and variables to retrieve. From there, it will be in memory - so you can do whatever you like with it, as part of the program you are writing.

[handy SO post](https://stackoverflow.com/questions/39169494/human-readable-hard-coding-dataframe-in-r)

