## ADD VARS/UPDATES ##

## This file should be generally for updating columns within tables ##

## simplest way - add data to a temporary table and join to existing table ##

## other options could be to update row by row ##

str <- "
UPDATE mcs5_cm_derived SET tstcntry = 
	(SELECT ecctry00 FROM mcs5_cm_interview mi
	 	WHERE mi.mcsid = mcs5_cm_derived.mcsid AND 
			mi.ecnum00 = mcs5_cm_derived.ecnum00)
WHERE EXISTS
	(SELECT * FROM mcs5_cm_interview mi
	 	WHERE mi.mcsid = mcs5_cm_derived.mcsid AND 
			mi.ecnum00 = mcs5_cm_derived.ecnum00)
"
su <- "
SELECT ecctry00 FROM mcs5_cm_interview mi, mcs5_cm_derived md 
	 	WHERE mi.mcsid = md.mcsid  AND mi.ecnum00 = md.ecnum00"



addVarToTable <- function(vr, tbl, ids) {
	## vr should be an spss file with the correct IDs identified
	## tbl is an existing table in the database
	## id(s) are the identifier variables to match 
	## Steps:
	#   1. add temp table to database from spss file
	#   2. alter existing table to add new columns
    #   3. update existing table to add new variables from temp table
	#   4. drop temp table	

}
