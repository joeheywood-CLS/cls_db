## rawToPrep.R
## By Joe, May 2017
## Takes .sav files as input from agency and outputs .dat, .sql
## and .json files ready to be added to the database.

## To begin with, using MonetDB, so .dat files use "|" as delimiter

library(haven)
library(jsonlite)
library(readr)

source("/home/joe/cls_db/cls_class_basic.R")
f <- "/home/joe/mcs/raw/mcs5_cm_derived.sav"

################ Only "public" function here should be prepareSav #############

prepareSAV <- function(f, nm, prepHome = "/home/joe/mcs/dbprep/", drop = FALSE) { 
	## args: f - sav file, nm - name of table (no spaces)
    # create directory
    prepHome <- paste0(prepHome, nm)
    dir.create(prepHome)
	## get data from file
    st <- getDataFromSav(f)
    ## create schema ##
    schm <- createSchemaFromCLS_df(st, nm, prepHome, drop)
    writeLines(schm, file.path(prepHome, paste0(nm, "_schema.sql")) )
    ## create meta table sql and insert json
    mtSch <- createMetaSchema(st, nm, drop)
    writeLines(mtSch, file.path(prepHome, paste0(nm, "_m_schema.sql")) )
	## write delimited file with | and no headers
    write_delim(st, file.path(prepHome, paste0(nm, "_data.dat")), delim = "|", 
				na="", col_names = FALSE )
}

###############################################################################

getDataFromSav <- function(flnm) {
	dat <-read_sav(flnm, user_na = TRUE)
	cls_df <- list() 
	for(cln in colnames(dat)) {
		dat[[cln]][which(is.na(dat[[cln]]))] <- -42
		cls_df[[cln]] <- savToCLS_v(cln, dat)
	}
	new("cls_df", as.data.frame(cls_df))
}

getMetaHaven <- function(flnm) {
    fl <- read_sav(flnm, user_na = TRUE)
	for(cln in colnames(fl)) {
		fl[[cln]][which(is.na(fl[[cln]]))] <- -42
	}
    list(dat = fl, meta = lapply(names(fl), populateM, fl = fl))
}

populateM <- function(nm, fl) {
    x <- fl[[nm]]
    list(id = nm,
         label = attr(x, "label"),
         formatSpss = attr(x, "format.spss"),
         dbType = getDBType(x),
         ctgry = elemsListM(x, nm)
 	)
}

getDBType <- function(x) {
	if(all(is.na(x))) {
        return("TINYINT")
    }
	if(is.numeric(x)) {
		x <- x[which(!is.na(x))]
		if(all(floor(x) == x)) {
			if(all(x %in% -127:127)) {
				return("TINYINT")
			} else if(all(x %in% -32767:32767)) {
				return("SMALLINT")
			} else{
				return("INT")
			}
		} else {
			return("DECIMAL")
		}
	} else {
		longest <- max(nchar(x))
		if(length(unique(nchar(x))) == 1) {
			return(sprintf("CHAR(%d)", longest))
		}
		if(longest < 200) {
			return(sprintf("VARCHAR(%d)", longest))
		} else {
			return("TEXT")
		}
	}
}


createSchemaFromCLS_df <- function(l, tblNm, flPth, drop = FALSE) {
	## Args: l - cls_df, tblNm - name of table, flPth - where .dat file is saved
	##       drop - whether to drop table 
	sql_drop <- ifelse(drop == TRUE, paste0("DROP TABLE ", tblNm, ";"), "")
    sql_create <- sprintf("CREATE TABLE %s (", tblNm)
    sql_joins <- vapply(l, function(itm) {
        sprintf("%s %s", itm@vName, getDBType(itm))
    }, "")
    end <- ");"
	flPth <- paste0(flPth, "/", tblNm, "_data.dat")
    sql_copy <- sprintf("COPY INTO %s FROM '%s';", tblNm, flPth)
	## Value: character vector of SQL command
    c(sql_drop, sql_create, paste(sql_joins, collapse = ", "), end, sql_copy)
}


createMetaSchema <- function(l, tblNm, drop = FALSE) {
	first <- ifelse(drop == TRUE, paste0("DROP TABLE m_", tblNm, ";"), "")
    start <- sprintf("CREATE TABLE m_%s (", tblNm)
    mid <- vapply(l, function(itm) {
        sprintf("%s text", itm@vName)
    }, "")
    end <- ");"
    insSt <- sprintf("INSERT INTO m_%s VALUES (", tblNm)
    insts <- lapply(l, function(itm) {
        jstr <- cls_toJSON(itm)
		jstr <- gsub("'", " ", jstr)
		sprintf("'%s'", jstr)
    })
    c(first, start, paste(mid, collapse = ", \n"), end,
      insSt, paste0(insts, collapse = ", \n"), end)
}
