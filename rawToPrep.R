## rawToPrep.R
## By Joe, May 2017
## Takes .sav files as input from agency and outputs .dat, .sql
## and .json files ready to be added to the database.

## To begin with, using MonetDB, so .dat files use "|" as delimiter

 
###############################################################################

getDataFromSav <- function(flnm) {
	dat <- read_sav(flnm, user_na = TRUE)
	for(cln in colnames(dat)) {
		dat[[cln]][which(is.na(dat[[cln]]))] <- -42
		dat[[cln]] <- savToCls(dat[[cln]], cln)
	}
	dat
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
		att <- attributes(itm)
        ot <- sprintf("%s %s", att$vName, getDBType(itm))
		ot
    }, "")
    end <- ");"
	flPth <- paste0(flPth, "/", tblNm, "_data.dat")
    sql_copy <- sprintf("COPY INTO %s FROM '%s';", tblNm, flPth)
	## Value: character vector of SQL command
    c(sql_drop, sql_create, paste(sql_joins, collapse = ", "), end, sql_copy)
}


createMetaSchema <- function(a, tblNm, drop = FALSE) {
	first <- ifelse(drop == TRUE, paste0("DROP TABLE m_", tblNm, ";"), "")
    start <- sprintf("CREATE TABLE m_%s (", tblNm)
	save(a, tblNm, drop, first, start, file = "debug.Rda")
    mid <- vapply(a, function(itm) {
        sprintf("%s text", itm$vName)
    }, "")
    end <- ");"
    insSt <- sprintf("INSERT INTO m_%s VALUES (", tblNm)
    insts <- lapply(a, function(itm) {
        jstr <- jsonlite::toJSON(a) 
		jstr <- gsub("'", " ", jstr)
		sprintf("'%s'", jstr)
    })
    c(first, start, paste(mid, collapse = ", \n"), end,
      insSt, paste0(insts, collapse = ", \n"), end)
}
