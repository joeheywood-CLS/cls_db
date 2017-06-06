library(haven)
library(jsonlite)
library(readr)

dd <- dir("/home/db/mcs/raw", full.names = TRUE, pattern = ".sav")

################# only two 'public' functions ######################

addRec <- function(f) {
	### wrapper around createSchema ###
	## Should also include tests/reports
	createSchema(f)
}

addAllRecs <- function(dr) {
	lapply(dir(dr, full.names = TRUE, pattern = ".sav"))
}

####################################################################

saveMeta <- function(x, d) {
    xx <- lapply(x, attributes )
    yy <- vapply(xx, toJSON, "")
    data.frame(clstable = d, savmeta = yy, vrb = colnames(x), clsmeta = "{}",
               stringsAsFactors = FALSE)
}

createSchema <- function(f, prepHome = "/home/db/mcs/dbprep") {
	st <- Sys.time()
	tblNm <- gsub(".sav", "", basename(f))
	print(paste0("########### Adding table ", tblNm, " ##############"))
	prepHome = file.path(prepHome, tblNm)
	datFl <- file.path(prepHome, paste0(tblNm, ".dat"))
	metFl <- file.path(prepHome, paste0(tblNm, "_meta.dat"))
	if(!dir.exists(prepHome))dir.create(prepHome)
	df <- read_sav(f, user_na = TRUE)
	print(paste0("Read sav file in ", round(difftime(Sys.time(), st, unit = "secs")), " seconds"))
	for(i in colnames(df)) { 
		df[[i]][is.nan(df[[i]])] <- -42 
		if("hms" %in% class(df[[i]])) df[[i]] <- as.character(df[[i]])
	}
	sql <- paste(readLines("/home/db/cls_db/insert_template.sql"),
				 collapse = " \n")
	varDefs <- vapply(colnames(df), function(x) {
						  sprintf("%s %s", x, getDBType(df[[x]]))
				 }, "")
	write_delim(df, datFl, delim = "|", na = "", col_names = FALSE)
	mt <- saveMeta(df, tblNm)
	write_delim(mt, metFl, delim = "|", na = "{}", col_names = FALSE)
	sql_ins <- sprintf(sql, tblNm, paste(varDefs, collapse = ",\n"), datFl, metFl)
	print(paste0("Finished in ", round(difftime(Sys.time(), st, unit = "secs")), " seconds"))
	bldNm <- paste0(tblNm, "_build.sql")
	print(paste0("mclient -d mcs -i ", tblNm, " to add to db"))
	writeLines(sql_ins, file.path(prepHome, bldNm))
	dim(df)
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


