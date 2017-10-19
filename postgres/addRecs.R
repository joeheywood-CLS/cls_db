library(haven)
library(rjson)
library(readr)
library(RPostgreSQL)
library(DBI)

source("/home/db/cls_db/check.R")
source("/home/db/cls_db/cls_classS3.R")

connect <- function(){
	readRenviron("/home/db/.Renviron")
	drb <- dbDriver("PostgreSQL")
	dbConnect(drb, dbname = "pmcs", user = Sys.getenv("api_user"), 
			  password = Sys.getenv("api_password"))
}

## Main add rec file using either a sav file or csv and separate metadata
## args: d - data file (if sav) or a data frame, m: metadata list
addRec <- function(d, m = NULL, dbHome =  "/home/db/mcs/") {
	prepHome <- file.path(dbHome, "dbprep_pg")
	# look either for data in memory - or sav files
	if(is.character(d) & grepl(".sav$", d)) { 
		prep <- addRecPrepFromSav(d)
	} else { 
		prep <- addRecPrep(tblNm, d, m)
	}
	buildFl <- prep$fls$bld
	cmd <- sprintf("psql -d pmcs -f %s", buildFl)
	res <- system(cmd, intern = TRUE)
	conn <- connect()
	newTab <- dbGetQuery(conn, paste0("SELECT * FROM ", prep$tblNm))
	rawTab <- getDataFromSav(d)
	chk <- checkAvgs(rawTab, newTab)
	dbDisconnect(conn)
	out <- list(table = prep$tblNm, check = chk, cmd = cmd, res = res, prep=prep)
	tmStmp <- format(Sys.time(), "_%Y%m%dt%H%M")
	outFl <- file.path(dbHome, "dblog", paste0(prep$tblNm, tmStmp, ".Rda"))
	save(out, file = outFl)
	out
}
##############################################################################

## prepare data for ingestion to database
## args: tblNm (character) name of table, data (dataframe) data to be added
## meta (list) metadata to be added, prepHome (filepath)
addRecPrep <- function(tblNm, data, meta, prepHome =  "/home/db/mcs/dbprep") {
	fls <- createPrepDir(prepHome, tblNm)
	sql <- paste(readLines("/home/db/cls_db/insert_template_pg.sql"),
				 collapse = " \n")
	varDefs <- vapply(colnames(data), function(x) {
						  sprintf("%s %s", x, getDBType(data[[x]]))
				 }, "")
	write_delim(data, fls$datFl, delim = "|", na = "", col_names = FALSE)
	write.table(meta, fls$metFl, quote = FALSE, qmethod = "escape", sep = "|", 
				na = "{}", col.names = FALSE, row.names = FALSE)
	sql_ins <- sprintf(sql, tblNm, paste(varDefs, collapse = ",\n"), 
					   fls$datFl, fls$metFl)
	writeLines(sql_ins, fls$bldNm)
	out <- list(varDefs = varDefs, tblNm = tblNm, fls = fls, sql = sql_ins)
	structure(out, class = "addrec")
}


createPrepDir <- function(prepHome, tblNm) {
	prepHome = file.path(prepHome, tblNm)
	if(!dir.exists(prepHome))dir.create(prepHome)
	list(home = prepHome,
		 datFl = file.path(prepHome, paste0(tblNm, ".dat")),
		 bldNm = file.path(prepHome, paste0(tblNm, "_build.sql")),
		 metFl = file.path(prepHome, paste0(tblNm, "_meta.dat")))
}

addRecPrepFromSav <- function(f, prepHome = "/home/db/mcs/dbprep_pg/") {
	st <- Sys.time()
	tblNm <- gsub(".sav", "", basename(f))
	df <- getDataFromSav(f)
	print(paste0("Read sav file in ", 
				 round(difftime(Sys.time(), st, unit = "secs")), "seconds"))
	meta <- getMetaFromSav(df, tblNm)
	addRecPrep(tblNm, df, meta, prepHome = prepHome)
}

getMetaFromSav <- function(x, d) {
    xx <- lapply(x,attributes )
	for(xo in 1:ncol(x)) {
		xx[[xo]]$vnm <- colnames(x)[xo]
	}
    yy <- vapply(xx, rjson::toJSON, "")
	zz <- vapply(xx, function(xo) {
					 rjson::toJSON(savToCls(xo))
					  }, "") 
    data.frame(clstable = d, savmeta = yy, vrb = colnames(x), clsmeta = zz,
               stringsAsFactors = FALSE)
}

getDataFromSav <- function(f) {
	df <- read_sav(f, user_na = TRUE)
	for(i in colnames(df)) { 
		df[[i]][is.nan(df[[i]])] <- -42 
		if("hms" %in% class(df[[i]])) df[[i]] <- as.character(df[[i]])
	}
	df
}

getDBType <- function(x) {
	if(all(is.na(x))) {
        return("TINYINT")
   }
	if(is.numeric(x)) {
		x <- x[which(!is.na(x))]
		if(all(floor(x) == x)) {
			if(all(x %in% -32767:32767)) {
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

resetDB <- function() {
	conn <- connect()
	f <- dbListTables(conn)
	for(tbl in f[-1]) {
		dbSendQuery(conn, paste0("DROP TABLE ", tbl, ";"))
	}
	dbDisconnect(conn)
	system("psql -d pmcs -i /home/db/cls_db/jsonSchm.sql", intern = TRUE)
}
