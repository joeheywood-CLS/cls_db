library(haven)
library(rjson)
library(readr)
library(MonetDB.R)
library(DBI)

source("/home/db/cls_db/check.R")
source("/home/db/cls_db/cls_classS3.R")

addRec <- function(d, m = NULL, dbHome =  "/home/db/mcs/") {
	prepHome <- file.path(dbHome, "prep")
	# look either for data in memory - or sav files
	if(is.character(d) & grepl(".sav$", d)) { 
		prep <- addRecPrepFromSav(d)
	} else { 
		prep <- addRecPrep(tblNm, d, m)
	}
	buildFl <- prep$fls$bld
	cmd <- sprintf("mclient -d mcs -i %s", buildFl)
	res <- system(cmd, intern = TRUE)
	conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs")  # 
	newTab <- dbGetQuery(conn, paste0("SELECT * FROM ", prep$tblNm))
	rawTab <- getDataFromSav(d)
	chk <- checkAvgs(rawTab, newTab)
	dbDisconnect(conn)
	out <- list(table = tbl, check = chk, cmd = cmd, res = res)
	tmStmp <- format(Sys.time(), "_%Y%m%dt%H%M")
	outFl <- file.path(dbHome, "dblog", paste0(prep$tblNm, tmStmp, ".Rda"))
	save(out, file = outFl)
	out
}

addRecPrep <- function(tblNm, data, meta, prepHome =  "/home/db/mcs/dbprep") {
	fls <- createPrepDir(prepHome, tblNm)
	sql <- paste(readLines("/home/db/cls_db/insert_template.sql"),
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

addRecPrepFromSav <- function(f, prepHome = "/home/db/mcs/dbprep") {
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

