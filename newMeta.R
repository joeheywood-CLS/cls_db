library(haven)
library(rjson)
library(readr)
library(MonetDB.R)
library(DBI)

dd <- dir("/home/db/mcs/raw", full.names = TRUE, pattern = ".sav")

################# only two 'public' functions ######################

addRec <- function(f) {
	### wrapper around createSchema ###
	## Should also include tests/reports
	conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs",  # 
					  user="monetdb", password="monetdb")               # 
	tbl <- createSchema(f)
	buildFl <- sprintf("/home/db/mcs/dbprep/%s/%s_build.sql", tbl, tbl)
	cmd <- sprintf("mclient -d mcs -i %s", buildFl)
	hh <- system(cmd, intern = TRUE)
	newTab <- dbGetQuery(conn, paste0("SELECT * FROM ", tbl))
	rawTab <- getDataFromSav(f)
	chk <- checkAvgs(rawTab, newTab)
	dbDisconnect(conn)
	list(table = tbl, check = chk, cmd = cmd, res = hh)
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
	df <- getDataFromSav(f)
	print(paste0("Read sav file in ", round(difftime(Sys.time(), st, unit = "secs")), " seconds"))
	print(paste0("File has ", nrow(df), " rows and ", ncol(df), " columns"))
	sql <- paste(readLines("/home/db/cls_db/insert_template.sql"),
				 collapse = " \n")
	varDefs <- vapply(colnames(df), function(x) {
						  sprintf("%s %s", x, getDBType(df[[x]]))
				 }, "")
	write_delim(df, datFl, delim = "|", na = "", col_names = FALSE)
	mt <- saveMeta(df, tblNm)
	write.table(mt, metFl, quote = FALSE, qmethod = "escape", sep = "|", na = "{}", col.names = FALSE, row.names = FALSE)
	sql_ins <- sprintf(sql, tblNm, paste(varDefs, collapse = ",\n"), datFl, metFl)
	print(paste0("Finished in ", round(difftime(Sys.time(), st, unit = "secs")), " seconds"))
	bldNm <- paste0(tblNm, "_build.sql")
	print(paste0("mclient -d mcs -i ", tblNm, " to add to db"))
	writeLines(sql_ins, file.path(prepHome, bldNm))
	tblNm
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

toClsMeta <- function(vnm, vlbl, ctg, ...) {
	y <- as.list(...)
	out <- list(vName = vnm, vLabel = vlbl, ctg = ctg)
	for(a in names(y)) {
		out$a <- y[[a]]
	}
	out
}

updateClsRow <- function(cn, row) {
	x <- fromJSON(row['savmeta'])
	vnm  <- row['vrb']
	vlbl <- ""
	if('label' %in% names(x)) vlbl <- x$label
	ctg <- data.frame()
	if('labels' %in% names(x)) {
		lb <- unlist(x$labels)
		ctg <- data.frame(code = lb, label = names(lb), missing = FALSE)
		ctg$missing[which(ctg$code %in% x$na_values)] <- TRUE
	}
	x$labels <- x$label <- x$na_values <- NULL

}

# checkAverages <- function(a, b) {
#     if(!ncol(a) == ncol(b) & nrow(a) == nrow(b)) return(FALSE)
#     vapply(1:ncol(a), cmpVects, TRUE, a = a, b = b)
# }
# 
# cmpVects <- function(x, a, b) {
#     cmp1 <- a[[x]]
#     cmp2 <- b[[x]]
#     if(is.character(cmp1[1]) ) {
#         cmp1 <- vapply(cmp1, rawNumFromString, 0)
#         cmp2 <- vapply(cmp2, rawNumFromString, 0)
#     }
#     cmp1 <- as.numeric(cmp1)
#     cmp2 <- as.numeric(cmp2)
#     smp <- sample(1:length(cmp1), round(length(cmp1) / 10))
#     full <- all(mean(cmp1, na.rm = TRUE) == mean(cmp2, na.rm = TRUE))
#     part <- all(mean(cmp1[smp], na.rm = TRUE) == mean(cmp2[smp], na.rm = TRUE))
#     if(all(c(full, part)) == FALSE) {
#         save(full, part, cmp1, cmp2, x,a, b, file = "debug.Rda")
#         stop("NOOOO!!!")
#     }
#     all(c(full, part))
# }
# 
# rawNumFromString <- function(s) {
#     sum(as.numeric(charToRaw(s)))
# }

