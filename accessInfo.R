library(dplyr)
library(readr)
library(knitr)
library(tidyr)
library(MonetDB.R)
library(DBI)
library(jsonlite)

####
## Some functions:
## - getDF(table, col)
## - listTables
## - tab/crosstab

getDF <- function(tbl) {
	## connect to database
	conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs",  # 
			          user="monetdb", password="monetdb")                 # 
	dbListTables(conn)
	## get data (construct query)
	dat <- dbGetQuery(conn, paste0("SELECT * FROM ", tbl, " LIMIT 1000"))
	mt <- dbGetQuery(conn, paste0("SELECT * FROM m_", tbl ))
	mtObj <- list()
	for(m in colnames(mt)) {
		jsn <- gsub("\\\\", "", mt[[m]][1])
		mtObj[[m]] <- fromJSON(jsn)
	}
	dbDisconnect(conn)
	list(dat = dat, mt = mt, mtObj=mtObj)
}

buildQuery <- function(inDF = NULL, inCSV = "", joins = list()) {
	conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs",  # 
			          user="monetdb", password="monetdb")                 # 
	if(file.exists(inCSV)){
		## read from csv file
		inDF <- read_csv(inCSV)
	} 
	## bld <- manageQueryTable(inDF)
	## info needed at this point:
	##  - list of tables
	##  - list of columns (or *)
	##  - list of joins: defaulting to mcsid and cm number
	slct <- createSelects(df)
	jns <- createJoins(df, joins)
	qry <- paste(c(slct, jns), collapse = " ")
	xx <- dbGetQuery(conn, qry)
	mqry <- getMetaQry(df)
	mm <- dbGetQuery(conn, mqry)
	## build joins
	## execute query
	## save query
	## meta data: just use union?
}

createSelects <- function(df) {
	slcts <- "*"
	if(any(nchar(df$vrb) > 0 )) {
		slcts <- paste0(df$tbl, ".", df$vrb)
		## remove duplicates? for loop?
	}
	sprintf("SELECT %s FROM %s", paste(slcts, collapse = ", "), df$tbl[1])
}

createJoins <- function(df, jnLst) {
	jns <- c()
	tb1 <- df$tbl[1]
	for(tb in unique(df$tbl)[-1]) {
		jnClms <- "mcsid" 
		if(tb %in% names(jnLst)) {
			jnClms <- c(jnClms, jnLst[[tb]])
		} else if("def" %in% names(jnLst)) {
			jnClms <- c(jnClms, jnLst$def)
		}
		j <- paste(sprintf("%s.%s = %s.%s", tb, jnClms, tb1, jnClms), 
				   collapse = " AND ")
		jj <- sprintf("INNER JOIN %s ON %s", tb, j)
		jns <- c(jns, jj)
	}
	paste(jns, collapse = " \n")
}

mtF <- function(t) {
	if(any(nchar(t$vrb) > 0)) {
		sl <- paste0(t$tbl, ".", t$vrb)
	} else {
		sl <- "*"
	}
	sprintf("SELECT %s FROM %s", paste(sl, collapse = ", "),
				  t$tbl[1])
}

getMetaQry <- function(df) {
	df$tbl <- paste0("m_", df$tbl)
	tbx <- split(df, df$tbl)
	slcts <- vapply(tbx, mtF, "") 
	paste(slcts, collapse = " \n UNION \n ")
}


manageQueryTable <- function(df) {
	## input: data.frame (two cols: table, vrb)
	joins <- df$vrb[which(df$tbl == "_join_")]
	tbls <- df$tbl[which(df$tbl != "_join_")]
	df$tbl[which(df$tbl == "_join_")] <- tbls[1]
	slct <- paste0(df$tbl, ".", df$vrb)
	## Value: list of vectors - name is table, vector is the variables
	list(slct = c(), tbls = tbls, joins = join )
}

createQueryDF <- function() {
	conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs",  # 
			          user="monetdb", password="monetdb")                 # 
	dr <- dbListFields(conn, "mcs5_derived")
	ci <- dbListFields(conn, "mcs5_cm_interview")
	rbind(
		  data.frame(tbl = "mcs5_derived",
					 vrb = dr[3:7], stringsAsFactors = FALSE),
		  data.frame(tbl = "mcs5_cm_interview",
					 vrb = ci[3:7], stringsAsFactors = FALSE)  )
}

tab <- function(x, obj) {
	tb <- table(obj$dat[[x]])
	d <- as.data.frame(tb, stringsAsFactors = FALSE)
	d$Var1 <- as.numeric(d$Var1)
	ctg <- obj$mtObj[[x]]$ctgry
	ctgdf <- data.frame(Var1 = ctg$values, label = ctg$labels, 
						missing = ctg$missing, stringsAsFactors = FALSE)
	left_join(ctgdf, d)
}


# aa <- getDF("mcs5_derived") 
# bb <- getDF("mcs5_parent_cm")
# vv <- dbListFields(conn, "mcs5_derived")
# ddf <- data.frame(tbl = "_join_", vrb = c("mcsid", "ecnum00"), stringsAsFactors = FALSE )
# ddf <- rbind(ddf, data.frame(tbl = "mcs5_derived", vrb = vv[-(1:2)]))
# vv <-dbListFields(conn, "mcs5_parent_cm")
# ddf <- rbind(ddf, data.frame(tbl = "mcs5_parent_cm", vrb = vv[-(1:2)]))
# write_csv(ddf, "query.csv")
