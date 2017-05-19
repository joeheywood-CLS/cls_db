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

buildQuery <- function(inDF = NULL, inCSV = "") {
	if(file.exists(inCSV)){
		## read from csv file
		inDF <- read_csv(inCSV)
	} 
	bld <- manageQueryTable(inDF)
	## select statement should be * if empty vrb column
	## first 1 or 2 rows: tbl should be _join_ for join vrbs
	slct <- "SELECT %s FROM %s \n"
	## build joins
	sl <- sprintf(slct, paste(bld$slct, collapse = ", "))
	jn <- "%s JOIN %s ON \n"
	jnx <- "%s.%s = %s.%s"
	j <- sprintf(jn, "INNER", bld$tbls[-1]) 
	j2 <- paste(sprintf(jnx
	whr <- "WHERE %s"
	## execute query
	## save query
	## meta data: just use union?
}

inDF <- read_csv("query.csv") 
manageQueryTable <- function(df) {
	## input: data.frame (two cols: table, vrb)
	## TODO: allow for "*"
	joins <- df$vrb[which(df$tbl == "_join_")]
	tbls <- df$tbl[which(df$tbl != "_join_")]
	df$tbl[which(df$tbl == "_join_")] <- tbls[1]
	slct <- paste0(df$tbl, ".", df$vrb)
	## Value: list of vectors - name is table, vector is the variables
	list(slct = c(), tbls = tbls, joins = join )
}

createQueryDF <- function(t, ast = FALSE) {
	data.frame(tbl = c("_join_", "+"))

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
aa <- getDF("mcs5_derived") 
bb <- getDF("mcs5_parent_cm")
vv <- dbListFields(conn, "mcs5_derived")
ddf <- data.frame(tbl = "_join_", vrb = c("mcsid", "ecnum00"), stringsAsFactors = FALSE )
ddf <- rbind(ddf, data.frame(tbl = "mcs5_derived", vrb = vv[-(1:2)]))
vv <-dbListFields(conn, "mcs5_parent_cm")
ddf <- rbind(ddf, data.frame(tbl = "mcs5_parent_cm", vrb = vv[-(1:2)]))
write_csv(ddf, "query.csv")
