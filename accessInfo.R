library(dplyr)
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
	conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs", 
			          user="monetdb", password="monetdb")
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
	## getMetadata
	## apply to a labelled class (if appropriate)
	list(dat = dat, mt = mt, mtObj=mtObj)
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
bb <- getDF("mcs5_parent_derived")
knitr::kable(tab("eccsex00", aa))
cc <- new("ctg", code = mtOb
