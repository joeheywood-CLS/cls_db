library(dplyr)
library(readr)
library(knitr)
library(tidyr)
library(rjson)
library(DBI)
library(MonetDB.R)
source("cls_classS3.R")
####
## Some functions:
## - getDF(table, col)
## - listTables
## - tab/crosstab

getDF <- function(tbl, vrbs = "*") {
	## connect to database
	conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs")  
	## get data (construct query)
	if(length(vrbs) > 1) {
		vrbs <- paste(vrbs, collapse = ", ")
	}
	mtd <- dbGetQuery(conn, paste0("SELECT * FROM jsonmeta WHERE clstable = '", tbl, "'"))
	dat <- dbGetQuery(conn, paste0("SELECT ", vrbs, " FROM ", tbl))
	for(d in colnames(dat)) {
		mx <- which(tolower(mtd$vrb) == tolower(d))
		attributes(dat[[d]]) <- jsonToCls(mtd$clsmeta[mx])
	}
	dbDisconnect(conn)
	dat
}


### query data frame has two cols: tbl and vrb

buildQuery <- function(inDF = NULL, inCSV = "", joins = list()) {
	conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs") 
	if(file.exists(inCSV)){
		## read from csv file
		inDF <- read_csv(inCSV)
	} 
	df <- inDF
	slct <- createSelects(df)
	jns <- createJoins(unique(df$tbl), joins)
	qry <- paste(c(slct, jns), collapse = " ")
	xx <- dbGetQuery(conn, qry)
	df$find <- paste0(df$tbl, "_", df$vrb)
	mm <- getMQ(conn, df)
	mm$vrb <- tolower(mm$vrb)
	mm$find <- paste0(mm$clstable, "_", mm$vrb)
	mm <- mm[which(mm$find %in% unique(df$find)),]
	for(cln in colnames(xx)) {
		attributes(xx[[cln]]) <- jsonToCls(mm$clsmeta[which(mm$vrb == cln)])
	}
	dbDisconnect(conn)
	xx
}

createSelects <- function(df) {
	slcts <- "*"
	if(any(nchar(df$vrb) > 0 )) {
		slcts <- paste0(df$tbl, ".", df$vrb)
		## remove duplicates? for loop?
	}
	sprintf("SELECT %s FROM %s", paste(slcts, collapse = ", "), df$tbl[1])
}

createJoins <- function(tbls, jn) {
	jnStmt <- c()
	tb1 = tbls[1]
	tbls <- tbls[-1]
	j <- vapply(tbls, 
		   function(t) {
			   stmt <- sprintf("INNER JOIN %s ON %s.mcsid = %s.mcsid", t, tb1, t)
			   if(t %in% names(jn)) {
				   ### etc ###
				   j <- jn[[t]]
				   add <- sprintf("\n\t%s.%s = %s.%s", 
								  t, j$jnVrb, j$jn_tbl, j$jnVrb)
				   stmt <- paste0(stmt, " AND ", add)
			   }
			   stmt
		}, "")
	paste(j, collapse = " \n")
}

# createJoins <- function(df, jnLst) {
#     jns <- c()
#     tb1 <- df$tbl[1]
#     for(tb in unique(df$tbl)[-1]) {
#         jnClms <- "mcsid" 
#         if(tb %in% names(jnLst)) {
#             jnClms <- c(jnClms, jnLst[[tb]])
#         } else if("def" %in% names(jnLst)) {
#             jnClms <- c(jnClms, jnLst$def)
#         }
#         j <- paste(sprintf("%s.%s = %s.%s", tb, jnClms, tb1, jnClms), 
#                    collapse = " AND ")
#         jj <- sprintf("INNER JOIN %s ON %s", tb, j)
#         jns <- c(jns, jj)
#     }
#     paste(jns, collapse = " \n")
# }
# 
getMQ <- function(conn, df) {
	sl <- "SELECT * FROM jsonmeta WHERE clstable IN(%s)"
	tbls <- paste(paste0("'", unique(df$tbl), "'"), collapse = ", " )
	dbGetQuery(conn, sprintf(sl, tbls))
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

# createQueryDF <- function() {
#     conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs",  # 
#                       user="monetdb", password="monetdb")                 # 
#     dr <- dbListFields(conn, "mcs5_derived")
#     ci <- dbListFields(conn, "mcs5_cm_interview")
#     rbind(
#           data.frame(tbl = "mcs5_derived",
#                      vrb = dr[3:7], stringsAsFactors = FALSE),
#           data.frame(tbl = "mcs5_cm_interview",
#                      vrb = ci[3:7], stringsAsFactors = FALSE)  )
# }

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
