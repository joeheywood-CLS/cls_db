library(haven)

checkAvgs <- function(a, b) {
	if(!ncol(a) == ncol(b) & nrow(a) == nrow(b)) return(FALSE)
	for(col in ncol(a)) {
		if(is.character(a[[col]][1])) {
			a[[col]] <- as.numeric(vapply(a[[col]], rawNumFromString, 0))
			b[[col]] <- as.numeric(vapply(b[[col]], rawNumFromString, 0))
		}
		smp <- sample(1:nrow(a), nrow(a) / 10)
		full <- mean(a[[col]]) == mean(b[[col]])	
		part <- mean(a[[col]][smp]) == mean(b[[col]][smp])
		if(full == FALSE | part == FALSE) {
			save(a, b, col, file = "debug.Rda")
			stop(paste0("Fell down at column ", colnames(a)[col]))
		} 
	}
	TRUE
}

# f <- dd[2]
# conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs",  # 
#                       user="monetdb", password="monetdb")               # 
# tbl <- "mcs5_cm_assessment"
# newTab <- dbGetQuery(conn, paste0("SELECT * FROM ", tbl))
# rawTab <- getDataFromSav(f)
# newTab <- newTab[1:200,1:100]
# rawTab <- rawTab[1:200,1:100]
# microbenchmark(checkAverages(rawTab, newTab),
#                checkAvgs(rawTab,newTab), n = 500)
# 
