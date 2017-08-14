source("newMeta.R")
resetDB(conn)
head(dd)

errorMsg <- function(e) {
	print(e)
	print("DID NOT WORK")
	list(error = e)
}
sink(as.character(Sys.time(), 
				  format =  "/home/db/mcs/dblog/addRecs%Y%m%d_%H%M%S.txt"))
for(d in dd) {
	out <- tryCatch({
		addRec(d)
	}, error = errorMsg, finally = {print("done")})
	save(out, file = paste0("/home/db/mcs/dblog/", out$table, ".Rda"))
}

sink()
lgs <- dir("/home/db/mcs/dblog", pattern = "Rda", full.names = TRUE)
load(lgs[9])
names(out)
class(out$check)
length(out$check)


