source("../addRecs.R")
fls <- dir("/home/db/mcs/raw", full.names = TRUE, pattern = ".sav")
resetDB()

errorMsg <- function(e) {
	print(e)
	print("DID NOT WORK")
	list(error = e)
}

sink(file = as.character(Sys.time(), 
				  format =  "/home/db/mcs/dblog/addRecs%Y%m%d_%H%M.txt"),
	 type = c("output", "message"))
for(d in fls) {
	print("========================================")
	print(d)
	print("----------------------------------------")
	out <- tryCatch({
		ar <- addRec(d, dbHome = "/home/db/mcs")
		print(paste0("Successfully added ", ar$table))
	}, error = errorMsg, finally = {print("done")})
}
sink()


