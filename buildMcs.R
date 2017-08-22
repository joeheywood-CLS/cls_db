source("addRecs.R")

fls <- dir("/home/db/mcs/raw", full.names = TRUE, pattern = ".sav")

a <- addRec(fls[3], dbHome = "/home/db/mcs")
