library(haven)
library(jsonlite)
library(readr)


aa <- dir("N:/mcs/UKDA-7464-spss/spss/spss19/", full.names = T, pattern = ".sav")



## benchmark/compare 
saveMeta <- function(x, d) {
    xx <- lapply(x, attributes )
    yy <- vapply(xx, toJSON, "")
    data.frame(dataset = d, savMeta = yy, vrb = colnames(x),
               stringsAsFactors = FALSE)
}

runMt <- function(a) {
    st <- Sys.time()
    y <- read_sav(a, user_na = TRUE)
    print(paste0("Opened file in ", Sys.time() - st))
    flnm <- gsub(".sav", ".dat", a)
    bsnm <- gsub(".dat", "", basename(flnm))
    print(flnm)
    write_delim(x = y, path = bsnm, delim = "|", na = "-42")
    print(paste0("Finished in ", Sys.time() - st))
    saveMeta(y, a)
}

rr <- do.call(rbind, lapply(aa, runMt))