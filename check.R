###############################################################################
# Checking function to compare two data.frames. Checks that the dimensions are
# the same, that the mean average is the precisely the same and that the mean
# of a random sample is the same
###############################################################################

## checks averages of two data frames
## a and b (data.frame) data frames that should be identical
## value TRUE if succeeds, else stops program
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

## helper function to convert a character string to a numeric value 
rawNumFromString <- function(s) {
	sum(as.numeric(charToRaw(s)))
}

 
