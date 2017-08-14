library(dplyr)

savToCls <- function(att ) {
	## x = spss labelled variable
	out <- list()
	if(!is.null(att$labels)) {
		ctg <- data.frame(code = as.numeric(att$labels),
						  label = names(att$labels), stringsAsFactors = FALSE)
		ctg$missing <- FALSE
		if(!is.null(att$na_values)) {
			ctg$missing[which(ctg$code %in% att$na_values)] <- TRUE
		}
	} else {
		ctg <- data.frame(code = c(), label = c(), missing = c())
	}
	out$vName <- att$vnm
	out$label <- att$label
	out$ctg <- ctg
	structure(out, class = "cls")
}


### JSON functions ###

savAttsToJSON <- function(att) {
	## simple one here
	toJSON(att)
}

jsonToSavAtts <- function(jsn) {
	## this one is harder
	## it should return att EXACTLY as it is when attributes(x) is called

}

clsToJson <- function(cls) {
	## again. Should be kept simple
	toJSON(cls)

}

jsonToCls <- function(dbS) {
	## ctg needs to be a data frame.
	jsn <- gsub("\\\\", "", dbS)
	cls <- fromJSON(jsn)
	cls$ctg <- data.frame(cls$ctg, stringsAsFactors = FALSE)
	structure(cls, class = "cls")
}


clsFq <- function(x) {
	tb <- as.data.frame(table(x), stringsAsFactors = FALSE)
	colnames(tb) <- c("code", "frequency")
	att <- attributes(x)
	tb$code <- ifelse(is.numeric(att$ctg$code), 
					  as.numeric, as.character)(tb$code)
	tb <- left_join(tb, att$ctg)
	mss <- tb[which(tb$missing == TRUE),]
	tb <- tb[which(tb$missing == FALSE),]
	tb$percent <- tb$frequency / sum(tb$frequency)
	totalValid <- sum(tb$frequency)
	totalMissing <- sum(mss$frequency)
	percValid <- round(totalValid / (totalValid + totalMissing), 4)*100
	percMissing <- round(totalMissing / (totalValid + totalMissing), 4)*100
	out <- list(tb = tb, mss = mss, totalValid = totalValid, 
				totalMissing = totalMissing, percValid = percValid,
				percMissing = percMissing, att = att)
	structure(out, class = "clsFq")
}







