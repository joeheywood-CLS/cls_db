library(dplyr)

###############################################################################
# The 'cls' class is very simple. It is similar to labelled and jsonstat 
# objects. It includes, as standard, a vName string, a label string, and 
# labelled values. I have also included some more values below that use this
# class to give labelled frequency tables/cross-tabs etc.
###############################################################################

## creates a new (empty) CLS object
## args: nm (character) name of variable
createCLS <- function(nm) {
	out <- list()
	ctg <- data.frame(code = c(), label = c(), missing = c())
	out$vName <- nm
	out$label <- ""
	out$ctg <- ctg
	structure(out, class = "cls")
}

setCLSvLabel <- function(cls, lb) {
	cls$label <- lb
	cls
}

addCtg <- function(cls, vlb, mss) {
	ctg <- data.frame(code = as.numeric(vlb),
					  label = names(vlb), stringsAsFactors = FALSE)
	ctg$missing <- FALSE
	ctg$missing[which(ctg$code %in% mss)] <- TRUE
	cls$ctg <- rbind(cls$ctg, ctg)
	cls
}

####

savToCls <- function(att) {
	out <- createCLS(att$vnm)
	if(!is.null(att$labels)) {
		if(!is.null(att$na_values)) {
			att$na_values <- c()
		}
		out <- addCtg(out, att$labels, att$na_values)
	}
	if(!is.null(att$label)) out <- setCLSvLabel(out, att$label)
	out
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

clsdbFq <- function(tb, clsObj) {
	colnames(tb) <- c("code", "frequency")
	tb <- left_join(tb, clsObj$ctg)
	mss <- tb[which(tb$missing == TRUE),]
	tb <- tb[which(tb$missing == FALSE),]
	tb$percent <- tb$frequency / sum(tb$frequency)
	totalValid <- sum(tb$frequency)
	totalMissing <- sum(mss$frequency)
	percValid <- round(totalValid / (totalValid + totalMissing), 4)*100
	percMissing <- round(totalMissing / (totalValid + totalMissing), 4)*100
	out <- list(tb = tb, mss = mss, totalValid = totalValid, 
				totalMissing = totalMissing, percValid = percValid,
				percMissing = percMissing, att = clsObj)
	structure(out, class = "clsFq")
}





