savToCls <- function(x, n) {
	att <- attributes(x)
	if("format.spss" %in% names(att)) { 
		num <- grepl("^F", att$format.spss) 
	} else {
		num <- is.numeric(x)
	}
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
	# x <- ifelse(num == TRUE, as.numeric(x), as.character(x))
	if(num == TRUE) {
		x <- as.numeric(x)
	} else {
		x <- as.character(x)
	}
	attr(x, "vName") <- n
	attr(x, "label") <- att$label
	attr(x, "ctg") <- ctg
	x
}


