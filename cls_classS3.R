savToCls <- function(att, n) {
	## x = spss labelled variable, n = variable name
	out <- list()
	class(out) <- "cls"
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
	attr(out, "vName") <- n
	attr(out, "label") <- att$label
	attr(out, "ctg") <- ctg
	out
}


