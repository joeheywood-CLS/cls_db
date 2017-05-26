### class declarations

## some kind of abstract jsonstat-based abstract class that can contain all metadata?

## jstat
# stick with what is in the database for now.

setClass("ctg", representation(code = "numeric",
                               label = "character",
                               missing = "logical"))

## cls_df
setClass("cls_df", contains = "data.frame")

setClass("cls_v", contains = "numeric", 
         representation(vName = "character", label = "character",
						ctg = "ctg"))


setClass("cls_c", contains = "character", 
         representation(vName = "character", label = "character",
						ctg = "ctg"))
## cls_v_ctg
setClass("cls_v_ctg", contains = "cls_v",
         representation(ctg = "ctg", values = "numeric"))

## cls_v_num
setClass("cls_v_num", contains = "cls_v",
         representation(ctg = "ctg", range = "numeric"))


## from a haven read_sav or read_dta column, get a cls_v object
savToCLS_v <- function(n, df) {
	## get ctg
	ctg <- new("ctg")
	x <- df[[n]]
	att <- attributes(x)
	if(!is.null(att$labels)) {
		ctg@code <- as.numeric(att$labels)
		ctg@label <- names(att$labels)
		ctg@missing <- rep(FALSE, length(ctg@code))
		if(!is.null(att$na_values)) {
			ctg@missing[which(ctg@code %in% att$na_values)] <- TRUE
		}
	}
	if(is.numeric(x)) { 
		new("cls_v", as.numeric(x), vName = n, label = att$label, ctg = ctg)
	} else if(is.character(x)) { 
		new("cls_c", as.character(x), vName = n, label = att$label, ctg = ctg)
	}
}


cls_toJSON <- function(x) {
	out <- list()
	cat(paste0(" ."))
	out$ctg <- list(code = x@ctg@code, label = x@ctg@label,
					   missing = x@ctg@missing)
	out$vName <- x@vName
	out$label <- x@label
	jsonlite::toJSON(out)
}

cls_fromJSON <- function(j) {

}




## cls_tab

## cls_xtab
