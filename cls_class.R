## Class declarations for db/metadata objects
## This file should include functions to translate from one class to another
##  - for example, changing the metadata object found in the database to one 
##    better suited to labelled

## Joe - May 2017

## class that extends the labelled class that is in haven
## class that extends tibble

setClass("ctg",
		 representation(
						code = "numeric",
						label = "character",
						missing = "logical"
						)
		 )

setClass("var",
		 representation(
						varName = "character",
						varLabel = "character",
						ctg = "ctg"
						)
		 )

setValidity("ctg", function(x) {
				all( c(length(x@code), length(x@label)) == length(x@missing))
		 })

mtToVar <- function(m) {
	mt <- m$ctgry
	ctg <- new("ctg", code = mt$values, label = mt$labels,
			   missing = mt$missing)
	new("var", varName = m$id, varLabel = m$label, ctg = ctg)
}


varToLabelled <- function(vr, x) {
	## args: vr - var object, x - raw vector 
	## create and extend labelled class 
}

lTab <- function(l) {
	## arg: l - labelled
	ltab <- as.data.frame(table(l), stringsAsFactors = FALSE)
}

