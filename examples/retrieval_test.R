source("accessInfo.R")
## background ##
str <- "
tbl					| vrb	
mcs5_cm_derived 	| mcsid
mcs5_cm_derived 	| ecnum00
mcs5_cm_derived 	| eccsex00
mcs5_cm_derived 	| edc06e00
mcs5_cm_interview 	| ecnum00
mcs5_cm_interview 	| ecq11d00
mcs5_cm_interview 	| ecq53a00
mcs5_family_derived | eoecduk0
"
tbls <- read.table(text = str, header = TRUE, sep = "|", strip.white = TRUE,
				   stringsAsFactors = FALSE)
### joins ###

jn <- list(
		mcs5_cm_interview = list(
			tbl = "mcs5_cm_interview",
			jn_tbl = "mcs5_cm_derived",
			jnVrb = "ecnum00"
			)
		)


oo <- buildQuery(inDF = tbls, joins = jn)


c_fq <- function(o) {
	cat(paste0("Variable: ", o$att$vName, "\n------------\n", o$att$label ))
	o$tb$percent = round(o$tb$percent*100, 1)
	print(knitr::kable(o$tb[, -4], row.names = FALSE))
	cat(paste0("Total valid cases: ", o$totalValid, " (", round(o$percValid,1),
			   "%)"))
	print(knitr::kable(o$mss))
}

c_repDat <- function(df) {

}

c_sub <- function(x, wch) {
	a <- attributes(x)
	x <- x[wch]
	attributes(x) <- a
	x
}	

c_fq(clsFq(oo$ecq53a00))
c_fq(clsFq(oo$eccsex00))
c_fq(clsFq(c_sub(oo$ecq53a00, which(oo$eccsex00 == 1))))
c_fq(clsFq(c_sub(oo$ecq53a00, which(oo$eccsex00 == 2))))
