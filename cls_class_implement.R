### cls_df tibble made up of cls_lb vectors ###
## only function should be a data.frame() equivalent
source("cls_class_basic.R")

### cls_lb vectors
## extends labelled class
## includes variable label attribute
## include 

### lbTab function to tabulate data
## outputs table with flexible column and row labels
## printed to screen using kable

lb_df <- function(v) {
    ## args: a list of cls_v vectors
	out_df <- new("cls_df", v$dat)
	for(cl in names(v$mtObj)) { 
		out_df[[cl]] <- lb_v(v$dat[[cl]], v$mtObj[[cl]]) 
	}
	out_df
}

lb_v <- function(rw_v, m = list()) {
    ## args: rw_v - raw vector, m - metadata object
    ctg <- new("ctg", code = m$ctgry$values, label = m$ctgry$labels,
               missing = m$ctgry$missing)
    new("cls_v_ctg", rw_v, vName = m$id, label = m$label, ctg = ctg)
}

