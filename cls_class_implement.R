### cls_df tibble made up of cls_lb vectors ###
## only function should be a data.frame() equivalent

### cls_lb vectors
## extends labelled class
## includes variable label attribute
## include 

### lbTab function to tabulate data
## outputs table with flexible column and row labels
## printed to screen using kable

lb_df <- function(v) {
    ## args: a list of cls_v vectors
    
}

lb_v <- function(rw_v, vnm, m = list()) {
    ## args: rw_v - raw vector, m - metadata object
    ctg <- new("ctg", code = m$value, label = m$ctg$label,
               missing = m$ctg$missing)
    new("cls_v", vName = vnm, label = m$label, ctg = ctg)
}

