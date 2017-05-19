library(haven)
library(jsonlite)
library(readr)

## TODO
## - some kind of buildDB master bash file that calls all the
##   updates.
## - drop table at start of schema files
## - missing values in .dat files -99?

getMetaHaven <- function(flnm) {
    fl <- read_sav(flnm, user_na = TRUE)
	for(cln in colnames(fl)) {
		fl[[cln]][which(is.na(fl[[cln]]))] <- -42
	}
    list(
        dat = fl,
        meta = lapply(names(fl), populateM, fl = fl)
    )
}

populateM <- function(nm, fl) {
    cat(paste0("\n", nm, " " ))
    if(grepl("^[fF]", attr(fl[[nm]], "format.spss")) == TRUE) {
        cat(" NUMERIC ")
        dbType <- getNumericType(fl[[nm]])
        cat(" . ")
    } else {
        cat(" STRING ")
        dbType <- getCharType(fl[[nm]])
    }
    cat(" . ")
    list(id = nm,
         label = attr(fl[[nm]], "label"),
         formatSpss = attr(fl[[nm]], "format.spss"),
         dbType = dbType,
         ctgry = elemsListM(fl[[nm]], nm)
 	)
}

elemsListM <- function(x, n) {
    cat(" e ")
    aa <- list(id = n)
    lb <- attr(x, "labels")
    if(length(lb) == 0) {
        aa$values <- aa$labels <- numeric(0)
    } else {
        aa$values <- as.numeric(lb)
        aa$labels <- names(lb)
    }

    aa$missing <- rep(FALSE, length(lb))
    if(!is.null(attr(x, "na_values"))) {
        aa$missing[which(aa$labels %in% attr(x, "na_values"))] <- TRUE
    }
    aa
}

####################################

getNumericType <- function(x) {
    if(all(is.na(x))) {
        return("TINYINT")
    }
    
    x <- x[which(!is.na(x))]
    if(all(floor(x) == x)) {
        if(all(x %in% -127:127)) {
            return("TINYINT")
        } else if(all(x %in% -32767:32767)) {
            return("SMALLINT")
        } else{
            return("INT")
        }
    } else {
        return("DECIMAL")
    }
}

getCharType <- function(x) {
    longest <- max(nchar(x))
    if(length(unique(nchar(x))) == 1) {
        return(sprintf("CHAR(%d)", longest))
    }
    if(longest < 200) {
        return(sprintf("VARCHAR(%d)", longest))
    } else {
        return("TEXT")
    }
}

##################################

## creates schema, csv and json files ready to add.
prepareSAV <- function(f, nm, prepHome = "/home/joe/mcs/dbprep/", drop = FALSE) { 
	## args: f - sav file, nm - name of table (no spaces)
    # create directory
    prepHome <- paste0(prepHome, nm)
    dir.create(prepHome)
    ## get JSON ##
    st <- getMetaHaven(f)
    ## create schema ##
    schm <- createSchemaFromList(st$meta, nm, prepHome, drop)
    writeLines(schm, file.path(prepHome, paste0(nm, "_schema.sql")) )
    ## create meta table sql and insert json
    mtSch <- createMetaSchema(st$meta, nm, drop)
    writeLines(mtSch, file.path(prepHome, paste0(nm, "_m_schema.sql")) )
    df <- st$dat
    ## How to deal with missing values? ""? "."? "<NA>"
	## write delimited file with | and no headers
    write_delim(df, file.path(prepHome, paste0(nm, "_data.dat")), delim = "|", 
				na="", col_names = FALSE )
}


createSchemaFromList <- function(l, tblNm, flPth, drop = FALSE) {
	first <- ifelse(drop == TRUE, paste0("DROP TABLE ", tblNm, ";"), "")
    start <- sprintf("CREATE TABLE %s (", tblNm)
    end <- ");"
	flPth <- paste0(flPth, "/", tblNm, "_data.dat")
    last <- sprintf("COPY INTO %s FROM '%s';", tblNm, flPth)
    ix <- 1
    mid <- vapply(l, function(itm) {
        sprintf("%s %s", itm$id, itm$dbType)
    }, "")
    c(first, start, paste(mid, collapse = ", \n"), end, last)
}

createMetaSchema <- function(l, tblNm, drop = FALSE) {
	first <- ifelse(drop == TRUE, paste0("DROP TABLE m_", tblNm, ";"), "")
    start <- sprintf("CREATE TABLE m_%s (", tblNm)
    mid <- vapply(l, function(itm) {
        sprintf("%s text", itm$id)
    }, "")
    end <- ");"
    insSt <- sprintf("INSERT INTO m_%s VALUES (", tblNm)
    insts <- lapply(l, function(itm) {
        jstr <- toJSON(itm)
		jstr <- gsub("'", " ", jstr)
		sprintf("'%s'", jstr)
    })
    c(first, start, paste(mid, collapse = ", \n"), end,
      insSt, paste0(insts, collapse = ", \n"), end)
}



completeIngest <- function(t) {

}

#### For data delivery ###
## - get metadata as above
## - for different data types get key information
##   - number of distinct values
##   - do value labels match values?
