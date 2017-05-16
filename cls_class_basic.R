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
         representation(vName = "character", label = "character"))

## cls_v_ctg
setClass("cls_v_ctg", contains = "cls_v",
         representation(ctg = "ctg", values = "numeric"))

## cls_v_num
setClass("cls_v_num", contains = "cls_v",
         representation(ctg = "ctg", range = "numeric"))


## cls_v_dbl

## cls_v_chr


## cls_tab

## cls_xtab