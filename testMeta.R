conn <- dbConnect(MonetDB.R(), host="localhost", dbname="mcs",  # 
				  user="monetdb", password="monetdb")                 # 
dbListTables(conn)

qry <- "SELECT * FROM jsonmeta WHERE clstable IN ('mcs5_hhgrid', 'mcs5_cm_derived')"


a <- dbGetQuery(conn, qry)
colnames(a)
dim(a)
aa <- unique(a$clstable)
aa
a$clsmeta[14]
