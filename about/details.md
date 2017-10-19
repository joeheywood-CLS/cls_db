# DB Environment Details

## File structure

* **Raw input** - at the moment, SPSS files, but also raw CSV, Excel
* **dbprep** dat file with data, sql scripts for ingesting data/metadata
* **dbdev/dblive** files to update database (mostly R scripts)
* **dbretrievals** scripts used to retrieve data from the database (usually for specific projects). could also include generic code for things like
* **Shiny app** gui for manual searching through metadata, visualizations, viewing raw data, quick exports etc.
