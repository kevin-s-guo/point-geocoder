### DO NOT CHANGE UNLESS ALSO CHANGING docker-compose.yml to reflect changes

DBHOST = "db"
DBNAME = "census"
DBUSER = "census"
DBPASS = "census"
DBYEAR = "2020"  # will require column called "bg_<DBYEAR> in address table"

OTHER_YEAR_BG = {"2010": {"table": "bg_19", "col": "bg_2010"}}  # column for blockgroup in master_address_table

TABLENAME = "master_address_table"
