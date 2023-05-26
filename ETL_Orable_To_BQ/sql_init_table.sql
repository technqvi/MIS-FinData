DROP TABLE IF EXISTS log_error;
DROP TABLE IF EXISTS  etl_transaction;
-- CREATE TABLE "data_source" (
-- 	"id"	TEXT,
-- 	"first_load_col"	TEXT,
-- 	"partition_date_col"	TEXT,
-- 	"partition_date_type"	TEXT,
-- 	"cluster_col_list"	TEXT,
-- 	"date_col_list"	TEXT,
-- 	PRIMARY KEY("id")
-- );

CREATE TABLE "etl_transaction" (
	"id"	INTEGER,
	"etl_datetime"	TEXT NOT NULL,
	"data_source_id"	TEXT NOT NULL,
	"no_rows"	INTEGER NOT NULL,
	"is_load_all"	INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY("id" AUTOINCREMENT)
);


CREATE TABLE "log_error" (
	"id"	INTEGER,
	"error_datetime"	TEXT NOT NULL,
     "etl_datetime"	TEXT NOT NULL,
	"data_source_id"	TEXT NOT NULL,
	"message"	TEXT NOT NULL,
	PRIMARY KEY("id" AUTOINCREMENT)
);