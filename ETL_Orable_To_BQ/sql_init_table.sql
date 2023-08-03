DROP TABLE IF EXISTS log_error;
CREATE TABLE "log_error" (
	"id"	INTEGER,
	"error_datetime"	TEXT NOT NULL,
     "etl_datetime"	TEXT NOT NULL,
	"data_source_id"	TEXT NOT NULL,
	"message"	TEXT NOT NULL,
	PRIMARY KEY("id" AUTOINCREMENT)
);


DROP TABLE IF EXISTS  etl_transaction;

CREATE TABLE "etl_transaction" (
	"id"	INTEGER,
	"etl_datetime"	TEXT NOT NULL,
	"data_source_id"	TEXT NOT NULL,
	"is_load_all"	INTEGER NOT NULL DEFAULT 0,
	"completely"  INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY("id" AUTOINCREMENT)
);


DROP TABLE IF EXISTS  etl_transaction;

CREATE TABLE "etl_transaction" (
	"id"	INTEGER,
	"etl_datetime"	TEXT NOT NULL,
	"data_source_id"	TEXT NOT NULL,
	"is_load_all"	INTEGER NOT NULL DEFAULT 0,
	"completely"  INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY("id" AUTOINCREMENT)
);

CREATE TABLE "data_source" (
	"id"	TEXT,
	"first_load_col"	TEXT,
	"partition_date_col"	TEXT,
	"partition_date_type"	TEXT,
	"cluster_col_list"	TEXT,
	"date_col_list"	TEXT,
	"load_from_type"	TEXT,
	"datastore_id"	INTEGER,
	PRIMARY KEY("id")
);


CREATE TABLE "data_store" (
	"id"	INTEGER,
	"database_ip"	TEXT,
	"database_host"	TEXT,
	"database_port"	INTEGER,
	"databases_service_name"	TEXT,
	"databases_user"	TEXT,
	"databases_password"	TEXT,
	"data_store_name"	TEXT,
	PRIMARY KEY("id" AUTOINCREMENT)
);

