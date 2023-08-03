DELETE from log_error where error_datetime<=(select DATE('now', '-45 day'));
DELETE from etl_transaction where etl_datetime<=(select DATE('now', '-45 day'));